import os
import argparse
import shutil
import subprocess
import logging
import time
from datetime import datetime, timezone
from fileinput import filename
from typing import List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
import sys
import re
import tempfile
import contextlib
import dataclasses
from enum import Enum, auto
import threading

from tqdm import tqdm
from sqlalchemy.orm import Session, selectinload
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models
from photoprocessor.export_arguments import ExportArgument, DateTimeArgument
from photoprocessor.merger import MergeStep, GPSMergeStep, MergeContext, BasicFieldMergeStep, MergePipeline, \
    DateTimeAndZoneMergeStep

# --- Configuration ---
CONFIG = {
    "EXIFTOOL_PATH": "exiftool",
    "BATCH_SIZE": 100,
    "MAX_WORKERS": 4,
}


# --- End Configuration ---

# Add this new Enum and Dataclass
class ExportStatus(Enum):
    """Represents the state of a file export job."""
    PENDING_EXPORT = auto()
    SUCCESS = auto()
    CONFLICT = auto()
    FAILED = auto()
    SKIPPED = auto()


@dataclasses.dataclass
class FileExportJob:
    """A dataclass to hold all information for exporting a single media file."""
    media_file: models.MediaFile
    source_location_to_copy: models.Location
    export_arguments: List[ExportArgument]
    relative_path: str

    # These fields will be populated as the job is processed
    final_output_path: str = ""
    status: ExportStatus = ExportStatus.PENDING_EXPORT
    error_message: str = ""

    def get_exiftool_args_as_list(self) -> List[str]:
        """Helper to get the string args for logging or debugging."""
        args = []
        for arg_object in self.export_arguments:
            args.extend(arg_object.build())
        return args


# --- Core Functions ---

def get_locations_for_owner(db: Session, owner: models.Owner) -> List[models.Location]:
    """Queries all locations owned by a person with all necessary related data eagerly loaded."""
    print(f"Querying files for owner: {owner.name}...")
    return db.query(models.Location).join(
        models.MediaOwnership
    ).filter(
        models.MediaOwnership.owner_id == owner.id
    ).options(
        selectinload(models.Location.media_file).options(
            selectinload(models.MediaFile.locations).options(
                selectinload(models.Location.owners),
                selectinload(models.Location.metadata_sources).selectinload(models.MetadataSource.entries)
            )
        )
    ).all()


def get_locations_by_paths(db: Session, paths: List[str]) -> List[models.Location]:
    """Queries for specific locations based on a list of file paths."""
    print(f"Querying for {len(paths)} specific file paths...")
    return db.query(models.Location).filter(
        models.Location.path.in_(paths)
    ).options(
        selectinload(models.Location.media_file).options(
            # This is the full chain to load everything needed
            selectinload(models.MediaFile.locations).options(
                selectinload(models.Location.owners),  # <-- ADD THIS LINE
                selectinload(models.Location.metadata_sources).selectinload(
                    models.MetadataSource.entries)
            )
        )
    ).all()

def write_metadata_batch(jobs_to_process: List[FileExportJob]):
    """
    Writes metadata by creating a new file using ExifTool's -o option.
    Handles partially successful batch runs before falling back to individual processing.
    """
    if not jobs_to_process:
        return

    # --- Stage 1: Try the fast batch method first ---
    base_args = ["-common_args"]
    command_args = []
    for job in jobs_to_process:
        if not job.export_arguments:
            # Handle files with no metadata as simple copies
            try:
                copy_file_task((job.source_location_to_copy.path, job.final_output_path))
                job.status = ExportStatus.SUCCESS
            except Exception as e:
                job.status = ExportStatus.FAILED
                job.error_message = f"File copy failed: {e}"
            continue

        file_args = job.get_exiftool_args_as_list()
        if file_args:
            command_args.extend(file_args)
            command_args.extend(["-o", job.final_output_path, job.source_location_to_copy.path])
            command_args.append("-execute")

    if not command_args:
        return  # All jobs were simple copies

    final_batch_args = base_args + command_args[:-1]
    argfile_path = None
    try:
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8', suffix=".txt") as argfile:
            argfile.write("\n".join(final_batch_args))
            argfile_path = argfile.name

        final_command = [CONFIG["EXIFTOOL_PATH"], "-@", argfile_path]
        subprocess.run(final_command, check=True, capture_output=True, text=True, encoding='utf-8')

        # If the batch command succeeds, all pending jobs are now successful.
        for job in jobs_to_process:
            if job.status == ExportStatus.PENDING_EXPORT:
                job.status = ExportStatus.SUCCESS
        return  # Success, exit the function.

    except subprocess.CalledProcessError as e:
        print(f"\n--- ExifTool Batch Failed. Falling back to individual processing. ---")
        print(f"Original Batch Error: {e.stderr.strip()}")
        print("----------------------------------------------------------------------")
        # Proceed to fallback...

    finally:
        # This ensures the temp file is always removed, even after an error.
        if argfile_path and os.path.exists(argfile_path):
            os.remove(argfile_path)

    # --- Stage 2: Fallback to processing files individually ---
    for job in jobs_to_process:
        if job.status != ExportStatus.PENDING_EXPORT:
            continue

        # CRITICAL FIX: Check if the file was created by the failed batch before retrying.
        if os.path.exists(job.final_output_path):
            job.status = ExportStatus.SUCCESS
            continue

        file_specific_args = job.get_exiftool_args_as_list()
        final_individual_args = [CONFIG["EXIFTOOL_PATH"]] + file_specific_args + \
                                ["-o", job.final_output_path, job.source_location_to_copy.path]

        try:
            subprocess.run(final_individual_args, check=True, capture_output=True, text=True, encoding='utf-8')
            job.status = ExportStatus.SUCCESS
        except subprocess.CalledProcessError as individual_e:
            job.status = ExportStatus.FAILED
            job.error_message = individual_e.stderr.strip()
            print(f"\n--- Individual ExifTool Error ---")
            print(f"Failed to process: {os.path.basename(job.final_output_path)}")
            print(f"Error: {job.error_message}")
            print("---------------------------------")

def copy_file_task(src_dst_tuple: Tuple[str, str]):
    """
    Simple wrapper for shutil.copyfile with a retry mechanism for file locks.
    """
    src, dst = src_dst_tuple
    retries = 3
    delay = 2  # seconds

    for attempt in range(retries):
        try:
            shutil.copyfile(src, dst)
            return src, None  # Success!
        except OSError as e:
            # On Windows, error 32 is "The process cannot access the file..."
            if hasattr(e, 'winerror') and e.winerror == 32:
                if attempt < retries - 1:
                    # If it's the specific lock error and not the last attempt, wait and retry.
                    time.sleep(delay)
                    continue
            # If it's a different OS error or the last attempt, return the error.
            return src, e
        except Exception as e:
            # Catch any other unexpected copy errors
            return src, e

def log_conflict(logger: logging.Logger, file_path: str, conflicts: Dict[str, List[str]]):
    """Formats and logs a merge conflict message with messages grouped by field."""

    conflict_lines = []
    # Iterate through each field that has conflicts.
    for field, messages in conflicts.items():
        # Add a line for the field itself.
        conflict_lines.append(f"Field '{field}':")
        # Add an indented line for each specific error message for that field.
        for message in messages:
            conflict_lines.append(f"  - {message}")

    # Join all lines, ensuring proper indentation for the whole block.
    details_str = "\n    ".join(conflict_lines)
    logger.warning(f"{file_path}\n    {details_str}")

def _get_best_location(locations: List[models.Location]) -> models.Location:
    """Selects the best location from a list based on largest file size, with ID as a tie-breaker."""
    if not locations:
        raise ValueError("Cannot select best location from an empty list.")
    return sorted(locations, key=lambda l: (-l.file_size, l.id))[0]

def generate_relative_export_path(media_file: models.MediaFile, export_arguments: List[ExportArgument], owner: models.Owner) -> Tuple[str, models.Location]:
    """
    Generates the full relative export path for a media file based on a prioritized
    set of rules, falling back to a year-based structure using the merged metadata.
    """

    ### --- First Priority: Check for User-Suggested Export Path --- ###
    ownerships = [
        mo for loc in media_file.locations
        for mo in loc.owners
        if mo.owner_id == owner.id and mo.suggested_export_path is not None
    ]
    unique_suggestions = set(mo.suggested_export_path for mo in ownerships if mo.suggested_export_path)
    if unique_suggestions:
        target_subdir = ""
        if len(unique_suggestions) == 1:
            target_subdir = unique_suggestions.pop()
        else:
            sanitized_paths = sorted([p.replace(os.sep, '-') for p in unique_suggestions if p])
            target_subdir = '--'.join(sanitized_paths)

        # For suggested paths, we still use the overall best location
        best_overall_location = _get_best_location(media_file.locations)
        return os.path.join(target_subdir, best_overall_location.filename), best_overall_location

    # --- Define Folder Hierarchy Rules ---
    priority_rules = [
        ('whatsapp images/sent', os.path.join("Whatsapp Images", "Sent")),
        ('whatsapp video/sent', os.path.join("Whatsapp Video", "Sent")),
        ('whatsapp images', "Whatsapp Images"),
        ('whatsapp video', "Whatsapp Video"),
    ]
    whatsapp_filename_pattern = re.compile(r'-WA\d{4}', re.IGNORECASE)

    # --- Priority 2: Owner-Specific WhatsApp Logic ---
    owner_locations = [loc for loc in media_file.locations if any(mo.owner_id == owner.id for mo in loc.owners)]

    for loc in owner_locations:
        # Check owner's locations against WhatsApp-specific rules
        path_lower = loc.path.lower().replace('\\', '/')
        is_whatsapp = False
        target_subdir = ""

        for pattern, subdir in priority_rules:
            if pattern in path_lower:
                is_whatsapp = True
                target_subdir = subdir
                break

        if not is_whatsapp and whatsapp_filename_pattern.search(loc.filename):
            is_whatsapp = True
            target_subdir = "Whatsapp Video" if media_file.mime_type.startswith('video/') else "Whatsapp Images"

        if is_whatsapp:
            # If it's a WhatsApp file, the source location MUST be from the owner's pool.
            source_location = _get_best_location(owner_locations)
            relative_path = os.path.join(target_subdir, source_location.filename)
            return relative_path, source_location

    # --- Priority 3: General Rules (Non-WhatsApp) using ALL locations ---
    general_rules = [
        ('dcim/camera', "Camera"),
        ('screenshots', "Screenshots"),
    ]
    all_paths = [loc.path.lower().replace('\\', '/') for loc in media_file.locations]
    best_overall_location = _get_best_location(media_file.locations)

    for pattern, target_subdir in general_rules:
        for path in all_paths:
            if pattern in path:
                return os.path.join(target_subdir, best_overall_location.filename), best_overall_location

    if re.search(r'screenshot', best_overall_location.filename, re.IGNORECASE):
        return os.path.join("Screenshots", best_overall_location.filename), best_overall_location

    # --- Priority 4: Default to Year-Based Pathing using Merged Date ---
    date_taken = None
    date_modified = None
    for arg in export_arguments:
        if isinstance(arg, DateTimeArgument):
            if arg.date_type == "taken" and isinstance(arg.value, datetime):
                date_taken = arg.value
            elif arg.date_type == "modified" and isinstance(arg.value, datetime):
                date_modified = arg.value

    year_str = "Unknown_Date"
    if date_taken and isinstance(date_taken, datetime):
        year_str = str(date_taken.year)
    elif date_modified and isinstance(date_modified, datetime):
        year_str = str(date_modified.year)

    return os.path.join(year_str, best_overall_location.filename), best_overall_location

def find_unique_filepath(destination_path: str) -> str:
    """
    Checks if a file exists at the destination. If so, it appends a number
    like '-[1]' to the filename until a unique path is found.
    """
    if not os.path.exists(destination_path):
        return destination_path  # The original path is already unique

    directory = os.path.dirname(destination_path)
    filename = os.path.basename(destination_path)
    base_name, extension = os.path.splitext(filename)

    counter = 1
    while True:
        # Create a new filename, e.g., "my_photo-[1].jpg"
        new_filename = f"{base_name}-[{counter}]{extension}"
        new_path = os.path.join(directory, new_filename)

        if not os.path.exists(new_path):
            return new_path  # Found a unique path

        counter += 1


# In export_pipe.py

def _prepare_export_jobs(
        locations: List[models.Location],
        pipeline: MergePipeline,
        owner: models.Owner,
        export_dir: str,
        processed_media_ids: set,
        processed_ids_lock: threading.Lock
) -> List[FileExportJob]:
    """
    Runs the merge pipeline for each location and creates a FileExportJob object.
    This step identifies conflicts and files to be skipped.
    """
    jobs = []
    for loc in locations:
        with processed_ids_lock:
            if loc.media_file.id in processed_media_ids:
                job = FileExportJob(loc.media_file, loc, [], "", status=ExportStatus.SKIPPED)
                jobs.append(job)
                continue # Continue inside the lock to ensure it's released

            processed_media_ids.add(loc.media_file.id)


        all_sources_for_file = [s for l in loc.media_file.locations for s in l.metadata_sources]

        final_arguments = []
        result_context = None

        if all_sources_for_file:
            # Run the merge pipeline only if metadata sources exist
            result_context = pipeline.run(all_sources_for_file)
            final_arguments = result_context.get_all_arguments()

        relative_path, source_loc_to_copy = generate_relative_export_path(loc.media_file, final_arguments, owner)

        job = FileExportJob(loc.media_file, source_loc_to_copy, final_arguments, relative_path)

        # Check for merge conflicts
        if result_context.conflicts:
            job.status = ExportStatus.CONFLICT
            job.error_message = str(result_context.conflicts)  # Store conflicts as the error

        jobs.append(job)
    return jobs


def _handle_failed_job(job: FileExportJob, failed_dir: str):
    """Copies the source file that failed to the failed_dir and logs its arguments."""
    try:
        # The file was never copied to the export dir, so we copy the original source
        failure_path = os.path.join(failed_dir, job.relative_path)
        unique_failure_path = find_unique_filepath(failure_path)
        os.makedirs(os.path.dirname(unique_failure_path), exist_ok=True)
        shutil.copyfile(job.source_location_to_copy.path, unique_failure_path)

        # Create the arguments log file
        args_log_path = os.path.splitext(unique_failure_path)[0] + ".txt"
        with open(args_log_path, 'w', encoding='utf-8') as f:
            f.write(f"Source file: {job.source_location_to_copy.path}\n")
            f.write(f"Intended destination: {job.final_output_path}\n\n")
            f.write(f"--- ExifTool Error ---\n{job.error_message}\n\n")
            f.write("--- Generated Arguments ---\n")
            args_list = job.get_exiftool_args_as_list()
            f.write("\n".join(args_list))
    except Exception as e:
        print(f"\nCRITICAL: Could not process failed job for {job.source_location_to_copy.path}: {e}")


def process_export_batch(
        batch_locations: List[models.Location],
        export_dir: str,
        conflict_dir: str,
        failed_dir: str,
        logger: logging.Logger,
        conflict_fp,
        pipeline: MergePipeline,
        processed_media_ids: set,
        owner: models.Owner,
        processed_ids_lock: threading.Lock,
        conflict_fp_lock: threading.Lock
) -> Tuple[Dict[str, int], int]:
    """
    Processes a batch of files using the new Job-based workflow.
    Returns a dictionary of stats.
    """
    # 1. Prepare job objects for all files in the batch
    jobs = _prepare_export_jobs(batch_locations, pipeline, owner, export_dir, processed_media_ids, processed_ids_lock)

    # 2. Handle conflicts: log them and copy to conflict_dir
    conflicted_jobs = [j for j in jobs if j.status == ExportStatus.CONFLICT]
    for job in conflicted_jobs:
        log_conflict(logger, job.source_location_to_copy.path, eval(job.error_message))
        with conflict_fp_lock:
            conflict_fp.write(f"{job.source_location_to_copy.path}\n")
            conflict_fp.flush()

        conflict_path = os.path.join(conflict_dir, job.relative_path)
        unique_conflict_path = find_unique_filepath(conflict_path)
        os.makedirs(os.path.dirname(unique_conflict_path), exist_ok=True)
        copy_file_task((job.source_location_to_copy.path, unique_conflict_path))

    # 3. Handle pending exports: Calculate final paths and run batch exiftool command
    jobs_to_export = [j for j in jobs if j.status == ExportStatus.PENDING_EXPORT]

    # Set final paths and create directories
    for job in jobs_to_export:
        initial_path = os.path.join(export_dir, job.relative_path)
        job.final_output_path = find_unique_filepath(initial_path)
        os.makedirs(os.path.dirname(job.final_output_path), exist_ok=True)

    # Batch write metadata. This function now handles the copy as well.
    if jobs_to_export:
        write_metadata_batch(jobs_to_export)

    # 4. Handle jobs that failed the exiftool step
    failed_jobs = [j for j in jobs_to_export if j.status == ExportStatus.FAILED]
    for job in failed_jobs:
        _handle_failed_job(job, failed_dir)

    # 5. Tally final stats from all job objects
    stats = {
        "exported": len([j for j in jobs if j.status == ExportStatus.SUCCESS]),
        "skipped": len([j for j in jobs if j.status == ExportStatus.SKIPPED]),
        "conflicts": len([j for j in jobs if j.status == ExportStatus.CONFLICT]),
        "failed": len([j for j in jobs if j.status == ExportStatus.FAILED]),
    }
    batch_size_bytes = sum(loc.file_size for loc in batch_locations)
    return stats, batch_size_bytes
# --- Main Execution ---

def export_main(owner_name: str, export_dir: str, filelist_path: str = None):
    """Main function to orchestrate the media export process."""
    if not shutil.which(CONFIG["EXIFTOOL_PATH"]):
        print(f"❌ ERROR: ExifTool not found at '{CONFIG['EXIFTOOL_PATH']}'. Please install it or update the path.")
        return

    print("Initializing...")
    os.makedirs(export_dir, exist_ok=True)

    conflict_dir = os.path.join(export_dir, "conflicted_files_for_review")
    os.makedirs(conflict_dir, exist_ok=True)

    failed_dir = os.path.join(export_dir, "failed_exports")
    os.makedirs(failed_dir, exist_ok=True)

    conflict_log_path = os.path.join(export_dir, 'export_conflicts.log')
    conflict_paths_file = os.path.join(export_dir, 'export_conflicts_paths.txt')

    if filelist_path and os.path.abspath(filelist_path) == os.path.abspath(conflict_paths_file):
        print("\n❌ SAFETY ERROR: The input file list is the same as the conflict output file.")
        print(f"   Input file: {filelist_path}")
        print("\n   Running the script would overwrite your input file.")
        print("   Please copy the list of paths to a different file and use that as your input.")
        sys.exit(1)  # Exit to prevent data loss

    conflict_logger = logging.getLogger('conflict_logger')
    conflict_logger.setLevel(logging.WARNING)
    fh = logging.FileHandler(conflict_log_path, mode='w', encoding='utf-8')
    fh.setLevel(logging.WARNING)
    formatter = logging.Formatter('%(asctime)s - FILE: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    conflict_logger.addHandler(fh)

    total_stats = {"exported": 0, "skipped": 0, "conflicts": 0, "failed": 0}
    processed_media_ids = set()


    export_merge_pipeline = MergePipeline.get_default_pipeline()

    processed_ids_lock = threading.Lock()
    conflict_fp_lock = threading.Lock()

    try:
        with SessionLocal() as db, open(conflict_paths_file, 'w', encoding='utf-8') as conflict_fp:

            locations_to_export = []
            if filelist_path:
                print(f"Reading file list from: {filelist_path}")
                with open(filelist_path, 'r', encoding='utf-8') as f:
                    paths = [line.strip() for line in f if line.strip()]
                    print(paths)
                locations_to_export = get_locations_by_paths(db, paths)
            elif owner_name:
                owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
                if not owner:
                    raise ValueError(f"Owner '{owner_name}' not found.")
                locations_to_export = get_locations_for_owner(db, owner)

            if not locations_to_export:
                print("No files found to process.")
                return

            total_size_bytes = sum(loc.file_size for loc in locations_to_export)
            total_files = len(locations_to_export)
            print(f"Found {total_files} files to process for export ({total_size_bytes / (1024 ** 3):.2f} GB).")

            with ThreadPoolExecutor(max_workers=CONFIG["MAX_WORKERS"]) as executor, \
                 tqdm(total=total_size_bytes, desc="Exporting Media", unit="B", unit_scale=True, unit_divisor=1024) as pbar:

                futures = []
                # Submit all batches to the thread pool
                for i in range(0, total_files, CONFIG["BATCH_SIZE"]):
                    batch = locations_to_export[i:i + CONFIG["BATCH_SIZE"]]
                    # Submit the job and pass the required locks
                    future = executor.submit(
                        process_export_batch,
                        batch, export_dir, conflict_dir, failed_dir,
                        conflict_logger, conflict_fp, export_merge_pipeline,
                        processed_media_ids, owner,
                        processed_ids_lock, conflict_fp_lock
                    )
                    futures.append(future)

                # Process results as they are completed
                for future in as_completed(futures):
                    try:
                        stats, processed_bytes = future.result()
                        # Update totals and progress bar
                        for key in total_stats:
                            total_stats[key] += stats[key]
                        pbar.update(processed_bytes)
                        pbar.set_postfix(exported=total_stats['exported'], skipped=total_stats['skipped'],
                                         conflicts=total_stats['conflicts'], failed=total_stats['failed'])
                    except Exception as e:
                        print(f"\nCRITICAL ERROR in worker thread: {e}")
    finally:
        print("\n--- Export Complete ---")
        print(f"✅ Successfully exported {total_stats['exported']} new files.")
        print(f"⏩ Skipped {total_stats['skipped']}.")
        if total_stats['conflicts'] > 0:
            print(
                f"⚠️ Encountered {total_stats['conflicts']} merge conflicts. These files were copied WITHOUT metadata to the '{os.path.basename(conflict_dir)}' subfolder for manual review.")
            print(f"   See the full list of conflicts in the log file: {conflict_log_path}")
            print(f"   A list of conflicted file paths has been saved to: {conflict_paths_file}")

        if total_stats['failed'] > 0:
            # UPDATE THIS MESSAGE for more clarity
            print(f"❌ Failed to export {total_stats['failed']} files due to copy or metadata errors.")
            print(
                f"   These files, along with their intended ExifTool arguments, have been saved to the '{os.path.basename(failed_dir)}' folder for inspection.")

        print("-----------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media files with merged metadata from a database.")
    parser.add_argument("export_dir", type=str, help="The target directory for the export.")
    parser.add_argument("--owner", type=str, help="The name of the owner whose files to export.", required=True)
    parser.add_argument("--filelist", "-f", type=str,
                        help="Optional path to a file with absolute file paths to export.")

    args = parser.parse_args()

    if not args.owner and not args.filelist:
        parser.error("Either an --owner or the --filelist argument must be provided.")

    export_main(args.owner, args.export_dir, args.filelist)