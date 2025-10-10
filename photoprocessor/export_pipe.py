import os
import argparse
import shutil
import subprocess
import logging
from datetime import datetime, timezone
from fileinput import filename
from typing import List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor
import sys
import re

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
    "BATCH_SIZE": 50,
    "MAX_COPY_WORKERS": 8,
}


# --- End Configuration ---

class MergeConflictException(Exception):
    def __init__(self, message, conflicts):
        super().__init__(message)
        self.conflicts = conflicts


# --- Core Functions ---

def get_locations_for_owner(db: Session, owner: models.Owner) -> List[models.Location]:
    """Queries all locations owned by a person with all necessary related data eagerly loaded."""
    print(f"Querying files for owner: {owner.name}...")
    ownership_records = db.query(models.MediaOwnership).filter(
        models.MediaOwnership.owner_id == owner.id
    ).options(
        selectinload(models.MediaOwnership.location).selectinload(models.Location.media_file).options(
            # This is the full chain to load everything needed
            selectinload(models.MediaFile.locations).options(
                selectinload(models.Location.owners),  # <-- ADD THIS LINE
                selectinload(models.Location.metadata_sources).selectinload(
                    models.MetadataSource.entries)
            )
        )
    ).all()
    return [record.location for record in ownership_records]


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
                selectinload(models.Location.metadata_entries).selectinload(
                    models.MetadataSource.entries)
            )
        )
    ).all()


def _generate_exiftool_args_for_file(export_arguments: List[ExportArgument]) -> List[str]:
    """
    Generates the list of ExifTool command-line args for a single file's metadata
    by calling the build() method on each ExportArgument object.
    """
    args = []
    for arg_object in export_arguments:
        args.extend(arg_object.build())
    return args


def write_metadata_batch(files_to_process: List[Tuple[str, List[ExportArgument]]]):
    """Writes metadata to a batch of files using a single ExifTool command."""
    if not files_to_process:
        return
    base_args = [CONFIG["EXIFTOOL_PATH"], "-overwrite_original", "-common_args"]
    command_args = []

    for output_path, export_args in files_to_process:
        if not export_args:
            continue
        file_args = _generate_exiftool_args_for_file(export_args)
        if file_args:
            command_args.extend(file_args)
            command_args.append(output_path)
            command_args.append("-execute")

    if not command_args:
        return

    final_args = base_args + command_args[:-1]
    try:
        subprocess.run(final_args, check=True, capture_output=True, text=True, encoding='utf-8')
    except subprocess.CalledProcessError as e:
        print(
            f"\n--- ExifTool Batch Error ---\nError applying metadata to a batch of files.\nError: {e.stderr}\n--------------------------")
        raise


def copy_file_task(src_dst_tuple: Tuple[str, str]):
    """Simple wrapper for shutil.copy2 for use with ThreadPoolExecutor."""
    try:
        shutil.copy2(*src_dst_tuple)
    except Exception as e:
        return src_dst_tuple[0], e
    return src_dst_tuple[0], None


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


def generate_relative_export_path(media_file: models.MediaFile, export_arguments: List[ExportArgument], owner: models.Owner) -> str:
    """
    Generates the full relative export path for a media file based on a prioritized
    set of rules, falling back to a year-based structure using the merged metadata.
    """

    filename = media_file.locations[0].filename

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

        return os.path.join(target_subdir, filename)


    ### --- Second Priority: Check for Known Folder Patterns in Any Location Path --- ###

    # --- DEFINE YOUR FOLDER HIERARCHY HERE ---
    priority_rules = [
        # (pattern_to_find_in_path, target_export_subdirectory)
        ('whatsapp images/sent', os.path.join("Whatsapp Images", "Sent")),
        ('whatsapp video/sent', os.path.join("Whatsapp Video", "Sent")),
        ('whatsapp images', "Whatsapp Images"),
        ('whatsapp video', "Whatsapp Video"),
        ('dcim/camera', "Camera"),
        ('screenshots', "Screenshots"),
    ]

    all_paths = [loc.path.lower().replace('\\', '/') for loc in media_file.locations]

    # 1. Check paths against priority rules
    for pattern, target_subdir in priority_rules:
        for path in all_paths:
            if pattern in path:
                return os.path.join(target_subdir, filename)  # Return full relative path

    # 2. Check filename pattern
    if re.search(r'-WA\d{4}', filename, re.IGNORECASE):
        if media_file.mime_type.startswith('video/'):
            return os.path.join("Whatsapp Video", filename)
        else:
            return os.path.join("Whatsapp Images", filename)

    # - check screenshots in filename
    if re.search(r'screenshot', filename, re.IGNORECASE):
        return os.path.join("Screenshots", filename)

    # --- 3. Default to Year-Based Pathing using Merged Date ---

    date_taken = None
    date_modified = None
    for arg in export_arguments:
        if isinstance(arg, DateTimeArgument):
            if arg.date_type == "taken" and isinstance(arg.value, datetime):
                date_taken = arg.value
            elif arg.date_type == "modified" and isinstance(arg.value, datetime):
                date_modified = arg.value

    if date_taken and isinstance(date_taken, datetime):
        year_str = str(date_taken.year)
        return os.path.join(year_str, filename)
    elif date_modified and isinstance(date_modified, datetime):
        year_str = str(date_modified.year)
        return os.path.join(year_str, filename)

    # 4. Final fallback for files with no usable date information
    return os.path.join("Unknown_Date", filename)


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

def process_export_batch(
        batch_locations: List[models.Location],
        export_dir: str,
        conflict_dir: str,
        executor: ThreadPoolExecutor,
        logger: logging.Logger,
        conflict_fp,
        pipeline: MergePipeline,
        processed_media_ids: set,
        owner: models.Owner
) -> Dict[str, Any]:
    """Processes a single batch of files: merge, parallel copy, and batch metadata write."""
    stats = {"exported": 0, "skipped": 0, "conflicts": 0}
    files_to_copy = []
    files_to_copy_conflict = []
    files_for_metadata: List[Tuple[str, List[ExportArgument]]] = []

    for loc in batch_locations:
        if loc.media_file.id in processed_media_ids:
            stats["skipped"] += 1
            continue

        # Get all locations for this media file
        all_locations_for_file = loc.media_file.locations

        # Sort them by file size (desc) and then by location ID (asc)
        # We use os.path.getsize() for the most accurate current size.
        sorted_locations = sorted(
            all_locations_for_file,
            key=lambda l: (-l.file_size, l.id)
        )

        # The best location to copy from is the first one in the sorted list
        source_loc_to_copy = sorted_locations[0]

        # IMPORTANT: A file's canonical metadata is derived from ALL its locations.
        all_sources_for_file = [source for location in loc.media_file.locations for source in location.metadata_sources]
        if not all_sources_for_file:
            stats["skipped"] += 1
            continue

        result_context = pipeline.run(all_sources_for_file)

        # Get the final arguments. This also runs the conflict check internally.
        final_arguments = result_context.get_all_arguments()
        relative_path = generate_relative_export_path(loc.media_file, final_arguments, owner)

        # Check for any conflicts recorded during the merge process OR by the argument validation.
        if result_context.conflicts:
            stats["conflicts"] += 1
            log_conflict(logger, loc.path, result_context.conflicts)
            conflict_fp.write(f"{loc.path}\n")
            conflict_fp.flush()
            initial_conflict_path = os.path.join(conflict_dir, relative_path)
            unique_conflict_path = find_unique_filepath(initial_conflict_path)

            os.makedirs(os.path.dirname(unique_conflict_path), exist_ok=True)
            files_to_copy_conflict.append((source_loc_to_copy.path, unique_conflict_path))
            continue

        # Pass the raw merged_data dict for path generation, as it contains simple values.
        initial_output_path = os.path.join(export_dir, relative_path)
        unique_output_path = find_unique_filepath(initial_output_path)

        output_dir_for_file = os.path.dirname(unique_output_path)
        os.makedirs(output_dir_for_file, exist_ok=True)

        files_to_copy.append((source_loc_to_copy.path, unique_output_path))
        files_for_metadata.append((unique_output_path, final_arguments))
        processed_media_ids.add(loc.media_file.id)

    if files_to_copy:
        copy_results = executor.map(copy_file_task, files_to_copy)
        for src, error in copy_results:
            if error:
                print(f"\nError copying {src}: {error}")
            else:
                stats["exported"] += 1

    if files_to_copy_conflict:
        # We just need to execute the copy, we already counted the conflicts
        for src, error in executor.map(copy_file_task, files_to_copy_conflict):
            if error:
                print(f"\nError copying conflicted file {src}: {error}")

    if files_for_metadata:
        try:
            write_metadata_batch(files_for_metadata)
        except Exception as e:
            print(f"\nMetadata batch write failed: {e}")
    return stats


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

    total_stats = {"exported": 0, "skipped": 0, "conflicts": 0}
    processed_media_ids = set()


    export_merge_pipeline = MergePipeline.get_default_pipeline()

    try:
        with SessionLocal() as db, ThreadPoolExecutor(max_workers=CONFIG["MAX_COPY_WORKERS"]) as executor, \
                open(conflict_paths_file, 'w', encoding='utf-8') as conflict_fp:

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

            with tqdm(total=total_size_bytes, desc="Exporting Media", unit="B", unit_scale=True,
                      unit_divisor=1024) as pbar:
                for i in range(0, total_files, CONFIG["BATCH_SIZE"]):
                    batch = locations_to_export[i:i + CONFIG["BATCH_SIZE"]]
                    batch_size_bytes = sum(loc.file_size for loc in batch)
                    stats = process_export_batch(batch, export_dir, conflict_dir, executor, conflict_logger, conflict_fp, export_merge_pipeline, processed_media_ids, owner)
                    for key in total_stats:
                        total_stats[key] += stats[key]
                    pbar.update(batch_size_bytes)
                    pbar.set_postfix(exported=total_stats['exported'], skipped=total_stats['skipped'],
                                     conflicts=total_stats['conflicts'])
    finally:
        print("\n--- Export Complete ---")
        print(f"✅ Successfully exported {total_stats['exported']} new files.")
        print(f"⏩ Skipped {total_stats['skipped']} files that already existed in the destination.")
        if total_stats['conflicts'] > 0:
            print(f"⚠️ Encountered {total_stats['conflicts']} merge conflicts. These files were copied WITHOUT metadata to the '{os.path.basename(conflict_dir)}' subfolder for manual review.")
            print(f"   See the full list of conflicts in the log file: {conflict_log_path}")
            print(f"   A list of conflicted file paths has been saved to: {conflict_paths_file}")
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