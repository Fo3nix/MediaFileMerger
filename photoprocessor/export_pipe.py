import os
import argparse
import shutil
import subprocess
import logging
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor
import sys

from tqdm import tqdm
from sqlalchemy.orm import Session, selectinload
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models
from photoprocessor.merger import MergeStep, GPSMergeStep, MergeContext, BasicFieldMergeStep, MergePipeline, \
    DateTimeAndZoneMergeStep

# --- Configuration ---
CONFIG = {
    "EXIFTOOL_PATH": "exiftool",
    "BATCH_SIZE": 75,
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
        selectinload(models.MediaOwnership.location).selectinload(models.Location.media_file).selectinload(
            models.MediaFile.metadata_sources)
    ).all()
    return [record.location for record in ownership_records]


def get_locations_by_paths(db: Session, paths: List[str]) -> List[models.Location]:
    """Queries for specific locations based on a list of file paths."""
    print(f"Querying for {len(paths)} specific file paths...")
    return db.query(models.Location).filter(
        models.Location.path.in_(paths)
    ).options(
        selectinload(models.Location.media_file).selectinload(models.MediaFile.metadata_sources)
    ).all()


def _generate_exiftool_args_for_file(metadata: Dict[str, Any]) -> List[str]:
    """Generates the list of ExifTool command-line args for a single file's metadata."""
    args = []
    tag_map = {"gps_latitude": "-GPSLatitude", "gps_longitude": "-GPSLongitude"}

    # --- Date Taken ---
    date_taken = metadata.get("date_taken")
    if date_taken and isinstance(date_taken, datetime):
        # Format for EXIF/File dates (local time, no offset)
        local_time_str = date_taken.strftime('%Y:%m:%d %H:%M:%S')
        args.extend([
            f"-EXIF:DateTimeOriginal={local_time_str}",
            f"-EXIF:CreateDate={local_time_str}",
            f"-FileCreateDate={local_time_str}",
        ])

        # If the date is timezone-aware, write additional offset and UTC tags
        if date_taken.tzinfo:
            offset_str = date_taken.strftime('%z')
            offset_str_formatted = f"{offset_str[:3]}:{offset_str[3:]}"
            utc_date = date_taken.astimezone(timezone.utc)
            utc_time_str = utc_date.strftime('%Y:%m:%d %H:%M:%S')

            args.extend([
                f"-EXIF:OffsetTimeOriginal={offset_str_formatted}",
                f"-XMP:DateTimeOriginal={date_taken.isoformat()}",  # XMP uses full ISO 8601
                f"-QuickTime:CreateDate={utc_time_str}",
                f"-Keys:CreationDate={utc_time_str}",
            ])

    # --- Date Modified ---
    date_modified = metadata.get("date_modified")
    if date_modified and isinstance(date_modified, datetime):
        local_mod_time_str = date_modified.strftime('%Y:%m:%d %H:%M:%S')
        args.extend([
            f"-EXIF:ModifyDate={local_mod_time_str}",
            f"-FileModifyDate={local_mod_time_str}",
        ])

        # If the date is aware, write UTC and XMP tags
        if date_modified.tzinfo:
            utc_mod_date = date_modified.astimezone(timezone.utc)
            utc_mod_time_str = utc_mod_date.strftime('%Y:%m:%d %H:%M:%S')
            args.extend([
                f"-XMP:ModifyDate={date_modified.isoformat()}",
                f"-QuickTime:ModifyDate={utc_mod_time_str}",
            ])

    # --- GPS and other tags ---
    for key, value in metadata.items():
        if key in tag_map and value is not None:
            args.append(f"{tag_map[key]}={value}")
    return args


def write_metadata_batch(files_to_process: List[Tuple[str, Dict]]):
    """Writes metadata to a batch of files using a single ExifTool command."""
    if not files_to_process:
        return
    base_args = [CONFIG["EXIFTOOL_PATH"], "-overwrite_original", "-common_args"]
    command_args = []
    for output_path, merged_meta in files_to_process:
        if not merged_meta:
            continue
        file_args = _generate_exiftool_args_for_file(merged_meta)
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


def process_export_batch(
        batch_locations: List[models.Location],
        export_dir: str,
        conflict_dir: str,
        executor: ThreadPoolExecutor,
        logger: logging.Logger,
        conflict_fp,  # File pointer for writing conflict paths
        pipeline: MergePipeline
) -> Dict[str, Any]:
    """Processes a single batch of files: merge, parallel copy, and batch metadata write."""
    stats = {"exported": 0, "skipped": 0, "conflicts": 0}
    files_to_copy = []
    files_to_copy_conflict = []
    files_for_metadata = []

    for loc in batch_locations:
        output_path = os.path.join(export_dir, loc.filename)
        if os.path.exists(output_path):
            stats["skipped"] += 1
            continue

        # Run the pipeline
        metadata_sources = loc.media_file.metadata_sources
        if not metadata_sources:
            continue  # Or handle as needed

        result_context = pipeline.run(metadata_sources)

        if result_context.conflicts:
            stats["conflicts"] += 1
            log_conflict(logger, loc.path, result_context.conflicts)
            conflict_fp.write(f"{loc.path}\n")
            conflict_fp.flush()
            conflict_output_path = os.path.join(conflict_dir, loc.filename)
            if not os.path.exists(conflict_output_path):
                files_to_copy_conflict.append((loc.path, conflict_output_path))
            continue

        files_to_copy.append((loc.path, output_path))
        files_for_metadata.append((output_path, result_context.merged_data))

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


    export_merge_pipeline = MergePipeline(steps=[
        GPSMergeStep(),
        DateTimeAndZoneMergeStep("date_taken"),
        DateTimeAndZoneMergeStep("date_modified"),
    ])



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

            total_size_bytes = sum(loc.media_file.file_size for loc in locations_to_export)
            total_files = len(locations_to_export)
            print(f"Found {total_files} files to process for export ({total_size_bytes / (1024 ** 3):.2f} GB).")

            with tqdm(total=total_size_bytes, desc="Exporting Media", unit="B", unit_scale=True,
                      unit_divisor=1024) as pbar:
                for i in range(0, total_files, CONFIG["BATCH_SIZE"]):
                    batch = locations_to_export[i:i + CONFIG["BATCH_SIZE"]]
                    batch_size_bytes = sum(loc.media_file.file_size for loc in batch)
                    stats = process_export_batch(batch, export_dir, conflict_dir, executor, conflict_logger, conflict_fp, export_merge_pipeline)
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
    parser.add_argument("--owner", type=str, help="The name of the owner whose files to export.")
    parser.add_argument("--filelist", "-f", type=str,
                        help="Optional path to a file with absolute file paths to export.")

    args = parser.parse_args()

    if not args.owner and not args.filelist:
        parser.error("Either an --owner or the --filelist argument must be provided.")

    export_main(args.owner, args.export_dir, args.filelist)