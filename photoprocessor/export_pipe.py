import os
import argparse
import shutil
import subprocess
from datetime import datetime, timezone
from typing import List, Dict, Tuple, Any
from concurrent.futures import ThreadPoolExecutor

from tqdm import tqdm
from sqlalchemy.orm import Session, selectinload
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models
from photoprocessor.merge_rules import rules

# --- Configuration ---
CONFIG = {
    "EXIFTOOL_PATH": "exiftool",
    "BATCH_SIZE": 75,  # Process files in chunks for efficiency
    "MAX_COPY_WORKERS": 8,  # Number of parallel file copy operations
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
    # Eagerly load all relationships needed for the export process in a single query
    ownership_records = db.query(models.MediaOwnership).filter(
        models.MediaOwnership.owner_id == owner.id
    ).options(
        selectinload(models.MediaOwnership.location).selectinload(models.Location.media_file).selectinload(
            models.MediaFile.metadata_sources)
    ).all()
    return [record.location for record in ownership_records]


def merge_metadata_for_export(main_source: models.Metadata, metadata_sources: List[models.Metadata]) -> Tuple[
    Dict, Dict]:
    """
    Merges multiple metadata sources into a single dictionary for export.
    (This function remains unchanged but is used by the new batch processor)
    """

    def source_as_dict(source: models.Metadata) -> Dict[str, Any]:
        if not source:
            return {}
        return {
            "date_taken": source.date_taken,
            "gps_latitude": source.gps_latitude,
            "gps_longitude": source.gps_longitude,
        }

    main_dict = source_as_dict(main_source)

    if not metadata_sources:
        return main_dict, {}

    merged = main_dict.copy()
    conflicts = {}

    for source in metadata_sources:
        src_dict = source_as_dict(source)
        for key, value in src_dict.items():
            if value is None:
                continue

            if key not in merged or merged[key] is None:
                merged[key] = value
            elif not rules.compare(key, merged[key], value):
                # If values are not equivalent, record a conflict.
                if key not in conflicts:
                    conflicts[key] = {merged[key]}
                conflicts[key].add(value)

    return merged, conflicts


def _generate_exiftool_args_for_file(metadata: Dict[str, Any]) -> List[str]:
    """Generates the list of ExifTool command-line args for a single file's metadata."""
    args = []
    tag_map = {
        "gps_latitude": "-GPSLatitude",
        "gps_longitude": "-GPSLongitude",
    }

    # Handle date_taken separately as it writes to multiple tags
    date_taken = metadata.get("date_taken")
    if date_taken and isinstance(date_taken, datetime):
        # Format for EXIF (Local Time + Offset)
        local_time_str = date_taken.strftime('%Y:%m:%d %H:%M:%S')
        offset_str = date_taken.strftime('%z')
        offset_str_formatted = f"{offset_str[:3]}:{offset_str[3:]}"

        # Format for QuickTime/HEIC (UTC)
        utc_date = date_taken.astimezone(timezone.utc)
        utc_time_str = utc_date.strftime('%Y:%m:%d %H:%M:%S')

        args.extend([
            f"-EXIF:DateTimeOriginal={local_time_str}",
            f"-EXIF:CreateDate={local_time_str}",
            f"-EXIF:OffsetTimeOriginal={offset_str_formatted}",
            f"-QuickTime:CreateDate={utc_time_str}",
            f"-QuickTime:ModifyDate={utc_time_str}",
            f"-Keys:CreationDate={utc_time_str}",  # For HEIC files
            f"-FileModifyDate={local_time_str}",
        ])

    # Handle other tags
    for key, value in metadata.items():
        if key in tag_map and value is not None:
            args.append(f"{tag_map[key]}={value}")

    return args


def write_metadata_batch(files_to_process: List[Tuple[str, Dict]]):
    """
    Writes metadata to a batch of files using a single ExifTool command.
    Uses the -execute flag to process multiple files with different tags.
    """
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
        return  # Nothing to do

    # Remove the last, unnecessary '-execute'
    final_args = base_args + command_args[:-1]

    try:
        subprocess.run(final_args, check=True, capture_output=True, text=True, encoding='utf-8')
    except subprocess.CalledProcessError as e:
        print("\n--- ExifTool Batch Error ---")
        print(f"Error applying metadata to a batch of files.")
        print(f"Error: {e.stderr}")
        print("--------------------------")
        raise


def copy_file_task(src_dst_tuple: Tuple[str, str]):
    """Simple wrapper for shutil.copy2 for use with ThreadPoolExecutor."""
    try:
        shutil.copy2(*src_dst_tuple)
    except Exception as e:
        # This allows the main thread to catch errors from the copy workers
        return src_dst_tuple[0], e
    return src_dst_tuple[0], None


def process_export_batch(
        batch_locations: List[models.Location],
        export_dir: str,
        executor: ThreadPoolExecutor
) -> Dict[str, Any]:
    """
    Processes a single batch of files: merge, parallel copy, and batch metadata write.
    """
    stats = {"exported": 0, "skipped": 0, "conflicts": 0}
    batch_failures = []

    # 1. Merge metadata and filter existing files
    files_to_copy = []
    files_for_metadata = []

    for loc in batch_locations:
        output_path = os.path.join(export_dir, loc.filename)
        if os.path.exists(output_path):
            stats["skipped"] += 1
            continue

        # Perform metadata merge
        main_source = loc.media_file.metadata_sources[0] if loc.media_file.metadata_sources else None
        other_sources = loc.media_file.metadata_sources[1:]
        merged_meta, conflicts = merge_metadata_for_export(main_source, other_sources)

        if conflicts:
            stats["conflicts"] += 1
            batch_failures.append({"location": loc.path, "conflicts": conflicts})
            continue

        files_to_copy.append((loc.path, output_path))
        files_for_metadata.append((output_path, merged_meta))

    # 2. Copy files in parallel
    if files_to_copy:
        copy_results = executor.map(copy_file_task, files_to_copy)
        for src, error in copy_results:
            if error:
                print(f"\nError copying {src}: {error}")
            else:
                stats["exported"] += 1

    # 3. Write metadata to the copied files in a single batch command
    if files_for_metadata:
        try:
            write_metadata_batch(files_for_metadata)
        except Exception as e:
            # If exiftool fails, we log it, but the files are already copied.
            # We don't decrement the 'exported' count.
            print(f"\nMetadata batch write failed: {e}")

    return stats, batch_failures


# --- Main Execution ---

def export_main(owner_name: str, export_dir: str):
    """Main function to orchestrate the media export process."""
    if not shutil.which(CONFIG["EXIFTOOL_PATH"]):
        print(f"❌ ERROR: ExifTool not found at '{CONFIG['EXIFTOOL_PATH']}'. Please install it or update the path.")
        return

    print("Initializing...")
    os.makedirs(export_dir, exist_ok=True)

    total_stats = {"exported": 0, "skipped": 0, "conflicts": 0}
    all_failures = []

    try:
        with SessionLocal() as db, ThreadPoolExecutor(max_workers=CONFIG["MAX_COPY_WORKERS"]) as executor:
            owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
            if not owner:
                raise ValueError(f"Owner '{owner_name}' not found.")

            locations_to_export = get_locations_for_owner(db, owner)
            if not locations_to_export:
                print(f"No files found for owner '{owner_name}'.")
                return

            # Sum the file_size from each MediaFile object associated with the locations.
            total_size_bytes = sum(loc.media_file.file_size for loc in locations_to_export)
            total_files = len(locations_to_export)  # Still useful for the final printout
            print(f"Found {total_files} files to process for export ({total_size_bytes / (1024 ** 3):.2f} GB).")

            with tqdm(total=total_size_bytes, desc="Exporting Media", unit="B", unit_scale=True, unit_divisor=1024) as pbar:
                for i in range(0, total_files, CONFIG["BATCH_SIZE"]):
                    batch = locations_to_export[i:i + CONFIG["BATCH_SIZE"]]

                    batch_size_bytes = sum(loc.media_file.file_size for loc in batch)

                    stats, failures = process_export_batch(batch, export_dir, executor)

                    # Aggregate results
                    for key in total_stats:
                        total_stats[key] += stats[key]
                    all_failures.extend(failures)

                    pbar.update(batch_size_bytes)
                    pbar.set_postfix(
                        exported=total_stats['exported'],
                        skipped=total_stats['skipped'],
                        conflicts=total_stats['conflicts']
                    )
    finally:
        print("\n--- Export Complete ---")
        print(f"✅ Successfully exported {total_stats['exported']} new files.")
        print(f"⏩ Skipped {total_stats['skipped']} files that already existed in the destination.")
        if total_stats['conflicts'] > 0:
            print(f"⚠️ Encountered {total_stats['conflicts']} merge conflicts. These files were NOT exported:")
            for failure in all_failures[:10]:  # Print first 10 conflicts
                print(f"\n  - File: {failure['location']}")
                for key, values in failure['conflicts'].items():
                    print(f"    - Conflict on '{key}': Values were {values}")
            if len(all_failures) > 10:
                print(f"    ... and {len(all_failures) - 10} more conflicts.")
        print("-----------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media files with merged metadata, optimized for speed.")
    parser.add_argument("owner_name", type=str, help="The name of the owner whose files to export.")
    parser.add_argument("export_dir", type=str, help="The target directory for the export.")
    args = parser.parse_args()
    export_main(args.owner_name, args.export_dir)