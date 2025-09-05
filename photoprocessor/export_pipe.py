import os
import argparse
import shutil
import subprocess
from datetime import datetime
from typing import List, Dict, Tuple, Any

from tqdm import tqdm
from sqlalchemy.orm import Session, joinedload
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models


# --- Configuration ---
CONFIG = {
    # Path to the ExifTool executable.
    # On Windows, it's 'exiftool.exe'. On Linux/macOS, it's just 'exiftool'.
    # Ensure this is in your system's PATH or provide a full path.
    "EXIFTOOL_PATH": "exiftool",
}


# --- End Configuration ---

class MergeConflictException(Exception):
    """Custom exception to signal a conflict during a metadata merge."""

    def __init__(self, message, conflicts):
        super().__init__(message)
        self.conflicts = conflicts


def get_owner(db: Session, name: str) -> models.Owner:
    """Retrieves an owner by name from the database."""
    owner = db.query(models.Owner).filter(models.Owner.name == name).first()
    if not owner:
        raise ValueError(f"Owner '{name}' not found in the database.")
    return owner


def get_files_for_owner(db: Session, owner: models.Owner) -> List[models.MediaOwnership]:
    """Queries all media ownership records for a given owner."""
    print(f"Querying files for owner: {owner.name}...")
    return db.query(models.MediaOwnership).options(
        joinedload(models.MediaOwnership.media_file).joinedload(models.MediaFile.processed_metadata),
        joinedload(models.MediaOwnership.media_file).joinedload(models.MediaFile.google_metadata)
    ).filter(models.MediaOwnership.owner_id == owner.id).all()


def merge_export_metadata(metadata: models.Metadata, google_meta: models.GooglePhotosMetadata) -> Dict:
    """
    Performs a symmetrical merge of Metadata and GooglePhotosMetadata.

    Raises:
        MergeConflictException if any non-empty attributes have differing values.
    """
    conflicts = {}
    merged_data = {}

    def _is_empty(value):
        return value is None or value == '' or value == 0

    def _merge_attr(attr_name, val1, val2):
        if _is_empty(val1) and _is_empty(val2):
            return None
        if _is_empty(val1):
            return val2
        if _is_empty(val2):
            return val1
        if val1 == val2:
            return val1

        conflicts[attr_name] = (val1, val2)
        return None

    # Define all fields to be merged
    fields = [
        "title", "description", "gps_latitude", "gps_longitude", "is_favorite"
    ]

    for field in fields:
        val_meta = getattr(metadata, field, None)
        val_google = getattr(google_meta, field, None)
        merged_data[field] = _merge_attr(field, val_meta, val_google)

    # Handle date separately as the names differ
    merged_data['date_taken'] = _merge_attr(
        'date_taken',
        getattr(metadata, 'date_taken', None),
        getattr(google_meta, 'creation_timestamp', None)
    )

    # Add fields that only exist in one source
    merged_data.update({
        "camera_make": getattr(metadata, 'camera_make', None),
        "camera_model": getattr(metadata, 'camera_model', None),
        "lens_model": getattr(metadata, 'lens_model', None),
        "width": getattr(metadata, 'width', None),
        "height": getattr(metadata, 'height', None),
        "aperture": getattr(metadata, 'aperture', None),
        "focal_length": getattr(metadata, 'focal_length', None),
        "iso": getattr(metadata, 'iso', None),
    })

    if conflicts:
        raise MergeConflictException("Metadata conflicts found", conflicts)

    return {k: v for k, v in merged_data.items() if v is not None}


def write_metadata_with_exiftool(filepath: str, metadata: Dict, date_taken: datetime):
    """
    Writes metadata to a file using the exiftool command-line utility.
    """
    args = [CONFIG["EXIFTOOL_PATH"], "-overwrite_original"]

    # Map our merged data keys to ExifTool tag names
    tag_map = {
        "title": "-Title",
        "description": "-Description",
        "date_taken": "-AllDates",  # Sets EXIF:DateTimeOriginal, CreateDate, and ModifyDate
        "gps_latitude": "-GPSLatitude",
        "gps_longitude": "-GPSLongitude",
        "camera_make": "-Make",
        "camera_model": "-Model",
        "lens_model": "-LensModel",
        "aperture": "-FNumber",
        "focal_length": "-FocalLength",
        "iso": "-ISO",
    }

    for key, value in metadata.items():

        # Sanitize string values to remove null characters before passing to subprocess
        if isinstance(value, str):
            value = value.replace('\x00', '')

        if key in tag_map:
            if key == 'date_taken':
                # Format datetime for exiftool
                args.append(f"{tag_map[key]}={value.strftime('%Y:%m:%d %H:%M:%S')}")
            else:
                args.append(f"{tag_map[key]}={value}")

    # Set the file system created and modified dates using ExifTool
    if date_taken:
        formatted_date = date_taken.strftime('%Y:%m:%d %H:%M:%S')
        args.append(f"-FileCreateDate={formatted_date}")
        args.append(f"-FileModifyDate={formatted_date}")

    args.append(filepath)

    try:
        subprocess.run(args, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        print(f"\nError running ExifTool for {os.path.basename(filepath)}.")
        print(f"Command: {' '.join(e.cmd)}")
        print(f"Error: {e.stderr}")
        raise

def export_main(owner_name: str, export_dir: str):
    """Main function to orchestrate the media export process."""
    if not shutil.which(CONFIG["EXIFTOOL_PATH"]):
        print(f"❌ ERROR: '{CONFIG['EXIFTOOL_PATH']}' not found in your system's PATH.")
        print("Please install ExifTool and ensure it is accessible to run the export.")
        return

    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)

    # ADDED: Ensure the main export directory exists before starting.
    os.makedirs(export_dir, exist_ok=True)

    all_failures = []
    exported_count = 0
    skipped_count = 0
    failed_count = 0

    try:
        with SessionLocal() as db:
            owner = get_owner(db, owner_name)
            files_to_export = get_files_for_owner(db, owner)

            total_files = len(files_to_export)
            if not total_files:
                print(f"No files found for owner '{owner_name}'.")
                return

            print(f"Found {total_files} files to potentially export.")

            with tqdm(total=total_files, desc="Exporting Media", unit="file") as pbar:
                for ownership in files_to_export:
                    pbar.update(1)

                    try:
                        merged_meta = {}
                        media_file = ownership.media_file

                        has_db_meta = media_file.processed_metadata is not None
                        has_google_meta = media_file.google_metadata is not None

                        if has_db_meta and has_google_meta:
                            merged_meta = merge_export_metadata(
                                media_file.processed_metadata,
                                media_file.google_metadata
                            )
                        elif has_db_meta or has_google_meta:
                            source = media_file.processed_metadata or media_file.google_metadata
                            merged_meta = {
                                "title": getattr(source, 'title', None),
                                "description": getattr(source, 'description', None),
                                "date_taken": getattr(source, 'date_taken', None) or getattr(source,
                                                                                             'creation_timestamp',
                                                                                             None),
                                "gps_latitude": getattr(source, 'gps_latitude', None),
                                "gps_longitude": getattr(source, 'gps_longitude', None),
                                "camera_make": getattr(source, 'camera_make', None),
                                "camera_model": getattr(source, 'camera_model', None),
                            }

                        date_taken = merged_meta.get('date_taken')
                        if not date_taken:
                            mtime = os.path.getmtime(ownership.location)
                            date_taken = datetime.fromtimestamp(mtime)
                            merged_meta['date_taken'] = date_taken

                        # --- MODIFIED LOGIC: Simplified output path ---
                        # The file will be placed directly in the export_dir with its original name.
                        output_path = os.path.join(export_dir, ownership.filename)

                        if os.path.exists(output_path):
                            skipped_count += 1

                            pbar.set_postfix(exported=exported_count, skipped=skipped_count, failed=len(all_failures))
                            continue

                        shutil.copy2(ownership.location, output_path)
                        write_metadata_with_exiftool(output_path, merged_meta, date_taken)

                        exported_count += 1
                        pbar.set_postfix(exported=exported_count, skipped=skipped_count, failed=len(all_failures))

                    except MergeConflictException as e:
                        all_failures.append({"location": ownership.location, "conflicts": e.conflicts})
                        pbar.set_postfix(exported=exported_count, skipped=skipped_count, failed=len(all_failures))
                    except Exception as e:
                        failed_count += 1
                        print(f"\nAn unexpected error occurred while processing {ownership.location}: {e}")

    finally:
        # Final report is unchanged
        print("\nExport complete!")
        total_exported = exported_count
        print(f"✅ Successfully exported {total_exported} files in total.")
        print(f"⏩ Skipped {skipped_count} files that already existed in the destination.")
        print(f"❌ Failed to export {failed_count} files due to errors.")
        if all_failures:
            print(f"⚠️ Encountered {len(all_failures)} merge conflicts. These files were skipped:")
            for failure in all_failures:
                print(f"\n  - File: {failure['location']}")
                for key, values in failure['conflicts'].items():
                    print(f"    - Conflict on '{key}': DB='{values[0]}', Google='{values[1]}'")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media files with merged metadata.")
    parser.add_argument("owner_name", type=str, help="The name of the owner whose files to export.")
    parser.add_argument("export_dir", type=str, help="The target directory for the export.")

    args = parser.parse_args()
    export_main(args.owner_name, args.export_dir)