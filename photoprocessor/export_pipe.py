import os
import argparse
import shutil
import subprocess
from datetime import datetime
from typing import List, Dict, Tuple, Any

from tqdm import tqdm
from sqlalchemy.orm import Session, joinedload, selectinload
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
    def __init__(self, message, conflicts):
        super().__init__(message)
        self.conflicts = conflicts


def get_locations_for_owner(db: Session, owner: models.Owner) -> List[models.Location]:
    """Queries all locations owned by a person."""
    print(f"Querying files for owner: {owner.name}...")
    ownership_records = db.query(models.MediaOwnership).filter(
        models.MediaOwnership.owner_id == owner.id
    ).options(
        selectinload(models.MediaOwnership.location).selectinload(models.Location.media_file).selectinload(
            models.MediaFile.metadata_sources)
    ).all()
    return [record.location for record in ownership_records]


def merge_metadata_for_export(metadata_sources: List[models.Metadata]) -> Tuple[Dict, Dict]:
    """
    Merges multiple metadata sources into a single dictionary for export.
    Priority: 'exif' > 'google_json'.
    """
    if not metadata_sources:
        return {}, {}

    merged = {}
    conflicts = {}

    # Sort sources by priority
    sources = sorted(metadata_sources, key=lambda m: 1 if m.source == 'exif' else 2)

    # Define which raw keys map to our simple fields
    key_map = {
        'date_taken': 'date_taken',
        'gps_latitude': 'gps_latitude',
        'gps_longitude': 'gps_longitude',
        # Add more mappings from raw_data if needed (e.g., description, title)
        'description': 'description',
        'title': 'title',
    }

    for key, field_name in key_map.items():
        for source in sources:
            value = source.raw_data.get(field_name) if field_name in source.raw_data else getattr(source, field_name,
                                                                                                  None)

            if value is None or value == '' or value == 0:
                continue

            if key not in merged:
                merged[key] = value
            elif merged[key] != value:
                if key not in conflicts:
                    conflicts[key] = []
                conflicts[key].append(value)

    if conflicts:
        # For simplicity, we just report conflicts. A more complex strategy could resolve them.
        pass

    # You can add logic here to pull more fields directly from the preferred raw_data (e.g., camera model from exif)
    exif_source = next((s for s in sources if s.source == 'exif'), None)
    if exif_source and exif_source.raw_data:
        merged.update({
            "camera_make": exif_source.raw_data.get("Make"),
            "camera_model": exif_source.raw_data.get("Model"),
            # ... add other exif-specific fields you want to preserve
        })

    return merged, conflicts

def write_metadata_with_exiftool(filepath: str, metadata: Dict):
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
    # if date_taken:
    #     formatted_date = date_taken.strftime('%Y:%m:%d %H:%M:%S')
    #     args.append(f"-FileCreateDate={formatted_date}")
    #     args.append(f"-FileModifyDate={formatted_date}")

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
        print(f"❌ ERROR: '{CONFIG['EXIFTOOL_PATH']}' not found.")
        return

    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)
    os.makedirs(export_dir, exist_ok=True)

    all_failures = []
    exported_count, skipped_count, conflict_count = 0, 0, 0

    try:
        with SessionLocal() as db:
            owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
            if not owner:
                raise ValueError(f"Owner '{owner_name}' not found.")

            locations_to_export = get_locations_for_owner(db, owner)
            total_files = len(locations_to_export)
            if not total_files:
                print(f"No files found for owner '{owner_name}'.")
                return

            print(f"Found {total_files} files to potentially export.")

            with tqdm(total=total_files, desc="Exporting Media", unit="file") as pbar:
                for location in locations_to_export:
                    pbar.update(1)
                    output_path = os.path.join(export_dir, location.filename)

                    if os.path.exists(output_path):
                        skipped_count += 1
                        pbar.set_postfix(exported=exported_count, skipped=skipped_count, conflicts=conflict_count)
                        continue

                    try:
                        merged_meta, conflicts = merge_metadata_for_export(location.media_file.metadata_sources)

                        if conflicts:
                            conflict_count += 1
                            all_failures.append({"location": location.path, "conflicts": conflicts})
                            pbar.set_postfix(exported=exported_count, skipped=skipped_count, conflicts=conflict_count)
                            continue

                        shutil.copy2(location.path, output_path)

                        if merged_meta:
                            write_metadata_with_exiftool(output_path, merged_meta)

                        exported_count += 1
                        pbar.set_postfix(exported=exported_count, skipped=skipped_count, conflicts=conflict_count)

                    except Exception as e:
                        print(f"\nAn unexpected error occurred while processing {location.path}: {e}")

    finally:
        print("\nExport complete!")
        print(f"✅ Successfully exported {exported_count} files.")
        print(f"⏩ Skipped {skipped_count} files that already existed.")
        if all_failures:
            print(f"⚠️ Encountered {len(all_failures)} merge conflicts. These files were skipped:")
            for failure in all_failures:
                print(f"\n  - File: {failure['location']}")
                for key, values in failure['conflicts'].items():
                    print(f"    - Conflict on '{key}': Values found were {values}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export media files with merged metadata.")
    parser.add_argument("owner_name", type=str, help="The name of the owner whose files to export.")
    parser.add_argument("export_dir", type=str, help="The target directory for the export.")
    args = parser.parse_args()
    export_main(args.owner_name, args.export_dir)