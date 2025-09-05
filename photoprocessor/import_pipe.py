import os
import argparse
from typing import List, Dict, Tuple, Any

from tqdm import tqdm
from sqlalchemy.orm import Session, joinedload
from photoprocessor.processor import PhotoProcessor
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models

# --- Configuration ---
CONFIG = {
    "BATCH_SIZE": 250,
    # MODIFIED: Changed 'update' to 'merge'
    "DUPLICATE_HANDLING": 'merge',  # 'skip' or 'merge'
    "MEDIA_EXTENSIONS": (
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp',
        '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv'
    )
}


# --- End Configuration ---


# ADD THIS ENTIRE CLASS
class MergeConflictException(Exception):
    """Custom exception to signal a rollback for a single file merge."""
    def __init__(self, message, location, conflicts):
        super().__init__(message)
        self.location = location
        self.conflicts = conflicts

def get_or_create_owner(db: Session, name: str) -> models.Owner:
    # This function remains unchanged
    owner = db.query(models.Owner).filter(models.Owner.name == name).first()
    if not owner:
        print(f"Owner '{name}' not found, creating new entry.")
        owner = models.Owner(name=name)
        db.add(owner)
        db.commit()
        db.refresh(owner)
    return owner


def scan_media_files(directory: str) -> List[str]:
    # This function remains unchanged
    print(f"Scanning for media files in {directory}...")
    paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(CONFIG["MEDIA_EXTENSIONS"]):
                paths.append(os.path.join(root, file))
    return paths


# +++ NEW MERGE LOGIC +++
def merge_metadata(existing_media_file: models.MediaFile, new_data: Dict) -> Tuple[bool, Dict]:
    """
    Attempts to merge metadata from new_data into an existing MediaFile object.

    Returns:
        A tuple of (success: bool, conflicts: dict).
        Conflicts dict contains details of any failed attribute merges.
    """
    conflicts = {}

    def _is_empty(value):
        """Helper to check if a value is considered empty, including GPS zeros."""
        return value is None or value == '' or value == 0

    def _merge_attribute(obj, attr_name, existing_val, new_val):
        """Merges a single attribute, returning True on success or False on conflict."""
        # Skip if new value is empty; keep the existing one.
        if _is_empty(new_val):
            return True
        # If existing value is empty, update it with the new value.
        if _is_empty(existing_val):
            setattr(obj, attr_name, new_val)
            return True
        # If they are the same, it's a success.
        if existing_val == new_val:
            return True
        # If they are different and neither is empty, it's a conflict.
        conflicts[f"{obj.__class__.__name__}.{attr_name}"] = (existing_val, new_val)
        return False

    # 1. Merge MediaFile attributes
    for key, value in new_data.get("media_file", {}).items():
        _merge_attribute(existing_media_file, key, getattr(existing_media_file, key), value)

    # 2. Merge Metadata attributes
    if new_data.get("metadata"):
        if not existing_media_file.processed_metadata:
            existing_media_file.processed_metadata = models.Metadata()

        for key, value in new_data["metadata"].items():
            # Special handling for GPS coordinates where 0 is invalid/empty
            if key in ["gps_latitude", "gps_longitude"] and value == 0:
                value = None

            _merge_attribute(
                existing_media_file.processed_metadata,
                key,
                getattr(existing_media_file.processed_metadata, key),
                value
            )

    # 3. Merge GooglePhotosMetadata attributes
    if new_data.get("google_metadata"):
        if not existing_media_file.google_metadata:
            existing_media_file.google_metadata = models.GooglePhotosMetadata()

        for key, value in new_data["google_metadata"].items():
            # Special handling for GPS coordinates where 0 is invalid/empty
            if key in ["gps_latitude", "gps_longitude"] and value == 0:
                value = None

            _merge_attribute(
                existing_media_file.google_metadata,
                key,
                getattr(existing_media_file.google_metadata, key),
                value
            )

    return not conflicts, conflicts


def process_batch(db: Session, processor: PhotoProcessor, paths: List[str], owner: models.Owner) -> Tuple[
    int, int, List]:
    """
    Processes a batch of files, handling new files and merging existing ones.
    Each file's operation is atomic; merge conflicts will cause a rollback for that file only.
    """
    inserted, merged = 0, 0
    failures = []

    # --- Step 1: Process all files in the batch with ONE call ---
    batch_data = processor.process_batch([os.path.abspath(p) for p in paths])

    if not batch_data:
        return 0, 0, []

    hashes_to_check = {
        item["media_file"]["file_hash"]
        for item in batch_data.values() if item and "media_file" in item
    }

    # --- Step 2: Find all existing MediaFile objects matching these hashes ---
    existing_media_files = {
        mf.file_hash: mf for mf in db.query(models.MediaFile).options(
            joinedload(models.MediaFile.processed_metadata),
            joinedload(models.MediaFile.google_metadata)
        ).filter(models.MediaFile.file_hash.in_(hashes_to_check))
    }

    # --- Step 3: Iterate and decide to INSERT or MERGE atomically per file ---
    for location, data in batch_data.items():
        try:
            # Use a nested transaction (SAVEPOINT) for each file.
            # If an exception is raised inside, only this file's changes are rolled back.
            with db.begin_nested():
                file_hash = data["media_file"]["file_hash"]

                if file_hash in existing_media_files:
                    # === MERGE PATH ===
                    if CONFIG["DUPLICATE_HANDLING"] == 'merge':
                        existing_file = existing_media_files[file_hash]
                        success, conflicts = merge_metadata(existing_file, data)

                        if not success:
                            # This exception will trigger the rollback of the nested transaction.
                            raise MergeConflictException(
                                "Metadata conflict during merge", location, conflicts
                            )

                        # This code only runs if the merge was successful.
                        if data.get("raw_exif"):
                            existing_file.raw_exif.append(models.RawExif(**data["raw_exif"]))
                        if data.get("raw_google_json"):
                            existing_file.raw_google_json.append(models.RawGoogleJson(**data["raw_google_json"]))

                        merged += 1

                else:
                    # === INSERT PATH ===
                    media_file = models.MediaFile()
                    _apply_processed_data(media_file, data)
                    new_ownership = models.MediaOwnership(
                        owner=owner, media_file=media_file,
                        location=location, filename=os.path.basename(location)
                    )
                    db.add(new_ownership)
                    existing_media_files[file_hash] = media_file
                    inserted += 1

        except MergeConflictException as e:
            # Catch the specific conflict, log it, and continue to the next file.
            failures.append({"location": e.location, "conflicts": e.conflicts})

    return inserted, merged, failures


def _apply_processed_data(media_file_obj: models.MediaFile, data: dict):
    # This function is now only used for creating NEW files, not updating.
    for key, value in data.get("media_file", {}).items():
        setattr(media_file_obj, key, value)
    if data.get("metadata"):
        media_file_obj.processed_metadata = models.Metadata(**data["metadata"])
    if data.get("google_metadata"):
        media_file_obj.google_metadata = models.GooglePhotosMetadata(**data["google_metadata"])
    # TO THIS:
    if data.get("raw_exif"):
        media_file_obj.raw_exif = [models.RawExif(**data["raw_exif"])]
    if data.get("raw_google_json"):
        media_file_obj.raw_google_json = [models.RawGoogleJson(**data["raw_google_json"])]


def main(owner_name: str, takeout_dir: str):
    """Main function to orchestrate the media import process."""
    if CONFIG["DUPLICATE_HANDLING"] not in ['skip', 'merge']:
        print(f"ERROR: Invalid DUPLICATE_HANDLING: '{CONFIG['DUPLICATE_HANDLING']}'. Must be 'skip' or 'merge'.")
        return

    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)
    processor = PhotoProcessor()

    all_paths = scan_media_files(takeout_dir)
    total_files = len(all_paths)
    if total_files == 0:
        print("No media files found. Exiting.")
        return

    print(f"Found {total_files} files. Mode: '{CONFIG['DUPLICATE_HANDLING']}'.")

    total_inserted = 0
    total_merged = 0
    all_failures = []

    try:
        with SessionLocal() as db:
            owner = get_or_create_owner(db, owner_name)

            with tqdm(total=total_files, desc="Importing Media", unit="file") as pbar:
                for batch_paths in (all_paths[i:i + CONFIG["BATCH_SIZE"]] for i in
                                    range(0, total_files, CONFIG["BATCH_SIZE"])):
                    inserted, merged, failures = process_batch(db, processor, batch_paths, owner)
                    db.commit()

                    total_inserted += inserted
                    total_merged += merged
                    all_failures.extend(failures)

                    pbar.update(len(batch_paths))
                    pbar.set_postfix(inserted=total_inserted, merged=total_merged, failed=len(all_failures))

    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        print("\nImport complete!")
        print(f"✅ Successfully inserted {total_inserted} new files.")
        print(f"🔄 Successfully merged metadata for {total_merged} existing files.")
        if all_failures:
            print(f"⚠️ Encountered {len(all_failures)} merge conflicts. Details below:")
            for failure in all_failures:
                print(f"\n  - File: {failure['location']}")
                for key, values in failure['conflicts'].items():
                    print(f"    - Conflict on '{key}': Existing='{values[0]}', New='{values[1]}'")


if __name__ == "__main__":
    # Use argparse for proper command-line argument handling
    parser = argparse.ArgumentParser(description="Import media files into the PhotoProcessor database.")
    parser.add_argument("owner_name", type=str, help="The name of the owner of these media files.")
    parser.add_argument("takeout_dir", type=str, help="The input directory for the import.")

    args = parser.parse_args()
    main(args.owner_name, args.takeout_dir)