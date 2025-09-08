import os
import argparse
from typing import List, Dict, Tuple, Any

from tqdm import tqdm
from sqlalchemy.orm import Session, joinedload, selectinload
from photoprocessor.processor import PhotoProcessor
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models

# --- Configuration ---
CONFIG = {
    "BATCH_SIZE": 250,
    "MEDIA_EXTENSIONS": (
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp',
        '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv'
    )
}


# --- End Configuration ---

class LocationHashConflictError(Exception):
    """Custom exception for when a path points to a file with a different hash than recorded."""
    def __init__(self, message, location_path, existing_hash, new_hash):
        super().__init__(message)
        self.location_path = location_path
        self.existing_hash = existing_hash
        self.new_hash = new_hash

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


def process_batch(db: Session, processor: PhotoProcessor, paths: List[str], owner: models.Owner) -> Dict:
    """Processes a batch of files with explicit get-or-create/update logic for metadata."""
    stats = {"inserted": 0, "updated": 0, "conflicts": 0}
    failures = []

    batch_data = processor.process_batch([os.path.abspath(p) for p in paths])

    if not batch_data:
        return stats, failures

    abs_paths = list(batch_data.keys())
    existing_locations = {loc.path: loc for loc in
                          db.query(models.Location).options(selectinload(models.Location.media_file)).filter(
                              models.Location.path.in_(abs_paths))}
    hashes_to_check = {item["media_file"]["file_hash"] for item in batch_data.values()}
    existing_media_files = {mf.file_hash: mf for mf in
                            db.query(models.MediaFile).filter(models.MediaFile.file_hash.in_(hashes_to_check))}

    for abs_path, data in batch_data.items():
        try:
            with db.begin_nested():
                current_hash = data["media_file"]["file_hash"]
                location_obj = existing_locations.get(abs_path)

                if location_obj and location_obj.media_file.file_hash != current_hash:
                    raise LocationHashConflictError(
                        "File content has changed for a known location",
                        abs_path, location_obj.media_file.file_hash, current_hash
                    )

                media_file_obj = existing_media_files.get(current_hash)
                if not media_file_obj:
                    media_file_obj = models.MediaFile(**data["media_file"])
                    db.add(media_file_obj)
                    existing_media_files[current_hash] = media_file_obj

                if not location_obj:
                    location_obj = models.Location(path=abs_path, filename=os.path.basename(abs_path),
                                                   media_file=media_file_obj)
                    db.add(location_obj)
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1

                if owner not in [own.owner for own in location_obj.owners]:
                    ownership = models.MediaOwnership(owner=owner, location=location_obj)
                    db.add(ownership)

                def upsert_metadata(source_name: str, source_data: Dict):
                    """
                    Creates or updates metadata for a specific location.
                    This allows duplicate files to have different sidecar metadata.
                    """
                    if not source_data:
                        return

                    # Query for existing metadata for THIS specific location and source.
                    metadata_entry = db.query(models.Metadata).filter_by(
                        location_id=location_obj.id,
                        source=source_name
                    ).first()

                    parsed_data = source_data["parsed"]

                    if metadata_entry:
                        # UPDATE the existing entry for this location.
                        metadata_entry.date_taken = parsed_data.get("date_taken")
                        metadata_entry.gps_latitude = parsed_data.get("gps_latitude")
                        metadata_entry.gps_longitude = parsed_data.get("gps_longitude")
                        metadata_entry.raw_data = source_data["raw"]
                    else:
                        # CREATE a new entry for this location.
                        metadata_entry = models.Metadata(
                            media_file=media_file_obj,
                            location=location_obj,
                            source=source_name,
                            date_taken=parsed_data.get("date_taken"),
                            gps_latitude=parsed_data.get("gps_latitude"),
                            gps_longitude=parsed_data.get("gps_longitude"),
                            raw_data=source_data["raw"]
                        )
                        db.add(metadata_entry)

                upsert_metadata('exif', data.get("exif_metadata"))
                upsert_metadata('google_json', data.get("google_metadata"))

        except LocationHashConflictError as e:
            db.rollback()
            stats["conflicts"] += 1
            failures.append({"path": e.location_path, "error": str(e),
                             "details": f"Existing Hash: {e.existing_hash}, New Hash: {e.new_hash}"})
        except Exception as e:
            db.rollback()
            print(f"Unexpected error for {abs_path}: {e}")

    return stats, failures

def main(owner_name: str, takeout_dir: str):
    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)
    processor = PhotoProcessor()

    all_paths = scan_media_files(takeout_dir)
    total_files = len(all_paths)
    if total_files == 0:
        print("No media files found. Exiting.")
        return

    print(f"Found {total_files} files to process.")

    total_stats = {"inserted": 0, "updated": 0, "conflicts": 0}
    all_failures = []

    try:
        with SessionLocal() as db:
            owner = get_or_create_owner(db, owner_name)
            with tqdm(total=total_files, desc="Importing Media", unit="file") as pbar:
                for batch_paths in (all_paths[i:i + CONFIG["BATCH_SIZE"]] for i in
                                    range(0, total_files, CONFIG["BATCH_SIZE"])):
                    stats, failures = process_batch(db, processor, batch_paths, owner)
                    db.commit()

                    for key in total_stats:
                        total_stats[key] += stats[key]
                    all_failures.extend(failures)

                    pbar.update(len(batch_paths))
                    pbar.set_postfix(inserted=total_stats['inserted'], updated=total_stats['updated'],
                                     failed=total_stats['conflicts'])
    finally:
        print("\nImport complete!")
        print(f"✅ Inserted {total_stats['inserted']} new file locations.")
        print(f"🔄 Scanned and updated metadata for {total_stats['updated']} existing file locations.")
        if total_stats['conflicts'] > 0:
            print(f"❌ Encountered {total_stats['conflicts']} hash conflicts. These files were skipped:")
            for failure in all_failures:
                print(f"  - File: {failure['path']}\n    Reason: {failure['details']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import media files into the PhotoProcessor database.")
    parser.add_argument("owner_name", type=str, help="The name of the owner of these media files.")
    parser.add_argument("takeout_dir", type=str, help="The input directory for the import.")
    args = parser.parse_args()
    main(args.owner_name, args.takeout_dir)