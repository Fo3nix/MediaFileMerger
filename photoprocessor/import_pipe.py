import os
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict

from tqdm import tqdm
from sqlalchemy.orm import Session
from photoprocessor.processor import PhotoProcessor
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models

# --- Configuration ---
CONFIG = {
    "BATCH_SIZE": 100,
    "MEDIA_EXTENSIONS": (
        '.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp',
        '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv'
    )
}


# --- End Configuration ---

# This worker function is executed by each process in the pool.
def process_media_chunk(chunk: List[str]) -> tuple[dict, list]:
    """Worker function to process a chunk of file paths."""
    processor = PhotoProcessor()
    return processor.process_batch(chunk)


def get_or_create_owner(db: Session, name: str) -> models.Owner:
    owner = db.query(models.Owner).filter(models.Owner.name == name).first()
    if not owner:
        owner = models.Owner(name=name)
        db.add(owner)
        db.commit();
        db.refresh(owner)
    return owner


def scan_media_files(directory: str) -> List[str]:
    print(f"Scanning for media files in {directory}...")
    paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.lower().endswith(CONFIG["MEDIA_EXTENSIONS"]):
                paths.append(os.path.join(root, file))
    return paths


def save_batch_to_db(db: Session, owner: models.Owner, batch_data: Dict) -> (Dict, List):
    """Saves processed data to the database and returns stats and failures."""
    stats = {"inserted": 0, "updated": 0, "conflicts": 0}
    failures = []
    if not batch_data:
        return stats, failures

    abs_paths = [os.path.abspath(p) for p in batch_data.keys()]
    existing_locations = {loc.path: loc for loc in
                          db.query(models.Location).filter(models.Location.path.in_(abs_paths))}
    hashes_to_check = {item["media_file"]["file_hash"] for item in batch_data.values()}
    existing_media_files = {mf.file_hash: mf for mf in
                            db.query(models.MediaFile).filter(models.MediaFile.file_hash.in_(hashes_to_check))}

    for path, data in batch_data.items():
        abs_path = os.path.abspath(path)
        try:
            with db.begin_nested():
                current_hash = data["media_file"]["file_hash"]
                media_file_obj = existing_media_files.get(current_hash)
                if not media_file_obj:
                    media_file_obj = models.MediaFile(**data["media_file"])
                    db.add(media_file_obj)
                    existing_media_files[current_hash] = media_file_obj

                location_obj = existing_locations.get(abs_path)
                if not location_obj:
                    location_obj = models.Location(path=abs_path, filename=os.path.basename(abs_path),
                                                   media_file=media_file_obj)
                    db.add(location_obj)
                    stats["inserted"] += 1
                else:
                    stats["updated"] += 1
                    if location_obj.media_file.file_hash != current_hash:
                        raise ValueError(
                            f"Hash conflict: path points to a different file. Old: {location_obj.media_file.file_hash}, New: {current_hash}")

                if owner not in [own.owner for own in location_obj.owners]:
                    db.add(models.MediaOwnership(owner=owner, location=location_obj))

                # Your location-specific metadata upsert logic from before
                def upsert_metadata(source_name: str, source_data: Dict):
                    if not source_data: return
                    metadata_entry = db.query(models.Metadata).filter_by(location_id=location_obj.id,
                                                                         source=source_name).first()
                    parsed = source_data["parsed"]
                    if metadata_entry:
                        metadata_entry.date_taken, metadata_entry.date_taken_key, metadata_entry.date_modified, metadata_entry.date_modified_key, metadata_entry.gps_latitude, metadata_entry.gps_longitude, metadata_entry.raw_data = parsed.get(
                            "date_taken"), parsed.get("date_taken_key"), parsed.get("date_modified"), parsed.get("date_modified_key"), parsed.get("gps_latitude"), parsed.get("gps_longitude"), source_data["raw"]
                    else:
                        db.add(models.Metadata(media_file=media_file_obj, location=location_obj, source=source_name,
                                               date_taken=parsed.get("date_taken"),
                                               date_taken_key=parsed.get("date_taken_key"),
                                               date_modified=parsed.get("date_modified"),
                                               date_modified_key=parsed.get("date_modified_key"),
                                               gps_latitude=parsed.get("gps_latitude"),
                                               gps_longitude=parsed.get("gps_longitude"), raw_data=source_data["raw"]))

                upsert_metadata('exif', data.get("exif_metadata"))
                upsert_metadata('google_json', data.get("google_metadata"))

        except Exception as e:
            db.rollback()
            stats["conflicts"] += 1
            failures.append({"path": path, "error": f"Database error: {e}"})

    return stats, failures


def main(owner_name: str, takeout_dir: str = None, filelist_path: str = None):
    print("Initializing...")
    models.Base.metadata.create_all(bind=engine)

    # --- Set up failure logger ---
    failure_log_path = 'import_failures.log'
    logging.basicConfig(level=logging.ERROR, filename=failure_log_path, filemode='w',
                        format='%(asctime)s - %(message)s')

    all_paths = []
    if filelist_path:
        print(f"Reading file list from: {filelist_path}")
        try:
            with open(filelist_path, 'r', encoding='utf-8') as f:
                all_paths = [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            print(f"❌ ERROR: Input file not found at '{filelist_path}'")
            return
    elif takeout_dir:
        all_paths = scan_media_files(takeout_dir)

    total_files = len(all_paths)
    if not total_files:
        print("No media files found.");
        return

    print(f"Found {total_files} files to process.")
    chunks = [all_paths[i:i + CONFIG["BATCH_SIZE"]] for i in range(0, total_files, CONFIG["BATCH_SIZE"])]

    total_stats = {"inserted": 0, "updated": 0, "conflicts": 0, "failures": 0}

    with tqdm(total=total_files, desc="Importing Media", unit="file") as pbar:
        with ProcessPoolExecutor() as executor, SessionLocal() as db:
            owner = get_or_create_owner(db, owner_name)

            # Submit all chunks to the executor
            futures = [executor.submit(process_media_chunk, chunk) for chunk in chunks]

            for future in as_completed(futures):
                try:
                    success_data, process_failures = future.result()

                    # Log failures from the worker process
                    for failure in process_failures:
                        logging.error(f"File: {failure['path']}\n  Error: {failure['error']}\n")
                        total_stats["failures"] += 1

                    # Save successes to the database
                    if success_data:
                        db_stats, db_failures = save_batch_to_db(db, owner, success_data)
                        db.commit()

                        # Log failures from the database operation
                        for failure in db_failures:
                            logging.error(f"File: {failure['path']}\n  Error: {failure['error']}\n")

                        # Aggregate stats
                        for key in db_stats: total_stats[key] += db_stats[key]
                        total_stats["failures"] += len(db_failures)

                    # Update progress bar by the number of files in the processed chunk
                    pbar.update(len(success_data) + len(process_failures))
                    pbar.set_postfix(inserted=total_stats['inserted'], updated=total_stats['updated'],
                                     failed=total_stats['failures'])

                except Exception as e:
                    # Catch unexpected errors from the worker process itself
                    logging.error(f"A worker process failed catastrophically: {e}")

    print("\n--- Import Complete ---")
    print(f"✅ Inserted {total_stats['inserted']} new file locations.")
    print(f"🔄 Scanned/updated {total_stats['updated']} existing file locations.")
    if total_stats['failures'] > 0:
        print(f"❌ Encountered {total_stats['failures']} failures. See details in {failure_log_path}")
    print("-----------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Import media files into the PhotoProcessor database.")
    parser.add_argument("owner_name", type=str, help="The name of the owner of these media files.")

    # Create a group for mutually exclusive arguments
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--directory", "-d", type=str, help="The input directory to scan for media.")
    source_group.add_argument("--filelist", "-f", type=str,
                              help="Path to a text file with one file path per line to import.")

    args = parser.parse_args()

    # Call main with the new arguments
    main(args.owner_name, takeout_dir=args.directory, filelist_path=args.filelist)