import os
from tqdm import tqdm
from sqlalchemy import tuple_
from photoprocessor.processor import PhotoProcessor
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models

# --- Configuration ---
TAKEOUT_DIR = r"E:\Inge backup Photos\Takeout\Google Foto_s"
BATCH_SIZE = 500
COMMIT_BATCH_SIZE = 100
# <-- NEW: Add a flag to control behavior for duplicates ('update' or 'skip').
DUPLICATE_HANDLING = 'skip'


# --- End Configuration ---

def chunker(seq, size):
    """Yield successive n-sized chunks from a sequence."""
    for pos in range(0, len(seq), size):
        yield seq[pos:pos + size]


def main():
    """Main function to orchestrate the photo import process."""
    if DUPLICATE_HANDLING not in ['skip', 'update']:
        print(f"ERROR: Invalid DUPLICATE_HANDLING setting: '{DUPLICATE_HANDLING}'. Must be 'skip' or 'update'.")
        return

    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    processor = PhotoProcessor()

    print(f"Scanning for media files in {TAKEOUT_DIR}...")
    image_paths = []
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp',
                        '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv')
    for root, _, files in os.walk(TAKEOUT_DIR):
        for file in files:
            if file.lower().endswith(image_extensions):
                image_paths.append(os.path.join(root, file))

    total_files = len(image_paths)
    print(f"Found {total_files} total files. Duplicate handling mode: '{DUPLICATE_HANDLING}'.")

    # <-- NEW: Separate counters for inserted and updated files.
    inserted_count = 0
    updated_count = 0
    pbar = None

    try:
        pbar = tqdm(total=total_files, desc="Importing Media", unit="file")

        for batch_paths in chunker(image_paths, BATCH_SIZE):

            # --- Step 1: Prepare batch data and find what's already in the DB ---
            batch_tuples_to_check = []
            tuple_to_path_map = {}
            for path in batch_paths:
                base_path = os.path.abspath(TAKEOUT_DIR)
                filename = os.path.basename(path)
                relative_path = os.path.dirname(os.path.relpath(path, base_path))

                path_tuple = (base_path, relative_path, filename)
                batch_tuples_to_check.append(path_tuple)
                tuple_to_path_map[path_tuple] = path

            # <-- CHANGE: Fetch the full MediaFile objects for existing files, not just their paths.
            existing_files_query = db.query(models.MediaFile).filter(
                tuple_(
                    models.MediaFile.base_path,
                    models.MediaFile.relative_path,
                    models.MediaFile.filename
                ).in_(batch_tuples_to_check)
            )
            # Create a lookup map from path_tuple -> MediaFile object for fast access.
            existing_objects_map = {
                (mf.base_path, mf.relative_path, mf.filename): mf
                for mf in existing_files_query
            }

            # --- Step 2: Loop through the batch and either INSERT new or UPDATE existing ---
            for i, path_tuple in enumerate(batch_tuples_to_check):
                # If we're in 'skip' mode and the file exists, do nothing with it.
                if DUPLICATE_HANDLING == 'skip' and path_tuple in existing_objects_map:
                    continue

                # Process the file from disk to get the latest data.
                path = tuple_to_path_map[path_tuple]
                processed_data = processor.process(path, TAKEOUT_DIR)
                if not processed_data:
                    continue

                # Check if this is an existing file to UPDATE.
                if path_tuple in existing_objects_map:
                    # --- UPDATE LOGIC ---
                    media_file = existing_objects_map[path_tuple]

                    # Update the main object's attributes.
                    for key, value in processed_data["media_file"].items():
                        setattr(media_file, key, value)

                    # Update or create related data.
                    for rel_name, model_class in [("processed_metadata", models.Metadata),
                                                  ("google_metadata", models.GooglePhotosMetadata),
                                                  ("raw_exif", models.RawExif),
                                                  ("raw_google_json", models.RawGoogleJson)]:
                        data = processed_data.get(rel_name.replace("processed_", ""))
                        if data:
                            existing_rel = getattr(media_file, rel_name)
                            if existing_rel:  # If relationship exists, update its fields.
                                for key, value in data.items():
                                    setattr(existing_rel, key, value)
                            else:  # If it doesn't exist, create it.
                                setattr(media_file, rel_name, model_class(**data))

                    updated_count += 1
                else:
                    # --- INSERT LOGIC (for new files) ---
                    media_file = models.MediaFile(**processed_data["media_file"])

                    if processed_data.get("metadata"):
                        media_file.processed_metadata = models.Metadata(**processed_data["metadata"])
                    if processed_data.get("google_metadata"):
                        media_file.google_metadata = models.GooglePhotosMetadata(**processed_data["google_metadata"])
                    if processed_data.get("raw_exif"):
                        media_file.raw_exif = models.RawExif(**processed_data["raw_exif"])
                    if processed_data.get("raw_google_json"):
                        media_file.raw_google_json = models.RawGoogleJson(**processed_data["raw_google_json"])

                    db.add(media_file)
                    inserted_count += 1

                # Commit in smaller batches. SQLAlchemy handles both INSERTs and UPDATEs.
                if (i + 1) % COMMIT_BATCH_SIZE == 0:
                    db.commit()

                pbar.update(1)

            db.commit()

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print("Rolling back any uncommitted changes.")
        db.rollback()
    finally:
        if pbar:
            pbar.close()

        db.close()
        # <-- NEW: Report both inserted and updated counts.
        print("\nImport complete!")
        print(f"✅ Successfully inserted {inserted_count} new files.")
        print(f"🔄 Successfully updated {updated_count} existing files.")


if __name__ == "__main__":
    main()