import os
from tqdm import tqdm
from photoprocessor.processor import PhotoProcessor
from photoprocessor.database import engine, SessionLocal
from photoprocessor import models

# --- Configuration ---
TAKEOUT_DIR = r"E:\Inge backup Photos\Takeout\Google Foto_s"
# --- End Configuration ---

def main():
    """Main function to orchestrate the photo import process."""
    if not os.path.isdir(TAKEOUT_DIR) or "path/to" in TAKEOUT_DIR:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! ERROR: Please update the 'TAKEOUT_DIR' variable    !!!")
        print("!!! in the 'import.py' script before running.          !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    print("Initializing database...")
    models.Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    processor = PhotoProcessor()

    # --- Pre-scan for existing files to avoid re-processing ---
    print("Checking for already processed files...")
    # <-- CHANGE: Query for the unique path components, not the hash.
    existing_files_query = db.query(
        models.MediaFile.base_path,
        models.MediaFile.relative_path,
        models.MediaFile.filename
    )
    existing_files = {result for result in existing_files_query}
    print(f"Found {len(existing_files)} files already in the database.")

    print(f"Scanning for media files in {TAKEOUT_DIR}...")
    image_paths = []
    # Note: Corrected tuple concatenation
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp',
                        '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv')
    for root, _, files in os.walk(TAKEOUT_DIR):
        for file in files:
            if file.lower().endswith(image_extensions):
                image_paths.append(os.path.join(root, file))

    print(f"Found {len(image_paths)} total files. Processing new files...")

    new_files_processed = 0
    commit_batch_size = 100
    try:
        for path in tqdm(image_paths, desc="Importing Media"):
            processed_data = processor.process(path, TAKEOUT_DIR)
            if not processed_data:
                continue

            # <-- CHANGE: Check for existence using the unique path tuple.
            media_file_data = processed_data["media_file"]
            file_path_tuple = (
                media_file_data["base_path"],
                media_file_data["relative_path"],
                media_file_data["filename"]
            )

            if file_path_tuple in existing_files:
                # This log is optional but helpful for debugging
                tqdm.write(f"Skipping already processed file: {media_file_data['base_path']}/{media_file_data['relative_path']}/{media_file_data['filename']}")
                continue

            # Create the main MediaFile object
            media_file = models.MediaFile(**media_file_data)

            # Link related data. SQLAlchemy handles assigning the foreign key automatically.
            if processed_data.get("metadata"):
                media_file.processed_metadata = models.Metadata(**processed_data["metadata"])
            if processed_data.get("google_metadata"):
                media_file.google_metadata = models.GooglePhotosMetadata(**processed_data["google_metadata"])
            if processed_data.get("raw_exif"):
                media_file.raw_exif = models.RawExif(**processed_data["raw_exif"])
            if processed_data.get("raw_google_json"):
                media_file.raw_google_json = models.RawGoogleJson(**processed_data["raw_google_json"])

            db.add(media_file)
            new_files_processed += 1

            if (new_files_processed % commit_batch_size) == 0:
                db.commit()

            # <-- CHANGE: Add the new path tuple to our set to prevent duplicates in this run.
            existing_files.add(file_path_tuple)

    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print("Rolling back any uncommitted changes.")
        db.rollback()
    finally:
        print("\nFinalizing database changes...")
        db.commit()
        db.close()
        print("\nImport complete!")
        print(f"Successfully added {new_files_processed} new files to the database.")

if __name__ == "__main__":
    main()