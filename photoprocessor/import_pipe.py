import os
from tqdm import tqdm
from photoprocessor.processor import PhotoProcessor  # Assuming processor is in the same package
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
    existing_hashes = {result[0] for result in db.query(models.MediaFile.file_hash)}
    print(f"Found {len(existing_hashes)} files already in the database.")

    print(f"Scanning for image files in {TAKEOUT_DIR}...")
    image_paths = []
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp')
    for root, _, files in os.walk(TAKEOUT_DIR):
        for file in files:
            if file.lower().endswith(image_extensions):
                image_paths.append(os.path.join(root, file))

    print(f"Found {len(image_paths)} total images. Processing new files...")

    new_files_processed = 0
    commit_batch_size = 100  # Commit every 100 files to avoid large transactions
    try:
        for path in tqdm(image_paths, desc="Importing Photos"):
            # --- Process file and populate models ---
            processed_data = processor.process(path)
            if not processed_data:
                continue

            # Skip if already in the database
            if processed_data["media_file"]["file_hash"] in existing_hashes:
                continue

            # Create the main MediaFile object
            media_file = models.MediaFile(**processed_data["media_file"])

            # Link related data if it exists
            if processed_data.get("metadata"):
                media_file.processed_metadata = models.Metadata(**processed_data["metadata"])

            if processed_data.get("google_metadata"):
                media_file.google_metadata = models.GooglePhotosMetadata(**processed_data["google_metadata"])

            if processed_data.get("raw_exif"):
                media_file.raw_exif = models.RawExif(**processed_data["raw_exif"])

            if processed_data.get("raw_google_json"):
                media_file.raw_google_json = models.RawGoogleJson(**processed_data["raw_google_json"])

            db.add(media_file)

            if (new_files_processed % commit_batch_size) == 0:
                #print(f"\nCommitting {commit_batch_size} new files to the database...")
                db.commit()

            # Add hash to our set to prevent duplicates in the same run
            existing_hashes.add(media_file.file_hash)
            new_files_processed += 1

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