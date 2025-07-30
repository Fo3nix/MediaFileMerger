# import.py
import os
from tqdm import tqdm
from photoprocessor.database import PhotoDatabase
from photoprocessor.processor import PhotoProcessor

# --- Configuration ---
TAKEOUT_DIR = r"E:\Inge backup Photos\Takeout\Google Foto_s\NewZealandCamera"
DB_PATH = "test.db"


# --- End Configuration ---

def main():
    """Main function to orchestrate the photo import process."""
    if not os.path.isdir(TAKEOUT_DIR) or "path/to" in TAKEOUT_DIR:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        print("!!! ERROR: Please update the 'TAKEOUT_DIR' variable    !!!")
        print("!!! in the 'import.py' script before running.          !!!")
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        return

    db = PhotoDatabase(db_path=DB_PATH)
    processor = PhotoProcessor()

    print(f"Scanning for image files in {TAKEOUT_DIR}...")
    image_paths = []
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp')
    for root, _, files in os.walk(TAKEOUT_DIR):
        for file in files:
            if file.lower().endswith(image_extensions):
                image_paths.append(os.path.join(root, file))

    print(f"Found {len(image_paths)} images. Processing and adding to database...")

    for path in tqdm(image_paths, desc="Importing Photos"):
        if db.is_file_processed(path):
            continue

        photo_data = processor.process(path)
        if photo_data:
            db.insert_photo(photo_data)

    db.close()
    print("Import complete! Your database is ready.")


if __name__ == "__main__":
    main()