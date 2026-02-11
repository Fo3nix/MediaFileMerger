import argparse
import logging
from pathlib import Path
from sqlalchemy.orm import Session
from photoprocessor.database import SessionLocal
from photoprocessor import models

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[
        logging.FileHandler("metadata_db_diff_report.log", mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Only check these keys to avoid noise from system-level attributes
RELEVANT_KEYS = {
    "EXIF:DateTimeOriginal", "XMP:DateTimeOriginal", "GPSLatitude", "GPSLongitude",
    "QuickTime:CreateDate", "google:photoTakenTime", "UserComment", "ImageDescription"
}


def get_db_metadata(db: Session, file_path: str) -> dict:
    """Fetches all metadata entries for a specific file path from the DB."""
    loc = db.query(models.Location).filter(models.Location.path == file_path).first()
    if not loc:
        return {}

    # Flatten all entries from all sources (exif, google_json, etc) into one dict
    metadata = {}
    for source in loc.metadata_sources:
        for entry in source.entries:
            metadata[entry.key] = entry.value
    return metadata


def compare_folders(old_base: str, new_base: str):
    old_root = Path(old_base).resolve()
    new_root = Path(new_base).resolve()

    with SessionLocal() as db:
        print(f"Scanning filesystem: {old_root}")
        # Map relative path -> absolute path
        old_files = {str(p.relative_to(old_root)): str(p) for p in old_root.rglob('*') if p.is_file()}
        new_files = {str(p.relative_to(new_root)): str(p) for p in new_root.rglob('*') if p.is_file()}

        common_rel_paths = set(old_files.keys()) & set(new_files.keys())
        print(f"Found {len(common_rel_paths)} matching file paths.")

        diff_count = 0

        for rel_path in common_rel_paths:
            old_abs = old_files[rel_path]
            new_abs = new_files[rel_path]

            # Get File Stats
            old_stat = Path(old_abs).stat()
            new_stat = Path(new_abs).stat()

            # TRIGGER CONDITION: Size and System Modified Date are identical
            # (Rclone would have updated the file if either changed)
            if old_stat.st_size == new_stat.st_size and old_stat.st_mtime == new_stat.st_mtime:

                meta_old = get_db_metadata(db, old_abs)
                meta_new = get_db_metadata(db, new_abs)

                if not meta_old or not meta_new:
                    continue

                diffs = []
                # Check relevant keys
                for key in RELEVANT_KEYS:
                    val_old = meta_old.get(key)
                    val_new = meta_new.get(key)

                    if val_old != val_new:
                        diffs.append(f"    {key}: '{val_old}' -> '{val_new}'")

                if diffs:
                    diff_count += 1
                    logging.info(f"SILENT CHANGE: {rel_path}")
                    logging.info(f"  (Size: {old_stat.st_size}, MTime: {old_stat.st_mtime})")
                    for d in diffs:
                        logging.info(d)
                    logging.info("-" * 40)

        print(f"\n--- Verification Complete ---")
        if diff_count > 0:
            print(f"❌ Found {diff_count} files with identical size/mtime but different DB metadata.")
            print("Check 'metadata_db_diff_report.log' for details.")
        else:
            print("✅ No silent metadata differences found in DB for identical-looking files.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compare DB metadata for files in two directories.")
    parser.add_argument("old_dir", help="Old export folder")
    parser.add_argument("new_dir", help="New export folder")
    args = parser.parse_args()

    compare_folders(args.old_dir, args.new_dir)