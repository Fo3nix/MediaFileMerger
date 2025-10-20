import os
import argparse
from typing import List, Dict

from tqdm import tqdm
from sqlalchemy.orm import Session, selectinload
from photoprocessor import models
from photoprocessor.database import SessionLocal


def get_media_files_for_owner(db: Session, owner_name: str) -> List[models.MediaFile]:
    """
    Queries all unique MediaFile objects associated with an owner,
    eagerly loading their locations.
    """
    owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
    if not owner:
        return []

    # This query joins through the tables to find all unique media files for the owner
    # and eagerly loads the 'locations' for each media file to prevent N+1 queries.
    return db.query(models.MediaFile).join(
        models.Location
    ).join(
        models.MediaOwnership
    ).filter(
        models.MediaOwnership.owner_id == owner.id
    ).distinct().options(
        selectinload(models.MediaFile.locations)
    ).all()


def main(owner_name: str):
    """
    For a given owner, finds all their media files and checks if any of
    the file's locations are missing from the filesystem.
    """
    print(f"Checking media file locations for owner: '{owner_name}'")
    print("-" * 30)

    with SessionLocal() as db:
        media_files = get_media_files_for_owner(db, owner_name)

    if not media_files:
        print(f"❌ Error: No media files found for owner '{owner_name}'. Please check the name.")
        return

    # A dictionary to store missing locations, keyed by the media file's hash
    missing_locations_report: Dict[str, List[str]] = {}

    print("Verifying locations for each media file...")
    with tqdm(total=len(media_files), desc="Scanning media files", unit="file") as pbar:
        for media_file in media_files:
            missing_paths_for_this_file = []
            for loc in media_file.locations:
                if not os.path.exists(loc.path):
                    missing_paths_for_this_file.append(loc.path)

            if missing_paths_for_this_file:
                missing_locations_report[media_file.file_hash] = sorted(missing_paths_for_this_file)

            pbar.update(1)

    # --- Print Summary Report ---
    print("\n--- Verification Report ---")
    print(f"Total unique media files checked: {len(media_files)}")

    if not missing_locations_report:
        print("✅ All locations for all media files are present on the filesystem.")
    else:
        num_files_with_missing_locs = len(missing_locations_report)
        total_missing_locs = sum(len(paths) for paths in missing_locations_report.values())
        print(
            f"Found {num_files_with_missing_locs} media file(s) with a total of {total_missing_locs} missing location(s).")
        print("-" * 25)

        print("\nDetails of missing locations:")
        for file_hash, paths in missing_locations_report.items():
            print(f"\n  Media File Hash: {file_hash}")
            for path in paths:
                print(f"    - Missing: {path}")

    print("\n--- Process Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="For a given owner, find all media files and check if any of their locations are missing from the filesystem.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("owner", type=str, help="The name of the owner whose files to verify.")
    args = parser.parse_args()

    main(args.owner)