import os
import argparse
import shutil
from typing import List

from tqdm import tqdm
from sqlalchemy.orm import Session
from photoprocessor import models
from photoprocessor.database import SessionLocal


def get_locations_for_owner(db: Session, owner_name: str) -> List[models.Location]:
    """Queries all locations for a given owner from the database."""
    owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
    if not owner:
        return []

    # This query efficiently gets all locations associated with the owner
    return db.query(models.Location).join(models.MediaOwnership).filter(
        models.MediaOwnership.owner_id == owner.id
    ).all()


def find_files_in_dir(directory: str, filename: str) -> List[str]:
    """
    Recursively finds all instances of a filename within a directory.
    Returns a list of full paths to the found files.
    """
    matches = []
    # os.walk is the most efficient way to scan a directory tree
    for root, _, files in os.walk(directory):
        if filename in files:
            matches.append(os.path.join(root, filename))
    return matches


def main(owner_name: str, output_dir: str, should_move: bool):
    """
    Main function to verify locations, find matches, and optionally move files.
    """
    print(f"Verifying locations for owner: '{owner_name}'")
    print(f"Searching for missing files in: '{output_dir}'")
    if should_move:
        print("MOVE flag is active. Files with a single match will be moved.")
    print("-" * 30)

    with SessionLocal() as db:
        locations = get_locations_for_owner(db, owner_name)

    if not locations:
        print(f"❌ Error: No locations found for owner '{owner_name}'. Please check the name.")
        return

    total_locations = len(locations)
    missing_locations = 0
    single_match_found = 0
    multiple_matches_found = 0

    # Store tuples of (source_path, destination_path) for the move operation
    files_to_move = []

    print("Step 1: Checking for missing files and finding matches...")
    with tqdm(total=total_locations, desc="Scanning locations", unit="loc") as pbar:
        for loc in locations:
            if not os.path.exists(loc.path):
                missing_locations += 1
                filename = os.path.basename(loc.path)

                # Search for the missing filename in the specified output directory
                matches = find_files_in_dir(output_dir, filename)

                if len(matches) == 1:
                    single_match_found += 1
                    # If a single match is found, add it to our list of files to move
                    files_to_move.append((matches[0], loc.path))
                elif len(matches) > 1:
                    multiple_matches_found += 1
            pbar.update(1)

    # --- Print Summary Report ---
    print("\n--- Verification Report ---")
    print(f"Total locations checked: {total_locations}")
    print(f"Missing locations: {missing_locations}")
    print("-" * 25)
    print(f"Found single match for: {single_match_found} missing files")
    print(f"Found multiple matches for: {multiple_matches_found} missing files")
    no_match_count = missing_locations - (single_match_found + multiple_matches_found)
    print(f"No match found for: {no_match_count} missing files")
    print("-" * 25)

    # --- Perform Move Operation if Flag is Set ---
    if should_move:
        if not files_to_move:
            print("No files to move.")
        else:
            print(f"\nStep 2: Moving {len(files_to_move)} files...")
            moved_count = 0
            with tqdm(total=len(files_to_move), desc="Moving files", unit="file") as move_pbar:
                for source_path, dest_path in files_to_move:
                    try:
                        # Ensure the destination directory exists before moving
                        dest_dir = os.path.dirname(dest_path)
                        os.makedirs(dest_dir, exist_ok=True)

                        shutil.move(source_path, dest_path)
                        moved_count += 1
                    except Exception as e:
                        print(f"\n❌ Error moving '{source_path}' to '{dest_path}': {e}")
                    move_pbar.update(1)
            print(f"\nSuccessfully moved {moved_count} files back to their original locations.")
    elif files_to_move:
        print("\nRun this script again with the --move flag to relocate the found files.")

    print("\n--- Process Complete ---")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Verify owner's file locations, find missing files in another directory, and optionally move them back.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("output_dir", type=str,
                        help="The directory to search for missing files (e.g., your export folder).")
    parser.add_argument("owner", type=str, help="The name of the owner whose files to verify.")
    parser.add_argument(
        "--move",
        action="store_true",  # This makes it a flag, e.g., --move
        help="If specified, move files with exactly one match from the output_dir back to their original location."
    )
    args = parser.parse_args()

    main(args.owner, args.output_dir, args.move)