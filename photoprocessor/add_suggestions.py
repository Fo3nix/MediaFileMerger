import os
import argparse
import sys
from tqdm import tqdm
from sqlalchemy.orm import selectinload

# Make sure the script can find your project modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from photoprocessor import models
from photoprocessor.database import SessionLocal


def main(owner_name: str, input_dir: str, base_dir: str, recursive: bool):
    """
    Adds suggested export paths to media files owned by a user, based on a
    folder's structure.
    """
    print("Initializing suggestion tool...")
    input_dir_abs = os.path.abspath(input_dir)
    base_dir_abs = os.path.abspath(base_dir)

    if not os.path.isdir(input_dir_abs):
        print(f"❌ ERROR: Input directory not found at '{input_dir_abs}'")
        return
    if not os.path.isdir(base_dir_abs):
        print(f"❌ ERROR: Base directory not found at '{base_dir_abs}'")
        return

    update_count = 0
    with SessionLocal() as db:
        print(f"Finding owner '{owner_name}'...")
        owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
        if not owner:
            print(f"❌ ERROR: Owner '{owner_name}' not found in the database.")
            return

        # Use the input directory to find the files to process.
        # os.path.join ensures a trailing slash for the startswith query.
        path_prefix = os.path.join(input_dir_abs, '')

        print(f"Querying for files in '{input_dir_abs}'...")
        ownerships_to_update = db.query(models.MediaOwnership).join(
            models.Location
        ).filter(
            models.MediaOwnership.owner_id == owner.id,
            models.Location.path.startswith(path_prefix)
        ).options(
            selectinload(models.MediaOwnership.location)
        ).all()

        if not ownerships_to_update:
            print("No files owned by this user found in the specified input directory.")
            return

        print(f"Found {len(ownerships_to_update)} files. Calculating suggestions relative to '{base_dir_abs}'...")

        with tqdm(total=len(ownerships_to_update), desc="Updating Suggestions", unit="file") as pbar:
            for mo in ownerships_to_update:
                try:
                    # Calculate the path relative to the BASE directory.
                    relative_path = os.path.relpath(mo.location.path, base_dir_abs)

                    # The suggestion is the directory part of the relative path.
                    suggested_dir = os.path.dirname(relative_path)

                    # If not recursive, only process files directly in the input_dir.
                    # We check this by seeing if the file's parent dir is the same as the input_dir.
                    if not recursive and os.path.dirname(mo.location.path) != input_dir_abs:
                        pbar.update(1)
                        continue

                    if mo.suggested_export_path != suggested_dir:
                        mo.suggested_export_path = suggested_dir
                        update_count += 1

                    pbar.update(1)
                except ValueError:
                    print(f"\nSkipping file on a different drive: {mo.location.path}")
                    pbar.update(1)
                    continue

        if update_count > 0:
            print(f"\nCommitting {update_count} updates to the database...")
            db.commit()
            print("✅ Commit successful.")
        else:
            print("\nNo changes needed; all suggestions are already up-to-date.")

    print("\n--- Suggestion Update Complete ---")
    print(f"Processed {len(ownerships_to_update)} file ownership records.")
    print(f"Updated {update_count} suggested export paths.")
    print("----------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Bulk add suggested export paths based on a folder structure.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("owner_name", type=str, help="The name of the owner of the files.")
    parser.add_argument(
        "--input-dir",
        required=True,
        help="The directory to scan for media files to process."
    )
    parser.add_argument(
        "--base-dir",
        help="The directory from which to calculate the relative export path.\n"
             "If not provided, defaults to the --input-dir."
    )
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Include subdirectories of the input directory in the scan."
    )

    args = parser.parse_args()

    # If base_dir is not specified, it defaults to input_dir for the original behavior.
    base_directory = args.base_dir if args.base_dir else args.input_dir

    main(args.owner_name, args.input_dir, base_directory, args.recursive)