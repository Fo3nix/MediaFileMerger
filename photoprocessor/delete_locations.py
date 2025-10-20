import os
import argparse
import sys
from tqdm import tqdm
from sqlalchemy.orm import joinedload

# Ensure the script can find project modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from photoprocessor import models
from photoprocessor.database import SessionLocal


def main(owner_name: str, folder_path: str, recursive: bool, force: bool):
    """
    Finds and deletes Location records from the database based on owner and path.
    """
    print("Initializing location deletion tool...")
    folder_path_abs = os.path.abspath(folder_path)

    if not os.path.isdir(folder_path_abs):
        print(f"❌ ERROR: Input directory not found at '{folder_path_abs}'")
        return

    with SessionLocal() as db:
        # 1. Find the owner
        print(f"Finding owner '{owner_name}'...")
        owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
        if not owner:
            print(f"❌ ERROR: Owner '{owner_name}' not found in the database.")
            return

        # 2. Build the base query to find candidate locations
        # We start with a recursive search as it's an efficient 'startswith' query.
        # We also load the related MediaOwnership to ensure we only get locations
        # linked to the correct owner.
        path_prefix = os.path.join(folder_path_abs, '')

        query = db.query(models.Location).join(
            models.MediaOwnership
        ).filter(
            models.MediaOwnership.owner_id == owner.id,
            models.Location.path.startswith(path_prefix)
        )

        # Eagerly load the owners relationship to prevent extra queries if needed later.
        query = query.options(joinedload(models.Location.owners))

        locations_to_delete = query.all()

        # 3. If not recursive, filter the results down in Python.
        # This is more efficient than trying to perform a complex non-recursive
        # path match in a database-agnostic way.
        if not recursive:
            locations_to_delete = [
                loc for loc in locations_to_delete
                if os.path.dirname(loc.path) == folder_path_abs
            ]

        if not locations_to_delete:
            print("✅ No matching locations found for the given criteria. Nothing to do.")
            return

        # 4. CRITICAL: Get user confirmation before deleting
        print("\n--- PENDING DELETION ---")
        print(f"Found {len(locations_to_delete)} database locations to delete.")
        print(f"Owner: {owner.name}")
        print(f"Folder: {folder_path_abs}")
        print(f"Recursive: {recursive}")
        print("\n⚠️ WARNING: This action is IRREVERSIBLE and will delete database records.")
        print("   This includes the Location itself and its associated Ownership and Metadata records.")
        print("   This script DOES NOT delete the actual files from your disk.")

        if not force:
            confirmation = input('Type "yes" to proceed with the deletion: ')
            if confirmation.lower() != 'yes':
                print("\nDeletion cancelled by user.")
                return

        # 5. Perform the deletion
        # This relies on the cascade="all, delete-orphan" setting on the
        # Location.owners and Location.metadata_sources relationships in models.py
        # to correctly remove dependent records (MediaOwnership, MetadataSource, etc.).
        print("\nDeleting records from the database...")
        try:
            with tqdm(total=len(locations_to_delete), desc="Deleting Locations", unit="record") as pbar:
                for loc in locations_to_delete:
                    db.delete(loc)
                    pbar.update(1)

            print("Committing changes...")
            db.commit()
            print("✅ Commit successful.")

        except Exception as e:
            print(f"\n❌ An error occurred during deletion. Rolling back changes.")
            print(f"   Error: {e}")
            db.rollback()

    print("\n--- Deletion Complete ---")
    print(f"Successfully deleted {len(locations_to_delete)} location records.")
    print("-------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Delete Location records from the database for a specific owner and folder.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("owner_name", type=str, help="The name of the owner.")
    parser.add_argument("folder_path", type=str, help="The folder path containing the locations to delete.")
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Recursively delete locations in all subdirectories of the folder path."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip the interactive confirmation prompt. Use with caution."
    )

    args = parser.parse_args()

    main(args.owner_name, args.folder_path, args.recursive, args.force)