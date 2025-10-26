#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
A script to update or create 'EXIF:DateTimeOriginal' metadata for a list of
media files based on their filesystem modification time.

This script connects to the PhotoProcessor database, reads a list of filenames
from a specified text file, and for each file:
1. Constructs the full path using a provided base directory.
2. Finds the corresponding 'Location' entry in the database.
3. Verifies that the location is associated with the specified owner.
4. Retrieves the file's last modification time (mtime).
5. Creates or updates the 'MetadataEntry' for the key 'EXIF:DateTimeOriginal'
   with the mtime, ensuring it is stored as a timezone-aware UTC datetime.
"""

import os
import argparse
from datetime import datetime, timezone

from sqlalchemy.orm import Session, joinedload
from tqdm import tqdm

# Ensure your project structure allows this import.
# This might mean running the script from the root directory of your project
# or having the 'photoprocessor' package installed.
from photoprocessor.database import SessionLocal
from photoprocessor.models import Owner, Location, MetadataSource, MetadataEntry, MediaOwnership


def update_datetime_from_mtime(db: Session, filelist_path: str, base_dir: str, owner_name: str):
    """
    Updates or creates an EXIF:DateTimeOriginal metadata entry for a list of files,
    using each file's last modification time.

    Args:
        db (Session): The SQLAlchemy database session.
        filelist_path (str): Path to the text file containing filenames.
        base_dir (str): The base directory where files are located.
        owner_name (str): The name of the media owner.
    """
    # 1. Get the owner object from the database
    owner = db.query(Owner).filter(Owner.name == owner_name).first()
    if not owner:
        print(f"❌ ERROR: Owner '{owner_name}' not found in the database.")
        return

    print(f"Found owner: {owner.name} (ID: {owner.id})")

    # 2. Read the list of files
    try:
        with open(filelist_path, 'r', encoding='utf-8') as f:
            filenames = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        print(f"❌ ERROR: Input file not found at '{filelist_path}'")
        return

    if not filenames:
        print("File list is empty. Nothing to do.")
        return

    print(f"Processing {len(filenames)} files from '{filelist_path}'...")

    stats = {"success": 0, "db_not_found": 0, "owner_mismatch": 0, "fs_not_found": 0}

    # 3. Process each file with a progress bar
    for filename in tqdm(filenames, desc="Updating Metadata", unit="file"):
        full_path = os.path.join(base_dir, filename)
        abs_path = os.path.abspath(full_path)

        # a. Get file modification time from the filesystem
        try:
            mtime = os.path.getmtime(abs_path)
            mod_datetime = datetime.fromtimestamp(mtime)
        except FileNotFoundError:
            tqdm.write(f"Warning: File not found on disk: {abs_path}")
            stats["fs_not_found"] += 1
            continue

        # b. Find the location in the database, pre-loading owner info to prevent N+1 queries
        location = db.query(Location).options(
            joinedload(Location.owners).joinedload(MediaOwnership.owner)
        ).filter(Location.path == abs_path).first()

        if not location:
            stats["db_not_found"] += 1
            continue

        # c. Verify ownership
        if not any(ownership.owner_id == owner.id for ownership in location.owners):
            stats["owner_mismatch"] += 1
            continue

        # d. Find or create the 'exif' MetadataSource for this location
        # This is the parent object for all EXIF-related key-value pairs
        exif_source = db.query(MetadataSource).filter(
            MetadataSource.location_id == location.id,
            MetadataSource.source == 'exif'
        ).first()

        if not exif_source:
            exif_source = MetadataSource(
                location=location,
                source='exif',
                raw_data={"note": f"Source created by update_mtime_as_exif.py at {datetime.now(timezone.utc).isoformat()}"}
            )
            db.add(exif_source)

        # e. Find an existing 'EXIF:DateTimeOriginal' entry for this source
        metadata_entry = db.query(MetadataEntry).filter(
            MetadataEntry.source_id == exif_source.id,
            MetadataEntry.key == 'EXIF:DateTimeOriginal'
        ).first()

        if metadata_entry:
            # Update the existing entry's datetime value
            metadata_entry.value_dt = mod_datetime
            metadata_entry.value_str = None  # Clear other potential value types
            metadata_entry.value_real = None
        else:
            # Create a new entry if it doesn't exist
            new_entry = MetadataEntry(
                source_info=exif_source,
                key='EXIF:DateTimeOriginal',
                value_dt=mod_datetime
            )
            db.add(new_entry)

        stats["success"] += 1

    # 4. Commit all changes to the database in a single transaction
    print("\nCommitting changes to the database...")
    db.commit()
    print("Commit complete.")

    # 5. Print a summary of the operation
    print("\n--- Update Summary ---")
    print(f"✅ Successfully updated/created metadata for {stats['success']} files.")
    if stats['db_not_found'] > 0:
        print(f"⚠️ {stats['db_not_found']} files were not found in the database.")
    if stats['owner_mismatch'] > 0:
        print(f"⚠️ {stats['owner_mismatch']} files did not match the specified owner.")
    if stats['fs_not_found'] > 0:
        print(f"❌ {stats['fs_not_found']} files were not found on the filesystem.")
    print("----------------------")


def main():
    """Parses command-line arguments and runs the update process."""
    parser = argparse.ArgumentParser(
        description="Create/update 'EXIF:DateTimeOriginal' metadata from file modification times.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "filelist",
        type=str,
        help="Path to a text file with one filename per line."
    )
    parser.add_argument(
        "--base_dir",
        "-b",
        type=str,
        required=True,
        help="The base directory where the files listed in 'filelist' are located."
    )
    parser.add_argument(
        "--owner",
        "-o",
        type=str,
        required=True,
        help="The name of the owner for the media files as it appears in the database."
    )

    args = parser.parse_args()

    # Create a new database session
    db_session = SessionLocal()
    try:
        # Run the main logic
        update_datetime_from_mtime(db_session, args.filelist, args.base_dir, args.owner)
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")
        db_session.rollback()
    finally:
        # Always close the session
        db_session.close()


if __name__ == "__main__":
    main()