#!/usr/bin/env python3

import argparse
import sys
from pathlib import Path
from sqlalchemy.orm import Session
from typing import Set, Dict, Any
from collections import defaultdict

# Import the models and session factory from your existing files
from models import Owner, Location, MediaOwnership
from database import SessionLocal


def print_summary_report(results: Dict[str, Any], total_scanned: int, total_found: int, total_missing: int):
    """Prints a detailed, structured summary of the scan results."""
    print("\n" + "=" * 50)
    print("📊 Final Scan Report")
    print("=" * 50)
    print(f"Total files scanned: {total_scanned}")
    print(f"✔️ Total FOUND in DB for this owner: {total_found}")
    print(f"❌ Total MISSING from DB for this owner: {total_missing}")

    if not results:
        print("\nNo files were found in the specified directory.")
        return

    print("\n--- Detailed Breakdown by File Type ---\n")

    # Sort extensions for consistent output, e.g., .jpeg, .jpg, .mov, .png
    for extension, dir_data in sorted(results.items()):
        ext_found = sum(counts['found'] for counts in dir_data.values())
        ext_missing = sum(counts['missing'] for counts in dir_data.values())

        print(f"📁 Extension: {extension} (Found: {ext_found}, Missing: {ext_missing})")

        # Sort directories for consistent output
        for dir_path, counts in sorted(dir_data.items()):
            found_count = counts['found']
            missing_count = counts['missing']

            # Only print directories that have relevant files
            if found_count > 0 or missing_count > 0:
                print(f"  - In '{dir_path}':")
                print(f"      ✔️ Found: {found_count}  |  ❌ Missing: {missing_count}")
        print("-" * 20)


def check_directory_for_owner(db: Session, owner_name: str, directory_path: str):
    """
    Checks files in a directory against the database for a specific owner,
    then generates a detailed summary report.

    Args:
        db: The SQLAlchemy session object.
        owner_name: The name of the owner to check for.
        directory_path: The path to the directory to scan.
    """
    # 1. Validate owner
    print(f"🔎 Verifying owner '{owner_name}' exists...")
    owner = db.query(Owner).filter(Owner.name == owner_name).first()
    if not owner:
        print(f"❌ Error: Owner '{owner_name}' not found in the database.", file=sys.stderr)
        sys.exit(1)
    print(f"✅ Owner '{owner_name}' found.")

    # 2. Validate directory
    directory = Path(directory_path)
    if not directory.is_dir():
        print(f"❌ Error: Directory '{directory_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    # 3. Efficiently fetch all location paths for this owner
    print("🚀 Fetching all known file paths for this owner...")
    paths_query = (
        db.query(Location.path)
        .join(MediaOwnership)
        .filter(MediaOwnership.owner_id == owner.id)
    )
    owner_paths_in_db: Set[str] = {path_tuple[0] for path_tuple in paths_query.all()}
    print(f"👍 Found {len(owner_paths_in_db)} database entries for '{owner_name}'.\n")

    # 4. Scan directory and compile results
    # Use a nested defaultdict for easily creating the structure
    # results[extension][directory] = {'found': 0, 'missing': 0}
    results: Dict[str, Any] = defaultdict(lambda: defaultdict(lambda: {'found': 0, 'missing': 0}))

    files_in_dir = [p for p in directory.rglob('*') if p.is_file()]
    total_files = len(files_in_dir)
    total_found = 0
    total_missing = 0

    print(f"🔄 Scanning {total_files} files in '{directory.resolve()}'...")
    for i, file_path in enumerate(files_in_dir, 1):
        # Update progress on the same line
        print(f"   Processing: {i}/{total_files}", end='\r')

        abs_path_str = str(file_path.resolve())
        parent_dir = str(file_path.parent)
        # Use lower() for case-insensitive extension grouping
        extension = file_path.suffix.lower() if file_path.suffix else ".<no_extension>"

        if abs_path_str in owner_paths_in_db:
            results[extension][parent_dir]['found'] += 1
            total_found += 1
        else:
            results[extension][parent_dir]['missing'] += 1
            total_missing += 1

    # Clear the progress line before printing the final report
    print(" " * 50, end='\r')

    # 5. Print the final, structured report
    print_summary_report(results, total_files, total_found, total_missing)


def main():
    """Main function to parse arguments and run the check."""
    parser = argparse.ArgumentParser(
        description="Check if files in a directory have a location registered for a specific owner.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("owner_name", help="The name of the owner to check against.")
    parser.add_argument("directory", help="The path to the directory to scan for files.")

    args = parser.parse_args()

    db: Session = SessionLocal()
    try:
        check_directory_for_owner(db, args.owner_name, args.directory)
    finally:
        db.close()


if __name__ == "__main__":
    main()