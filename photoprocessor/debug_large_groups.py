import sys
import os

# Add the parent directory to sys.path to allow imports from photoprocessor
# This assumes the script is run directly or from the root, but we want to be safe about imports.
# If this file is in /.../photoprocessor/debug_large_groups.py, the parent is /.../photoprocessor/
# We want to import 'photoprocessor', so we need /.../ in sys.path.
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from sqlalchemy import func, not_
from photoprocessor.database import SessionLocal
from photoprocessor.models import MediaFile, Location

def main():
    session = SessionLocal()
    try:
        print("Querying for file groups of size > 2 (excluding '(conflict ' in path)...")

        # We need to find MediaFiles that have more than 2 locations which do NOT contain "(conflict "
        # 1. Join MediaFile and Location
        # 2. Filter out locations with "(conflict "
        # 3. Group by MediaFile
        # 4. Count locations > 2

        media_files_with_large_groups = (
            session.query(MediaFile)
            .join(Location)
            .filter(not_(Location.path.contains("(conflict ")))
            .group_by(MediaFile.id)
            .having(func.count(Location.id) > 2)
            .all()
        )

        if not media_files_with_large_groups:
             print("No groups found satisfying the condition.")
             return

        print(f"Found {len(media_files_with_large_groups)} groups.")

        for mf in media_files_with_large_groups:
            # Re-filter locations in python for display to ensure we show the right ones
            # (mf.locations might contain all locations if lazy/eager loaded without filter)
            valid_locations = [
                loc for loc in mf.locations
                if "(conflict " not in loc.path
            ]

            # The count in HAVING clause ensures valid_locations > 2, but let's be safe
            if len(valid_locations) <= 2:
                # Should not happen if query is correct
                continue

            print("-" * 60)
            print(f"MediaFile ID: {mf.id}")
            print(f"Hash: {mf.file_hash}")
            print(f"Valid Copies Count: {len(valid_locations)}")
            print("Locations:")
            for loc in valid_locations:
                print(f"  {loc.path}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        session.close()

if __name__ == "__main__":
    main()
