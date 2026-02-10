import os
import argparse
import logging
import sys
from typing import List, Dict
from tqdm import tqdm
from photoprocessor import models
from photoprocessor.database import SessionLocal
from photoprocessor.merger import MergePipeline
from photoprocessor.export_pipe import get_locations_for_owner, get_locations_by_paths, log_conflict

# Configuration (can be smaller as it's not I/O intensive)
CONFIG = {
    "BATCH_SIZE": 250,
}


def process_test_batch(
        batch_locations: List[models.Location],
        logger: logging.Logger,
        conflict_fp,
        merged_fp,
        pipeline: MergePipeline,
        processed_media_files: set
) -> Dict[str, int]:
    """
    Runs only the merge logic for a batch of files and records conflicts and successful merges.
    Logs details about which original files were merged into which resulting files.
    Ensures each media file is processed only once.
    """
    stats = {"scanned": 0, "conflicts": 0, "merged": 0}

    for loc in batch_locations:
        media_file_id = loc.media_file.id  # Unique identifier for the media file
        if media_file_id in processed_media_files:
            continue  # Skip already processed media files

        processed_media_files.add(media_file_id)
        stats["scanned"] += 1
        metadata_sources = loc.media_file.all_metadata_sources
        if not metadata_sources:
            continue

        # Run the exact same pipeline as the real export
        result_context = pipeline.run(metadata_sources)

        if result_context.conflicts:
            stats["conflicts"] += 1
            log_conflict(logger, loc.path, result_context.conflicts)
            conflict_fp.write(f"{loc.path}\n")
            conflict_fp.flush()
        else:
            stats["merged"] += 1
            if len(loc.media_file.locations) > 1:
                merged_fp.write(f"Result File: {loc.path}\n")
                merged_fp.write("Merged From:\n")
                for location in loc.media_file.locations:
                    merged_fp.write(f"  - {location.path}\n")
                merged_fp.write("\n")
                merged_fp.flush()

    return stats


def merge_tester_main(owner_name: str, filelist_path: str = None):
    """Main function to orchestrate the merge testing process."""
    print("Initializing Merge Tester (Dry Run)...")

    # --- Setup logging, same as export_pipe.py ---
    output_dir = "merge_test_results"
    os.makedirs(output_dir, exist_ok=True)
    conflict_log_path = os.path.join(output_dir, 'merge_conflicts.log')
    conflict_paths_file = os.path.join(output_dir, 'merge_conflicts_paths.txt')
    merged_paths_file = os.path.join(output_dir, 'merged_files.log')

    # Safety check to prevent overwriting an input file
    if filelist_path and os.path.abspath(filelist_path) == os.path.abspath(conflict_paths_file):
        print("\n❌ SAFETY ERROR: The input file list is the same as the conflict output file.")
        sys.exit(1)

    conflict_logger = logging.getLogger('merge_conflict_logger')
    conflict_logger.setLevel(logging.WARNING)
    fh = logging.FileHandler(conflict_log_path, mode='w', encoding='utf-8')
    formatter = logging.Formatter('%(asctime)s - FILE: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)
    conflict_logger.addHandler(fh)

    total_stats = {"scanned": 0, "conflicts": 0, "merged": 0}

    # --- Instantiate the exact same pipeline from the export process ---
    export_merge_pipeline = MergePipeline.get_default_pipeline()

    try:
        with SessionLocal() as db, open(conflict_paths_file, 'w', encoding='utf-8') as conflict_fp, open(merged_paths_file, 'w', encoding='utf-8') as merged_fp:
            # --- Use the exact same query logic from export_pipe.py ---
            locations_to_test = []
            if filelist_path:
                with open(filelist_path, 'r', encoding='utf-8') as f:
                    paths = [line.strip() for line in f if line.strip()]
                locations_to_test = get_locations_by_paths(db, paths)
            elif owner_name:
                owner = db.query(models.Owner).filter(models.Owner.name == owner_name).first()
                if not owner:
                    raise ValueError(f"Owner '{owner_name}' not found.")
                locations_to_test = get_locations_for_owner(db, owner)

            if not locations_to_test:
                print("No files found to test.")
                return

            total_files = len(locations_to_test)
            print(f"Found {total_files} files to test for merge conflicts.")

            global_processed_ids = set()

            with tqdm(total=total_files, desc="Testing Merges", unit="file") as pbar:
                for i in range(0, total_files, CONFIG["BATCH_SIZE"]):
                    batch = locations_to_test[i:i + CONFIG["BATCH_SIZE"]]
                    stats = process_test_batch(batch, conflict_logger, conflict_fp, merged_fp, export_merge_pipeline, global_processed_ids)

                    total_stats["scanned"] += stats["scanned"]
                    total_stats["conflicts"] += stats["conflicts"]
                    total_stats["merged"] += stats["merged"]

                    pbar.update(len(batch))
                    pbar.set_postfix(scanned=total_stats['scanned'], conflicts=total_stats['conflicts'], merged=total_stats['merged'])
    finally:
        print("\n--- Merge Test Complete ---")
        print(f"✅ Scanned {total_stats['scanned']} files.")
        print(f"✅ Successfully merged {total_stats['merged']} files.")
        if total_stats['conflicts'] > 0:
            print(f"⚠️ Found {total_stats['conflicts']} files with merge conflicts.")
            print(f"   See conflict details in the log file: {conflict_log_path}")
            print(f"   A list of conflicted file paths has been saved to: {conflict_paths_file}")
        else:
            print("🎉 No merge conflicts found!")
        print(f"   A list of successfully merged file paths has been saved to: {merged_paths_file}")
        print("---------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dry run the metadata merge process to find conflicts.")
    parser.add_argument("--owner", type=str, help="The name of the owner whose files to test.")
    parser.add_argument("--filelist", "-f", type=str, help="Optional path to a file with paths to test.")
    args = parser.parse_args()

    if not args.owner and not args.filelist:
        parser.error("Either an --owner or the --filelist argument must be provided.")

    merge_tester_main(args.owner, args.filelist)