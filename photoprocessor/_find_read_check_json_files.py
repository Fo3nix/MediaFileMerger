import os
import json
import argparse
from tqdm import tqdm  # Import the tqdm library


def find_jsons_without_title(directory: str):
    """
    Scans a directory recursively for .json files and checks for a non-empty 'title' key.
    Prints the absolute path of files that are missing the key or have an empty/whitespace value.

    Args:
        directory (str): The path to the top-level directory to scan.
    """
    print(f"🔍 Preliminary scan: Finding all .json files in {os.path.abspath(directory)}...")

    # --- First Pass: Collect all file paths to get a total for the progress bar ---
    json_file_paths = []
    for root, _, files in os.walk(directory):
        for filename in files:
            if filename.lower().endswith('.json'):
                json_file_paths.append(os.path.join(root, filename))

    if not json_file_paths:
        print("\nNo .json files found in the specified directory.")
        return

    print(f"Found {len(json_file_paths)} files. Now checking contents...")
    issue_count = 0

    # --- Second Pass: Process the files with a tqdm progress bar ---
    # Wrap the list of paths in tqdm() to create and manage the progress bar
    for file_path in tqdm(json_file_paths, desc="Checking JSONs", unit="file"):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)

            title_value = data.get('title')

            if title_value is None or not isinstance(title_value, str) or not title_value.strip() or title_value.strip() == '':
                # Use tqdm.write() to print output without disturbing the progress bar
                tqdm.write(os.path.abspath(file_path))
                issue_count += 1
            else:
                # if title value contains -edit or -edited (case insensitive), also report it
                if '-edit' in title_value.lower() or '-edited' in title_value.lower():
                    tqdm.write(os.path.abspath(file_path))
                    issue_count += 1

                if '(' in title_value and ')' in title_value:
                    tqdm.write(os.path.abspath(file_path))
                    issue_count += 1

        except json.JSONDecodeError:
            tqdm.write(f"⚠️  Warning: Could not decode JSON in file: {os.path.abspath(file_path)}")
        except Exception as e:
            tqdm.write(f"❌ Error processing file {os.path.abspath(file_path)}: {e}")

    print(f"\n✅ Scan complete. Found {issue_count} files missing a valid 'title' key.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Find all .json files in a directory that are missing a non-empty 'title' key."
    )
    parser.add_argument(
        "directory",
        type=str,
        help="The path to the directory you want to scan recursively."
    )
    args = parser.parse_args()

    if not os.path.isdir(args.directory):
        print(f"❌ Error: The provided path '{args.directory}' is not a valid directory.")
    else:
        find_jsons_without_title(args.directory)