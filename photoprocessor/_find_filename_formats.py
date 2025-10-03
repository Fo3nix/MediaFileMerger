import os
import re
import argparse

def find_filename_formats(top_folder: str):
    """
    Scans a directory recursively to find all unique filename formats.
    Digits in filenames are replaced with '?' to create a generalized format.

    Args:
        top_folder: The path to the top-level directory to scan.
    """
    if not os.path.isdir(top_folder):
        print(f"❌ Error: Directory not found at '{top_folder}'")
        return

    # Dictionary to store: {format_string -> last_seen_example}
    filename_formats = {}
    files_scanned = 0

    print(f"🔍 Scanning '{os.path.abspath(top_folder)}' for filename formats...")

    # Recursively walk through the directory tree
    for root, _, files in os.walk(top_folder):
        for filename in files:
            files_scanned += 1
            # Get the filename part without its extension
            base_name = os.path.splitext(filename)[0]

            # Replace all digits with '?' to create the format string
            format_string = re.sub(r'\d', '?', base_name)

            # Store the format and the current base_name as its example
            # This automatically keeps the last-seen value for each format
            filename_formats[format_string] = base_name

    print(f"\n✅ Scan complete. Analyzed {files_scanned} files.")
    print(f"Found {len(filename_formats)} unique filename formats.\n")
    print("--- Filename Formats Found ---")

    if not filename_formats:
        print("No files were found to analyze.")
    else:
        # Find the longest format string for clean alignment
        max_len = max(len(key) for key in filename_formats.keys())
        # Print each format and its example, sorted for consistency
        for format_str, example in sorted(filename_formats.items()):
            print(f"{format_str:<{max_len}} => {example}")

    print("------------------------------")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scans a directory to find and report unique filename formats.",
        epilog="Example usage: python find_formats.py /path/to/your/photos"
    )
    parser.add_argument(
        "--directory",
        type=str,
        help="The path to the top-level directory you want to scan."
    )
    args = parser.parse_args()
    find_filename_formats(args.directory)