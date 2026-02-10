import os
import argparse
import re
from pathlib import Path

# Regex to find the "Core" WhatsApp signature
# Matches: IMG-20180101-WA0001
# Ignores: (1), (2), -1, copy, etc. appended before the extension
WA_REGEX = re.compile(r'((?:IMG|VID|AUD|PTT)-\d{8}-WA\d+)', re.IGNORECASE)


def get_file_signature(filename: str) -> str:
    """
    Returns the unique 'signature' of a file.
    If it's a WhatsApp file, returns the ID (e.g., IMG...WA0001).
    If it's a regular file, returns the full filename.
    """
    match = WA_REGEX.search(filename)
    if match:
        # Return just the signature (e.g., "IMG-20180106-WA0001")
        # We lowercase it to ensure case-insensitive matching
        return match.group(1).lower()

    # Fallback: strict filename match for non-WhatsApp files
    return filename.lower()


def scan_signatures(directory: str) -> set:
    """Recursively scans a directory and builds a set of all found signatures."""
    signatures = set()
    print(f"Scanning {directory}...")

    for root, _, files in os.walk(directory):
        for file in files:
            if file.startswith('.'): continue  # Skip hidden files
            sig = get_file_signature(file)
            signatures.add(sig)

    return signatures


def main(source_dir, dest_dir):
    source_path = Path(source_dir).resolve()
    dest_path = Path(dest_dir).resolve()

    if not source_path.exists():
        print(f"❌ Error: Source directory '{source_dir}' does not exist.")
        return

    # 1. Map Source Files
    # We store a dict { signature: [list_of_original_paths] } to track duplicates
    source_map = {}
    total_source_files = 0

    print(f"--- Step 1: Scanning Source (Originals) ---")
    for root, _, files in os.walk(source_path):
        for file in files:
            if file.startswith('.'): continue

            full_path = os.path.join(root, file)
            sig = get_file_signature(file)

            if sig not in source_map:
                source_map[sig] = []
            source_map[sig].append(full_path)
            total_source_files += 1

    print(f"Found {total_source_files} files representing {len(source_map)} unique signatures.\n")

    # 2. Scan Destination
    print(f"--- Step 2: Scanning Destination (Export) ---")
    dest_signatures = scan_signatures(dest_path)
    print(f"Found {len(dest_signatures)} unique signatures in destination.\n")

    # 3. Compare
    print(f"--- Step 3: Verification Report ---")
    missing_signatures = []

    for sig, original_paths in source_map.items():
        if sig not in dest_signatures:
            missing_signatures.append((sig, original_paths))

    if not missing_signatures:
        print("✅ SUCCESS: All distinct media signatures from Source are present in Destination.")
        print(f"   (Merged {total_source_files - len(source_map)} duplicates successfully.)")
    else:
        print(f"❌ WARNING: {len(missing_signatures)} unique media files are MISSING from the export!")
        print("\nMISSING FILES:")
        for sig, paths in missing_signatures:
            print(f"  [Sig: {sig}]")
            for p in paths:
                print(f"    - {p}")

    print("-" * 30)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Verify that all unique WhatsApp signatures were exported.")
    parser.add_argument("source", help="Path to the ORIGINAL folder (e.g. Whatsapp Images)")
    parser.add_argument("destination", help="Path to the EXPORT folder")

    args = parser.parse_args()
    main(args.source, args.destination)