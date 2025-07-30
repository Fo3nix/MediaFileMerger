import hashlib
import json
import os
from PIL import Image
from PIL.ExifTags import TAGS

# For HEIC support, if needed
try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except ImportError:
    pass


class PhotoProcessor:
    """Processes a single photo file to extract data, hashes, and metadata."""

    def hash_file_content(self, filepath):
        """Computes the SHA256 hash of a file's content."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def get_exif_data(self, filepath):
        """
        Extracts EXIF data, filtering for clean, readable values,
        and returns it as a sorted JSON string.
        """
        try:
            with Image.open(filepath) as img:
                exif_data_dict = {}

                # Load EXIF data robustly
                exif = img.getexif()
                if not exif and 'exif' in img.info:
                    exif = Image.Exif()
                    exif.load(img.info['exif'])

                # --- Process the main EXIF block with improved filtering ---
                if exif:
                    for tag_id, value in exif.items():
                        tag_name = TAGS.get(tag_id, str(tag_id))

                        # 1. If it's bytes, try to decode it into clean, printable text
                        if isinstance(value, bytes):
                            try:
                                # Decode, then strip whitespace and null characters
                                decoded_value = value.decode('utf-8', 'ignore').strip().strip('\x00')
                                # Only add if it's not empty and is readable
                                if decoded_value and decoded_value.isprintable():
                                    exif_data_dict[str(tag_name)] = decoded_value
                            except Exception:
                                # Ignore bytes that can't be processed
                                pass
                        # 2. If it's already a simple type, just add it
                        elif isinstance(value, (int, float, str)):
                            exif_data_dict[str(tag_name)] = str(value)
                        # We will intentionally skip other complex binary types

                # --- Process other .info items, also filtering for clean data ---
                for key, value in img.info.items():
                    if key != 'exif':  # Skip the raw block we already processed
                        if isinstance(value, (int, float, str)):
                            exif_data_dict[key] = str(value)
                        elif isinstance(value, tuple):  # Handle tuples like DPI
                            exif_data_dict[key] = str(value)

                if not exif_data_dict:
                    return None

                return json.dumps(exif_data_dict, sort_keys=True)

        except Exception as e:
            print(f"Error processing EXIF for {os.path.basename(filepath)}: {str(e)}")
            return None

    def find_google_json(self, image_path):
        """Finds and reads the corresponding Google Takeout JSON metadata file."""
        base, ext = os.path.splitext(image_path)

        # List of potential JSON filenames to check
        possible_json_paths = [
            image_path + ".json",  # IMG_1234.JPG.json
            base + ".json"  # IMG_1234.json
        ]

        # Handle cases like 'IMG_1234-edited.JPG' -> 'IMG_1234.JPG.json'
        if "-edited" in os.path.basename(base):
            original_base = base.replace("-edited", "")
            edited_path = original_base + os.path.splitext(image_path)[1] + ".json"
            possible_json_paths.append(edited_path)

        # Handle cases like 'IMG_1234-supplemental-metadata.json'
        possible_json_paths.append(base + ".supplemental-metadata.json")
        possible_json_paths.append(image_path + ".supplemental-metadata.json")

        # Check each possible JSON path
        json_paths = [p for p in possible_json_paths if os.path.exists(p)]

        if not json_paths:
            print(f"No JSON metadata found for {image_path}, for paths: {possible_json_paths}")
            return None

        data = {}
        for json_path in json_paths:
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    json_data = json.load(f)
                    data.update(json_data)
            except Exception as e:
                print(f"Error reading JSON file {json_path}: {str(e)}")

        return json.dumps(data, sort_keys=True)

    def process(self, filepath):
        """
        Processes a single image file and returns all its relevant data.

        Returns:
            A dictionary of the photo's data or None if processing fails.
        """
        if not os.path.exists(filepath):
            return None

        exif_json = self.get_exif_data(filepath)
        metadata_hash = hashlib.sha256(exif_json.encode()).hexdigest() if exif_json else None

        return {
            "filename": os.path.basename(filepath),
            "local_file_location": filepath,
            "image_hash": self.hash_file_content(filepath),
            "metadata_hash": metadata_hash,
            "actual_metadata": exif_json,
            "old_metadata": exif_json,
            "google_takeout_metadata": self.find_google_json(filepath)
        }