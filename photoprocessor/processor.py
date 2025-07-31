import hashlib
import json
import os
from datetime import datetime
from PIL import Image
from PIL.ExifTags import TAGS

# For HEIC support
try:
    from pillow_heif import register_heif_opener

    register_heif_opener()
except ImportError:
    pass


class PhotoProcessor:
    """Processes a single photo file to extract and structure data for the database."""

    def _hash_file_content(self, filepath):
        """Computes the SHA256 hash of a file's content."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _get_raw_exif_dict(self, filepath):
        """
        Extracts EXIF data robustly, checking multiple sources within the image file,
        and returns a clean dictionary.
        """
        try:
            with Image.open(filepath) as img:
                exif_data_dict = {}

                # Load EXIF data, checking both primary and info attributes
                exif = img.getexif()
                if not exif and 'exif' in img.info:
                    exif = Image.Exif()
                    exif.load(img.info['exif'])

                # Process the main EXIF block with improved filtering
                if exif:
                    for tag_id, value in exif.items():
                        tag_name = TAGS.get(tag_id, str(tag_id))
                        if isinstance(value, bytes):
                            # Decode, then strip whitespace and null characters
                            decoded_value = value.decode('utf-8', 'ignore').strip().strip('\x00')
                            if decoded_value and decoded_value.isprintable():
                                exif_data_dict[tag_name] = decoded_value
                        elif isinstance(value, (int, float, str)):
                            exif_data_dict[tag_name] = value

                # Process other .info items for additional metadata (like DPI)
                for key, value in img.info.items():
                    if key != 'exif':  # Skip the raw block we already processed
                        if isinstance(value, (int, float, str)):
                            exif_data_dict[key] = value
                        elif isinstance(value, tuple):
                            exif_data_dict[key] = str(value)

                # Also include basic image info
                exif_data_dict['width'] = img.width
                exif_data_dict['height'] = img.height

                return exif_data_dict if exif_data_dict else None
        except Exception:
            return None

    def _get_google_json_dict(self, image_path):
        """
        Finds and reads all corresponding Google Takeout JSON metadata files,
        merging them into a single dictionary.
        """
        base, ext = os.path.splitext(image_path)
        possible_json_paths = [
            image_path + ".json",  # IMG_1234.JPG.json
            base + ".json"  # IMG_1234.json
        ]

        # Handle cases like 'IMG_1234-edited.JPG' -> 'IMG_1234.JPG.json'
        if "-edited" in os.path.basename(base):
            original_base = base.replace("-edited", "")
            edited_path = original_base + ext + ".json"
            possible_json_paths.append(edited_path)

        # Handle cases like +'.supplemental-metadata.json'
        possible_json_paths += [
            base + '.supplemental-metadata.json',  # IMG_1234.supplemental-metadata.json
            base + '.supplemental_metadata.json',  # IMG_1234.supplemental_metadata.json
            base + ext + '.supplemental-metadata.json',  # IMG_1234.JPG.supplemental-metadata.json
            base + ext + '.supplemental_metadata.json'  # IMG_1234.JPG.supplemental_metadata.json
        ]

        # Check all possible paths
        found_paths = [p for p in possible_json_paths if os.path.exists(p)]
        if not found_paths:
            return None

        # Merge data from all found JSON files
        merged_data = {}
        for json_path in found_paths:
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    merged_data.update(json.load(f))
            except Exception:
                continue  # Ignore errors in individual JSON files

        return merged_data if merged_data else None

    def _to_datetime(self, date_str):
        """Safely converts a string to a datetime object from common formats."""
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            return datetime.strptime(date_str, '%Y:%m:%d %H:%M:%S')
        except ValueError:
            try:
                if date_str.endswith('Z'):
                    date_str = date_str[:-1] + '+00:00'
                return datetime.fromisoformat(date_str)
            except ValueError:
                return None

    def _parse_master_metadata(self, raw_exif):
        """Parses raw EXIF data into the format for the main 'Metadata' table."""
        if not raw_exif:
            return None
        return {
            "description": raw_exif.get("ImageDescription"),
            "date_taken": self._to_datetime(raw_exif.get("DateTimeOriginal") or raw_exif.get("DateTime")),
            "camera_make": raw_exif.get("Make"),
            "camera_model": raw_exif.get("Model"),
            "lens_model": raw_exif.get("LensModel"),
            "focal_length": raw_exif.get("FocalLength"),
            "aperture": raw_exif.get("FNumber"),
            "iso": raw_exif.get("ISOSpeedRatings"),
            "width": raw_exif.get("width"),
            "height": raw_exif.get("height"),
        }

    def _parse_google_metadata(self, google_json):
        """Parses Google JSON data for the 'GooglePhotosMetadata' table."""
        if not google_json:
            return None
        creation_time = google_json.get("photoTakenTime", {}).get("timestamp")
        modified_time = google_json.get("photoLastModifiedTime", {}).get("timestamp")
        return {
            "title": google_json.get("title"),
            "description": google_json.get("description"),
            "creation_timestamp": datetime.fromtimestamp(int(creation_time)) if creation_time else None,
            "modified_timestamp": datetime.fromtimestamp(int(modified_time)) if modified_time else None,
            "google_url": google_json.get("url"),
            "is_favorited": google_json.get("favorited", False),
            "gps_latitude": google_json.get("geoData", {}).get("latitude"),
            "gps_longitude": google_json.get("geoData", {}).get("longitude"),
        }

    def process(self, filepath, base_path):
        """
        Processes a file and returns a structured dictionary for all database models.
        """
        if not os.path.exists(filepath):
            return None

        file_hash = self._hash_file_content(filepath)
        raw_exif_dict = self._get_raw_exif_dict(filepath)
        google_json_dict = self._get_google_json_dict(filepath)

        file_size = os.path.getsize(filepath)

        base_path = os.path.abspath(base_path)
        file_name = os.path.basename(filepath)

        relative_path = os.path.relpath(filepath, base_path)
        relative_path = os.path.dirname(relative_path)

        mime_type = None
        ext = os.path.splitext(file_name)[1].lower()
        # possible extensions: ('.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.heic', '.webp'
        #                         '.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv')
        if ext in ['.jpg', '.jpeg']:
            mime_type = 'image/jpeg'
        elif ext == '.png':
            mime_type = 'image/png'
        elif ext == '.gif':
            mime_type = 'image/gif'
        elif ext == '.bmp':
            mime_type = 'image/bmp'
        elif ext == '.tiff':
            mime_type = 'image/tiff'
        elif ext == '.heic':
            mime_type = 'image/heic'
        elif ext == '.webp':
            mime_type = 'image/webp'
        elif ext in ['.mp4', '.mov', '.avi', '.mkv', '.flv', '.wmv']:
            mime_type = 'video/' + ext[1:]
        else:
            mime_type = 'unknown/unknown'

        return {
            "media_file": {
                "file_hash": file_hash,
                "filename": file_name,
                "base_path": base_path,
                "relative_path": relative_path,
                "mime_type": mime_type,
                "file_size": file_size,
            },
            "metadata": self._parse_master_metadata(raw_exif_dict),
            "google_metadata": self._parse_google_metadata(google_json_dict),
            "raw_exif": {"data": raw_exif_dict} if raw_exif_dict else None,
            "raw_google_json": {"data": google_json_dict} if google_json_dict else None
        }