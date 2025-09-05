import hashlib
import subprocess
import json
import os
from datetime import datetime
import magic


class PhotoProcessor:
    """Processes a single photo file to extract and structure data for the database."""

    def _hash_file_content(self, filepath):
        """Computes the SHA256 hash of a file's content."""
        sha256_hash = hashlib.sha256()
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def _get_exiftool_batch_dict(self, filepaths: list[str]) -> list[dict]:
        """
        Extracts metadata from a BATCH of files using a single exiftool call.
        """
        try:
            # Pass all filepaths to a single exiftool command.
            # It will return a list of JSON objects, one for each file.
            args = ["exiftool", "-G", "-n", "-json", *filepaths]
            result = subprocess.run(args, check=True, capture_output=True, text=True)
            return json.loads(result.stdout)
        except (FileNotFoundError):
            print("ERROR: exiftool command not found. Please install it and ensure it's in your PATH.")
            raise
        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            print(f"Warning: Could not get exiftool data for a batch: {e}")
            # Return an empty list of the same size so the caller can map results.
            return [{} for _ in filepaths]

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
        """Parses raw EXIF data from EXIFTOOL into the format for the 'Metadata' table."""
        if not raw_exif:
            return None

        # Note the new keys with group names like "EXIF:" and "File:"
        # We check multiple tags for some fields, as they can exist in different places.
        date_taken_str = raw_exif.get("EXIF:DateTimeOriginal") or \
                         raw_exif.get("QuickTime:CreateDate") or \
                         raw_exif.get("EXIF:CreateDate")

        return {
            "description": raw_exif.get("XMP:Description") or raw_exif.get("EXIF:ImageDescription"),
            "date_taken": self._to_datetime(date_taken_str),
            "camera_make": raw_exif.get("EXIF:Make"),
            "camera_model": raw_exif.get("EXIF:Model"),
            "lens_model": raw_exif.get("EXIF:LensModel"),
            "focal_length": raw_exif.get("EXIF:FocalLength"),
            "aperture": raw_exif.get("EXIF:FNumber"),
            "iso": raw_exif.get("EXIF:ISO"),
            "width": raw_exif.get("File:ImageWidth"),
            "height": raw_exif.get("File:ImageHeight"),
            "duration_seconds": raw_exif.get("QuickTime:Duration"),  # For videos!
            "gps_latitude": raw_exif.get("EXIF:GPSLatitude"),
            "gps_longitude": raw_exif.get("EXIF:GPSLongitude"),
            "rating": raw_exif.get("XMP:Rating"),
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

    def process_batch(self, filepaths: list[str]) -> dict[str, dict]:
        """
        Processes a BATCH of files and returns a dictionary mapping
        filepath -> structured data.
        """
        results = {}
        # Get all exif data in one shot
        all_raw_exif = self._get_exiftool_batch_dict(filepaths)

        # Create a map of SourceFile -> exif_data for easy lookup
        exif_map = {os.path.abspath(d.get('SourceFile')): d for d in all_raw_exif}

        for filepath in filepaths:
            if not os.path.exists(filepath):
                continue

            raw_exif_dict = exif_map.get(filepath)
            mime_type = raw_exif_dict.get("File:MIMEType", "unknown/unknown") if raw_exif_dict else "unknown/unknown"

            google_json_dict = self._get_google_json_dict(filepath)

            results[filepath] = {
                "media_file": {
                    "file_hash": self._hash_file_content(filepath),  # Hashing must still be one-by-one
                    "mime_type": mime_type,
                    "file_size": raw_exif_dict.get("File:FileSize", 0) if raw_exif_dict else os.path.getsize(filepath),
                },
                "metadata": self._parse_master_metadata(raw_exif_dict),
                "google_metadata": self._parse_google_metadata(google_json_dict),
                "raw_exif": {"data": raw_exif_dict} if raw_exif_dict else None,
                "raw_google_json": {"data": google_json_dict} if google_json_dict else None
            }
        return results