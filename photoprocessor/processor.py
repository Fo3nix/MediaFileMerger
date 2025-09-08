import hashlib
import subprocess
import json
import os
from datetime import datetime, timezone
import magic
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo


class PhotoProcessor:
    """Processes a single photo file to extract and structure data for the database."""

    def __init__(self):
        """Initializes the PhotoProcessor and its tools."""
        self.tf = TimezoneFinder()

    def _get_aware_datetime(self, dt_obj: datetime, lat: float, lon: float) -> datetime | None:
        """
        Takes a datetime object and ensures it is aware and in the correct
        local timezone based on coordinates.

        - If the object is naive, it is localized to the target timezone.
        - If the object is already aware (e.g., in UTC), it is converted to the target timezone.
        """
        # If we don't have a datetime or coordinates, we can't proceed.
        if not dt_obj or lat is None or lon is None:
            if dt_obj.tzinfo is None:
                return None

            return dt_obj

        # Find the target timezone name from the coordinates
        tz_name = self.tf.timezone_at(lng=lon, lat=lat)
        if not tz_name:
            if dt_obj.tzinfo is None:
                return None
            return dt_obj  # Return original if no timezone is found

        target_tz = ZoneInfo(tz_name)

        if dt_obj.tzinfo is None:
            # The datetime is NAIVE. Stamp it with the local timezone.
            return dt_obj.replace(tzinfo=target_tz)
        else:
            # The datetime is AWARE. Convert it to the local timezone.
            return dt_obj.astimezone(target_tz)

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
            args = [
                "exiftool",
                "-api", "QuickTimeUTC",  # ADDED: Essential for correct video time conversion
                "-d", "%Y-%m-%dT%H:%M:%S%:z",  # This format is correct
                "-G", "-n", "-json",
                *filepaths
            ]
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

    def _to_datetime(self, date_str: str) -> datetime | None:
        """
        Safely converts a string to a datetime object.
        It returns a naive object, which will be made aware later if possible.
        """
        if not date_str or not isinstance(date_str, str):
            return None
        try:
            iso_str = date_str.replace(" ", "T")
            if iso_str.count(':') > 2:
                iso_str = iso_str.replace(':', '-', 2)
            # This will return an AWARE object if offset is in the string,
            # or a NAIVE object if it's not.
            return datetime.fromisoformat(iso_str)
        except (ValueError, TypeError):
            print(f"Warning: Could not parse date string: '{date_str}'")
            return None

    def _get_optional(self, data_dict, keys):
        """Helper function to get in-order keys from a dictionary, or combinations of keys.
            keys is a list of str or tuple of str.
        """
        for key in keys:
            if isinstance(key, str):
                if key in data_dict:
                    return data_dict[key]
            elif isinstance(key, tuple):
                # Check if all keys in the tuple exist
                if all(k in data_dict for k in key):
                    # Return a combined value (e.g., date + offset)
                    return ''.join(str(data_dict[k]) for k in key)
        return None

    def _parse_key_exif_fields(self, raw_exif):
        """Extracts just the key fields needed for the unified Metadata table."""
        if not raw_exif:
            return None

        date_taken = self._to_datetime(self._get_optional(raw_exif, [
                ("EXIF:DateTimeOriginal", "EXIF:OffsetTimeOriginal"),
                "EXIF:DateTimeOriginal", "XMP:DateTimeOriginal", "QuickTime:CreateDate", "EXIF:CreateDate", "File:FileModifyDate"
            ]))
        gps_latitude = self._get_optional(raw_exif, ["EXIF:GPSLatitude", "Composite:GPSLatitude"])
        gps_longitude = self._get_optional(raw_exif, ["EXIF:GPSLongitude", "Composite:GPSLongitude"])

        date_taken = self._get_aware_datetime(date_taken, gps_latitude, gps_longitude)

        return {
            "date_taken": date_taken,
            "gps_latitude": gps_latitude,
            "gps_longitude": gps_longitude,
        }

    def _parse_key_google_fields(self, google_json):
        """
        Extracts key fields from Google JSON and converts the UTC timestamp
        to the photo's local time.
        """
        if not google_json:
            return None

        # First, extract the raw values
        creation_time = google_json.get("photoTakenTime", {}).get("timestamp")
        latitude = google_json.get("geoData", {}).get("latitude")
        longitude = google_json.get("geoData", {}).get("longitude")

        # Create a timezone-aware datetime object in UTC
        utc_date = datetime.fromtimestamp(int(creation_time), tz=timezone.utc) if creation_time else None

        # Now, use the helper to convert the UTC date to the photo's local timezone
        local_date = self._get_aware_datetime(utc_date, latitude, longitude)

        return {
            "date_taken": local_date,
            "gps_latitude": latitude,
            "gps_longitude": longitude,
        }

    def process_batch(self, filepaths: list[str]) -> dict[str, dict]:
        """
        Processes a BATCH of files and returns a dictionary mapping
        filepath -> structured data.
        """
        results = {}
        all_raw_exif = self._get_exiftool_batch_dict(filepaths)
        exif_map = {os.path.abspath(d.get('SourceFile')): d for d in all_raw_exif}

        for filepath in filepaths:
            if not os.path.exists(filepath):
                continue

            raw_exif_dict = exif_map.get(filepath)
            google_json_dict = self._get_google_json_dict(filepath)

            # The new structure separates data by its source
            results[filepath] = {
                "media_file": {
                    "file_hash": self._hash_file_content(filepath),
                    "mime_type": raw_exif_dict.get("File:MIMEType", "unknown/unknown") if raw_exif_dict else "unknown/unknown",
                    "file_size": raw_exif_dict.get("File:FileSize", 0) if raw_exif_dict else os.path.getsize(filepath),
                },
                "exif_metadata": {
                    "parsed": self._parse_key_exif_fields(raw_exif_dict),
                    "raw": raw_exif_dict
                } if raw_exif_dict else None,
                "google_metadata": {
                    "parsed": self._parse_key_google_fields(google_json_dict),
                    "raw": google_json_dict
                } if google_json_dict else None
            }
        return results