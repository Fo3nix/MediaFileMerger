import hashlib
import subprocess
import json
import os
from datetime import datetime, timezone
import magic
from timezonefinder import TimezoneFinder
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from queue import Queue, Empty
from functools import lru_cache

import imagehash
import cv2
import hashlib
from PIL import Image
import re

from pillow_heif import register_heif_opener
register_heif_opener()

@lru_cache(maxsize=256)
def get_directory_contents(directory_path: str) -> set:
    """
    Lists the contents of a directory and returns them as a set for fast lookups.
    The @lru_cache decorator automatically caches the results, so os.listdir()
    is only called once for each unique directory.
    """
    try:
        return set(os.listdir(directory_path))
    except (FileNotFoundError, NotADirectoryError):
        return set()

def _validate_gps(lat, lon) -> bool:
    """Validates GPS coordinates."""
    if lat is None or lon is None:
        return False

    try:
        # Attempt to convert to float, in case they are strings
        lat_f = float(lat)
        lon_f = float(lon)
    except (ValueError, TypeError):
        # If conversion fails, the coordinates are invalid
        return False

    if abs(lat) < 1e-6 and abs(lon) < 1e-6:
        return False
    if not (-90 <= lat <= 90) or not (-180 <= lon <= 180):
        return False
    return True

def _cryptographic_image_hash(img_obj: Image.Image) -> str | None:
    """
    Generates a SHA-256 hash of the raw pixel data of an image.
    This hash is extremely strict: only pixel-for-pixel identical images
    will produce the same hash.
    """
    if not img_obj:
        return None
    try:
        # Convert the image to its raw pixel data (bytes)
        pixel_data = img_obj.tobytes()
        # Create a SHA-256 hash of the pixel data
        return hashlib.sha256(pixel_data).hexdigest()
    except Exception as e:
        print(f"Warning: Could not generate cryptographic hash. Error: {e}")
        return None

def _perceptual_image_hash(image_path: str) -> str | None:
    """
    Generates a perceptual hash for an image file using the imagehash library.
    This function is optimized for speed and should handle most common image formats.
    """
    try:
        with Image.open(image_path) as img:
            # create a thumbnail for faster processing
            img.thumbnail((512, 512))
            # Use average hash (aHash) for speed; other options include phash, dhash, whash
            hash_value = imagehash.phash(img)
            return str(hash_value)
    except (FileNotFoundError, OSError):
        return None

def _strict_video_hash(video_path: str, num_frames=10) -> str | None:
    """
    Generates a strict visual hash for a video file by cryptographically
    hashing the pixel data of several evenly-sampled frames.
    """
    cap = None
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames <= 0:
            return None
        if total_frames < num_frames:
            num_frames = total_frames

        frame_hashes = []
        # Sample frames evenly across the video
        sample_indices = [int(i * (total_frames - 1) / (num_frames - 1)) for i in range(num_frames)] if num_frames > 1 else [0]

        for frame_idx in sample_indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if ret:
                # Convert frame to a Pillow Image object
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = Image.fromarray(rgb_frame)

                # Generate a cryptographic hash for the frame's pixel data
                pixel_data = img.tobytes()
                frame_hash = hashlib.sha256(pixel_data).hexdigest()
                frame_hashes.append(frame_hash)

        if not frame_hashes:
            return None

        # Combine all frame hashes into a single string
        combined_hash_string = "".join(frame_hashes)

        # Create a final, fixed-length hash of the combined string
        final_hash = hashlib.sha256(combined_hash_string.encode()).hexdigest()
        return final_hash

    except Exception as e:
        print(f"Warning: Could not process video {video_path}. Error: {e}")
        return None
    finally:
        if cap:
            cap.release()

def _perceptual_video_hash(video_path: str, num_frames = 5) -> str | None:
    """
    Generates a perceptual hash for a video file using the videohash library.
    This function extracts keyframes and computes a hash based on them.
    """
    """
        Generates a visual hash for a video file by sampling frames.
        """
    cap = None
    try:
        cap = cv2.VideoCapture(video_path)
        if not cap.isOpened():
            return None

        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        if total_frames < num_frames:
            num_frames = total_frames

        frame_hashes = []
        # Calculate frame indices to sample evenly across the video
        sample_indices = [int(i * total_frames / num_frames) for i in range(num_frames)]

        for frame_idx in sample_indices:
            cap.set(cv2.CAP_PROP_POS_FRAMES, frame_idx)
            ret, frame = cap.read()
            if ret:
                # Convert frame from OpenCV's BGR format to Pillow's RGB format
                rgb_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                img = Image.fromarray(rgb_frame)

                # Generate a perceptual hash for the frame
                frame_hash = imagehash.phash(img)
                frame_hashes.append(str(frame_hash))

        if not frame_hashes:
            return None

        # Combine all frame hashes into a single string
        combined_hash_string = "".join(frame_hashes)

        # Create a final, fixed-length hash of the combined string
        final_hash = hashlib.sha256(combined_hash_string.encode()).hexdigest()
        return final_hash

    except Exception as e:
        print(f"Warning: Could not process video {video_path}. Error: {e}")
        return None
    finally:
        if cap:
            cap.release()

def _hash_file_partially(filepath: str, chunk_size=1024 * 1024) -> str:
    """
    Generates a hash from the first and last chunks of a file.
    This is dramatically faster for large files as it avoids reading the entire file.
    """
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            # Read the first chunk
            first_chunk = f.read(chunk_size)
            sha256_hash.update(first_chunk)

            # If the file is large enough, seek to the end and read the last chunk
            file_size = os.path.getsize(filepath)
            if file_size > chunk_size:
                f.seek(-chunk_size, os.SEEK_END)
                last_chunk = f.read(chunk_size)
                sha256_hash.update(last_chunk)

        return sha256_hash.hexdigest()
    except (FileNotFoundError, OSError):
        return ""


def _standalone_get_google_json_dict(image_path: str) -> dict | None:
    """
    Finds and reads all corresponding Google Takeout JSON metadata files
    using regex, merging them into a single dictionary.
    """
    directory = os.path.dirname(image_path)
    dir_contents = get_directory_contents(directory)
    if not dir_contents:
        return None

    # Get the filename without its extension
    base_filename = os.path.splitext(os.path.basename(image_path))[0]

    # Handle Google's "-edited" suffix by looking for the original name
    if "-edited" in base_filename:
        base_filename = base_filename.replace("-edited", "")

    # Create a regex pattern to match:
    # ^                  - Start of the string
    # (re.escape(...))   - The literal base filename
    # .* - Any characters (the "SOMETHING")
    # \.json$            - Ending with exactly ".json"
    pattern = re.compile(rf"^{re.escape(base_filename)}.*\.json$")

    # Find all matching files in the directory
    found_files = [f for f in dir_contents if pattern.match(f)]
    if not found_files:
        return None

    merged_data = {}
    for filename in found_files:
        json_path = os.path.join(directory, filename)
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                merged_data.update(json.load(f))
        except Exception:
            continue

    return merged_data if merged_data else None

class PhotoProcessor:
    """Processes a single photo file to extract and structure data for the database."""

    def __init__(self):
        """Initializes the PhotoProcessor and its tools."""
        self.tf = TimezoneFinder()

    def _get_exiftool_batch_dict(self, filepaths: list[str]) -> list[dict]:
        """
        Extracts metadata from a BATCH of files using a single exiftool call.
        """
        try:
            # Pass all filepaths to a single exiftool command.
            # It will return a list of JSON objects, one for each file.

            required_tags = [
                "-MIMEType",
                "-FileSize",
                "-GPSLatitude",
                "-GPSLongitude",
                "-time:all"
            ]

            args = [
                "exiftool",
                # "-api", "QuickTimeUTC",  # Turns QuickTime dates into my local timezone
                "-d", "%Y-%m-%dT%H:%M:%S%:z",  # This format is correct
                "-G", "-n", "-json", "-a",
                *required_tags,
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

    def _to_datetime(self, date_str: str, default_timezone: timezone) -> datetime | None:
        """
        Safely converts a string to a datetime object.
        It returns a naive object, which will be made aware later if possible.
        """
        if not date_str or not isinstance(date_str, str) or date_str == "0000:00:00 00:00:00":
            return None
        try:
            iso_str = date_str.replace(" ", "T")
            if iso_str.count(':') > 2:
                iso_str = iso_str.replace(':', '-', 2)
            # This will return an AWARE object if offset is in the string,
            # or a NAIVE object if it's not.
            dt_obj = datetime.fromisoformat(iso_str)

            if dt_obj.tzinfo is None and default_timezone is not None:
                dt_obj = dt_obj.replace(tzinfo=default_timezone)

            return dt_obj
        except (ValueError, TypeError):
            print(f"Warning: Could not parse date string: '{date_str}'")
            return None

    def _get_optional(self, data_dict, keys, return_chosen_key = False):
        """Helper function to get in-order keys from a dictionary, or combinations of keys.
            keys is a list of str or tuple of str.
        """

        def return_chosen(value, key):
            return (value, key) if return_chosen_key else value

        for key in keys:
            if isinstance(key, str):
                if key in data_dict:
                    return return_chosen(data_dict[key], key)
            elif isinstance(key, tuple):
                # Check if all keys in the tuple exist
                if all(k in data_dict for k in key):
                    # Return a combined value (e.g., date + offset)
                    val = ''.join(str(data_dict[k]) for k in key)
                    return return_chosen(val, key)
        return return_chosen(None, None)

    def _parse_key_exif_fields(self, raw_exif):
        """Extracts just the key fields needed for the unified Metadata table."""
        if not raw_exif:
            return None

        date_str, chosen_key = self._get_optional(raw_exif, [
            "XMP:DateTimeOriginal",
            ("EXIF:DateTimeOriginal", "EXIF:OffsetTimeOriginal"),
            "EXIF:DateTimeOriginal",
            "QuickTime:CreationDate",
            "QuickTime:CreateDate",
            "Composite:GPSDateTime",
            "Keys:CreationDate",
            "UserData:DateTimeOriginal",
            "XMP:CreateDate",
            "EXIF:CreateDate",
        ], return_chosen_key = True)
        default_timezone = None
        if chosen_key == "Composite:GPSDateTime" or chosen_key == "QuickTime:CreationDate" or chosen_key == "QuickTime:CreateDate":
            default_timezone = timezone.utc
        date_taken = self._to_datetime(date_str, default_timezone=default_timezone)
        date_taken_key = str(chosen_key)

        date_str, chosen_key = self._get_optional(raw_exif, [
            "XMP:ModifyDate",
            "QuickTime:ModifyDate",
            "EXIF:ModifyDate",
        ], return_chosen_key = True)
        default_timezone = timezone.utc if chosen_key == "QuickTime:ModifyDate" else None
        date_modified = self._to_datetime(date_str, default_timezone=default_timezone)
        date_modified_key = str(chosen_key)

        gps_latitude = self._get_optional(raw_exif, ["Composite:GPSLatitude"])
        gps_longitude = self._get_optional(raw_exif, ["Composite:GPSLongitude"])

        # validate gps
        if not _validate_gps(gps_latitude, gps_longitude):
            gps_latitude = None
            gps_longitude = None

        return {
            "date_taken": date_taken,
            "date_taken_key": date_taken_key,
            "date_modified": date_modified,
            "date_modified_key": date_modified_key,
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

        # validate gps
        if not _validate_gps(latitude, longitude):
            latitude = None
            longitude = None

        # Create a timezone-aware datetime object in UTC
        utc_date = datetime.fromtimestamp(int(creation_time), tz=timezone.utc) if creation_time else None

        return {
            "date_taken": utc_date,
            "gps_latitude": latitude,
            "gps_longitude": longitude,
        }

    def process_batch(self, filepaths: list[str]) -> tuple[dict, list]:
        """
        Processes a batch of files using an internal producer-consumer pipeline
        to overlap I/O (file reading) and CPU (hashing) work.
        """
        if not filepaths:
            return {}, []

        # --- Step 1: Batched I/O for metadata (still the most efficient way) ---
        all_raw_exif = self._get_exiftool_batch_dict(filepaths)
        exif_map = {os.path.abspath(d.get('SourceFile')): d for d in all_raw_exif}

        successes = {}
        failures = []

        # --- Step 2: Set up the producer-consumer pipeline ---
        # A small queue to buffer images read from disk, preventing high memory usage.
        image_queue = Queue(maxsize=os.cpu_count() or 4)

        # We use a ThreadPoolExecutor with a few workers for the I/O-bound tasks.
        with ThreadPoolExecutor(max_workers=4) as io_executor:

            # --- The Producer's job: Read files and get metadata ---
            def producer(path):
                try:
                    abs_path = os.path.abspath(path)
                    raw_exif_dict = exif_map.get(abs_path)
                    google_json_dict = _standalone_get_google_json_dict(path)

                    mime_type = raw_exif_dict.get("File:MIMEType",
                                                  "unknown/unknown") if raw_exif_dict else "unknown/unknown"

                    # Pre-load the image data from disk (I/O-bound work)
                    image_obj = None
                    if mime_type.startswith("image/"):
                        with Image.open(path) as img:
                            img.thumbnail((256, 256))
                            image_obj = img.copy()  # copy() is important to release file handle

                    # Put all necessary data into the queue for the consumer
                    image_queue.put({
                        "path": path,
                        "image_obj": image_obj,
                        "raw_exif_dict": raw_exif_dict,
                        "google_json_dict": google_json_dict,
                        "mime_type": mime_type
                    })
                except Exception as e:
                    image_queue.put({"path": path, "error": e})

            # Submit all I/O-bound producer jobs to the thread pool
            for path in filepaths:
                io_executor.submit(producer, path)

            # --- The Consumer's job: Hash the pre-loaded data (CPU-bound) ---
            for _ in range(len(filepaths)):
                data = image_queue.get()
                path = data["path"]

                try:
                    if "error" in data:
                        raise data["error"]

                    mime_type = data["mime_type"]
                    file_hash = None

                    if mime_type.startswith("image/"):
                        # Hashing is CPU-bound and works on the pre-loaded image_obj
                        file_hash = _cryptographic_image_hash(data["image_obj"])
                    elif mime_type.startswith("video/"):
                        # Video hashing is a separate process, can be done directly
                        file_hash = _strict_video_hash(path)
                    else:
                        file_hash = _hash_file_partially(path)

                    if not file_hash:
                        raise ValueError("Hashing failed")

                    raw_exif_dict = data["raw_exif_dict"]
                    google_json_dict = data["google_json_dict"]

                    successes[path] = {
                        "media_file": {
                            "file_hash": file_hash, "mime_type": mime_type,
                            "file_size": raw_exif_dict.get("File:FileSize", 0) if raw_exif_dict else os.path.getsize(
                                path),
                        },
                        "exif_metadata": {"parsed": self._parse_key_exif_fields(raw_exif_dict),
                                          "raw": raw_exif_dict} if raw_exif_dict else None,
                        "google_metadata": {"parsed": self._parse_key_google_fields(google_json_dict),
                                            "raw": google_json_dict} if google_json_dict else None
                    }
                except Exception as e:
                    failures.append({"path": path, "error": str(e)})

        return successes, failures