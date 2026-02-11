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
import tempfile
import rawpy

from photoprocessor.google_json_finder import GoogleJsonFinder

import imagehash
import cv2
import hashlib
from PIL import Image
import re

from pillow_heif import register_heif_opener
register_heif_opener()

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

def _cryptographic_raw_hash(image_path: str) -> str | None:
    """
    Generates a SHA-256 hash of the pixel data from a RAW image file (.dng, etc.).
    This function uses rawpy to decode the image first.
    """
    try:
        with rawpy.imread(image_path) as raw:
            # postprocess() creates a standard 8-bit RGB image as a NumPy array
            rgb_pixels = raw.postprocess(half_size=True, use_camera_wb=True, no_auto_bright=True)

        # Convert the resulting NumPy array to bytes and hash it
        return hashlib.sha256(rgb_pixels.tobytes()).hexdigest()
    except Exception as e:
        print(f"Warning: Could not generate cryptographic hash for RAW file {image_path}. Error: {e}")
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

def _extract_whatsapp_signature(filename: str) -> str | None:
    """
    Extracts the 'IMG-YYYYMMDD-WAxxxx' signature from a filename.
    Ignores appended copy numbers like '(1)'.
    """
    # Regex matches standard WA format: prefix (IMG/VID/PTT) - 8 digits - WA - digits
    match = re.search(r'((?:IMG|VID|AUD|PTT)-\d{8}-WA\d+)', filename, re.IGNORECASE)
    if match:
        # return "YYYYMMDD-WAxxxx"
        return match.group(1).split('-', 1)[1]  # Split off the prefix (IMG/VID/AUD/PTT)

    return None

class PhotoProcessor:
    """Processes a single photo file to extract and structure data for the database."""

    def __init__(self):
        """Initializes the PhotoProcessor and its tools."""
        self.tf = TimezoneFinder()
        self.json_finder = GoogleJsonFinder()

    def _get_exiftool_batch_dict(self, filepaths: list[str]) -> tuple[list[dict], list[dict]]:
        """
        Extracts metadata from a BATCH of files using a single exiftool call.
        """
        required_tags = [
            "-MIMEType",
            "-FileSize",
            "-GPSLatitude",
            "-GPSLongitude",
            "-time:all"
        ]

        args = [
            # "-charset", "FileName=UTF8",
            # "-api", "QuickTimeUTC",  # Turns QuickTime dates into my local timezone !!NOT WANTED!!
            "-d", "%Y-%m-%dT%H:%M:%S%:z",  # This format is correct
            "-G", "-n", "-json", "-a",
            *required_tags,
        ]

        final_args = [*args, *filepaths]

        argfile_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w+', delete=False, encoding='utf-8', suffix=".txt") as argfile:
                argfile.write("\n".join(final_args))
                argfile_path = argfile.name

            final_command = ["exiftool", "-@", argfile_path]
            result = subprocess.run(final_command, check=True, capture_output=True, text=True)
            return json.loads(result.stdout), []  # Success, no failures

        except FileNotFoundError:
            print("ERROR: exiftool command not found. Please install it and ensure it's in your PATH.")
            raise

        except (subprocess.CalledProcessError, json.JSONDecodeError) as e:
            # --- Stage 2: Fallback to individual processing ---
            print(f"Warning: Exiftool batch failed. Falling back to individual processing. Error: {e}")

            results = []
            failures = []
            individual_args_base = [
                "exiftool",
                *args
            ]

            for path in filepaths:
                try:
                    final_individual_args = individual_args_base + [path]
                    result = subprocess.run(final_individual_args, check=True, capture_output=True, text=True)
                    data = json.loads(result.stdout)
                    results.append(data[0] if data else {})
                except (subprocess.CalledProcessError, json.JSONDecodeError) as individual_e:
                    stderr = getattr(individual_e, 'stderr', '').strip() or str(individual_e)
                    error_msg = f"Exiftool individual processing failed. Error: {stderr}"
                    print(f"  - Failed to process: {os.path.basename(path)}. {error_msg}")
                    # Add a placeholder to results so indices match, and log the failure.
                    results.append({"SourceFile": os.path.abspath(path)})
                    failures.append({"path": path, "error": error_msg})

            return results, failures

        finally:
            if argfile_path and os.path.exists(argfile_path):
                os.remove(argfile_path)

    def _to_datetime(self, date_str: str, default_timezone: timezone|None) -> datetime | None:
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

    def _get_metadata_entries_from_dict(self, data_dict: dict, keys: list, type_name: str) -> list[dict]:
        # type needs to be one of: str, real, dt
        entries = []
        for key in keys:
            if key in data_dict:
                value = data_dict[key]
                entry = {"key": key}
                if type_name == "str" and isinstance(value, str):
                    entry["value_str"] = value
                elif type_name == "real" and isinstance(value, (float, int)):
                    entry["value_real"] = float(value)
                elif type_name == "dt" and isinstance(value, str):
                    dt_value = self._to_datetime(value, default_timezone=None)
                    if dt_value:
                        entry["value_dt"] = dt_value
                    else:
                        continue
                else:
                    continue
                entries.append(entry)

        return entries

    def _parse_key_exif_fields(self, raw_exif):
        """Extracts key fields into a list of EAV dictionaries."""
        if not raw_exif:
            return []

        entries = []

        source_file_path = raw_exif.get("SourceFile")
        if source_file_path:
            entries.append({
                "key": "exiftool:SourceFile",
                "value_str": os.path.basename(source_file_path)
            })

        # GET DATES
        date_taken_entries = self._get_metadata_entries_from_dict(
            raw_exif,
            keys=[
                "XMP:DateTimeOriginal",
                "XMP-exif:DateTimeOriginal",
                "EXIF:DateTimeOriginal",
                "QuickTime:CreationDate",
                "QuickTime:CreateDate",
                "Composite:GPSDateTime",
                "Keys:CreationDate",
                "UserData:DateTimeOriginal",
                "XMP:CreateDate",
                "XMP-xmp:CreateDate",
                "EXIF:CreateDate",
            ],
            type_name="dt",
        )
        entries.extend(date_taken_entries)

        date_offset_entries = self._get_metadata_entries_from_dict(
            raw_exif,
            keys=["EXIF:OffsetTimeOriginal", "XMP:OffsetTime"],
            type_name="str"
        )
        entries.extend(date_offset_entries)

        date_modified_entries = self._get_metadata_entries_from_dict(
            raw_exif,
            keys=[
                "XMP:ModifyDate",
                "QuickTime:ModifyDate",
                "EXIF:ModifyDate",
                "File:FileModifyDate"
            ],
            type_name="dt",
        )
        entries.extend(date_modified_entries)

        # GET GPS
        gps_entries = self._get_metadata_entries_from_dict(
            raw_exif,
            keys=["Composite:GPSLatitude","Composite:GPSLongitude"],
            type_name="real",
        )
        entries.extend(gps_entries)


        return entries

    def _parse_key_google_fields(self, google_json):
        """Extracts key fields from Google JSON into a list of EAV dictionaries."""
        if not google_json:
            return []

        entries = []
        creation_time = google_json.get("photoTakenTime", {}).get("timestamp")
        latitude = google_json.get("geoData", {}).get("latitude")
        longitude = google_json.get("geoData", {}).get("longitude")
        title = google_json.get("title")

        if title:
            entries.append({"key": "google:title", "value_str": title})

        if creation_time:
            utc_date = datetime.fromtimestamp(int(creation_time), tz=timezone.utc)
            entries.append({"key": "google:photoTakenTime", "value_dt": utc_date})

        if _validate_gps(latitude, longitude):
            entries.append({"key": "google:geoDataLatitude", "value_real": latitude})
            entries.append({"key": "google:geoDataLongitude", "value_real": longitude})

        return entries

    def process_batch(self, filepaths: list[str]) -> tuple[dict, list]:
        """
        Processes a batch of files using an internal producer-consumer pipeline
        to overlap I/O (file reading) and CPU (hashing) work.
        """
        if not filepaths:
            return {}, []

        # --- Step 1: Batched I/O for metadata (still the most efficient way) ---
        all_raw_exif, exif_failures = self._get_exiftool_batch_dict(filepaths)
        exif_map = {os.path.abspath(d.get('SourceFile')): d for d in all_raw_exif}

        successes = {}
        failures = []
        failures.extend(exif_failures)
        failed_paths = {os.path.abspath(f['path']) for f in failures}

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
                    google_json_list = self.json_finder.get_metadata_for_file(path)

                    mime_type = raw_exif_dict.get("File:MIMEType",
                                                  "unknown/unknown") if raw_exif_dict else "unknown/unknown"

                    # Pre-load the image data from disk (I/O-bound work)
                    image_obj = None
                    # Only try to open with Pillow if it's a compatible image
                    if mime_type.startswith("image/") and not path.lower().endswith('.dng'):
                        with Image.open(path) as img:
                            img.thumbnail((256, 256))
                            image_obj = img.copy()

                    # Put all necessary data into the queue for the consumer
                    image_queue.put({
                        "path": path,
                        "image_obj": image_obj,
                        "raw_exif_dict": raw_exif_dict,
                        "google_json_list": google_json_list,
                        "mime_type": mime_type
                    })
                except Exception as e:
                    image_queue.put({"path": path, "error": e})

            # Submit all I/O-bound producer jobs to the thread pool
            for path in filepaths:
                if os.path.abspath(path) in failed_paths:
                    continue
                io_executor.submit(producer, path)

            # --- The Consumer's job: Hash the pre-loaded data (CPU-bound) ---
            num_to_consume = len(filepaths) - len(failed_paths)
            for _ in range(num_to_consume):
                data = image_queue.get()
                path = data["path"]

                try:
                    if "error" in data:
                        raise data["error"]

                    mime_type = data["mime_type"]
                    file_hash = None

                    if path.lower().endswith('.dng'):
                        # Use our new rawpy-based hasher for DNG files
                        file_hash = _cryptographic_raw_hash(path)
                    elif mime_type.startswith("image/") and data["image_obj"]:
                        # Hashing is CPU-bound and works on the pre-loaded image_obj
                        file_hash = _cryptographic_image_hash(data["image_obj"])
                    elif mime_type.startswith("video/"):
                        # Video hashing is a separate process, can be done directly
                        file_hash = _strict_video_hash(path)
                    else:
                        print("Hashing file partially due to unknown MIME type.")
                        file_hash = _hash_file_partially(path)

                    if not file_hash:
                        raise ValueError("Hashing failed")

                    # This splits identical files into separate MediaFiles
                    # IF AND ONLY IF their WhatsApp ID (date+number) is different.
                    filename = os.path.basename(path)
                    wa_sig = _extract_whatsapp_signature(filename)
                    if wa_sig:
                        file_hash = f"{file_hash}-{wa_sig}"

                    raw_exif_dict = data["raw_exif_dict"]
                    google_json_list = data["google_json_list"]

                    # Create List of dicts for google metadata
                    google_metadata_list = []
                    for google_json_dict in google_json_list:
                        if google_json_dict:  # Ensure dict is not None or empty
                            google_metadata_list.append({
                                "parsed": self._parse_key_google_fields(google_json_dict),
                                "raw": google_json_dict
                            })

                    successes[path] = {
                        "location_data": {
                            "file_size": raw_exif_dict.get("File:FileSize", 0) if raw_exif_dict else os.path.getsize(
                                path)
                        },
                        "media_file": {
                            "file_hash": file_hash,
                            "mime_type": mime_type,
                        },
                        "exif_metadata": {"parsed": self._parse_key_exif_fields(raw_exif_dict),
                                          "raw": raw_exif_dict} if raw_exif_dict else None,
                        "google_metadata_list": google_metadata_list
                    }
                except Exception as e:
                    failures.append({"path": path, "error": str(e)})

        return successes, failures