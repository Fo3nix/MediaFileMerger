# Multi-Source Photo Merging & Deduplication Processor

## Why this exists

I created this project to solve a specific, complex problem: merging multiple photo libraries (local hard drives, backups, and Google Photos Takeouts) into a single, clean collection without creating duplicates.

Existing tools often failed on specific edge cases that I needed to handle:
1.  **True Deduplication:** I didn't want duplicates based on filenames. I needed content-based deduplication (hashing pixel data) because the same photo often existed with different names (e.g., `IMG_1234.JPG` vs `IMG_1234(1).JPG`).
2.  **Metadata Integrity:** Google Takeout separates metadata into sidecar `.json` files. Merging these back into the images correctly—while handling timezone shifts and missing EXIF data—was critical.
3.  **Shared Albums & Multi-User Support:** My friend and I shared many Google Photos albums. When downloading these, you often lose context or get partial metadata. This tool processes files for specific "Owners" (users), allowing me to clean our collective libraries at once while tracking who the file originally belonged to, but still merging the best available metadata for the final output.

## Requirements

### System Dependencies
* **Python 3.12+**
* **ExifTool**: This project relies heavily on `exiftool` for reading and writing metadata. It must be installed and available in your system's PATH.
    * *MacOS*: `brew install exiftool`
    * *Debian/Ubuntu*: `sudo apt-get install libimage-exiftool-perl`
    * *Windows*: Download the executable and add it to your PATH.

### Python Dependencies
This project uses **Poetry** for dependency management.

1.  Install Poetry:
    ```bash
    curl -sSL https://install.python-poetry.org | python3 -
    ```
2.  Install project dependencies:
    ```bash
    poetry install
    ```

Key libraries used: `SQLAlchemy` (Database), `Pillow` & `rawpy` (Image processing), `opencv-python` (Video hashing), `timezonefinder` (GPS timezone lookup), `tqdm` (Progress bars).

## Project Structure & File Descriptions

### Core Pipelines
* **`photoprocessor/import_pipe.py`**: The entry point for **importing** media. It scans directories, generates cryptographic hashes of media content, reads raw metadata (Exif + Google JSON), and stores everything in the database.
* **`photoprocessor/export_pipe.py`**: The entry point for **exporting** the final library. It pulls data from the database, runs the merge logic, generates the final directory structure, and writes metadata to the files using ExifTool.

### Logic & Processing
* **`photoprocessor/processor.py`**: Handles the heavy lifting for individual files. It performs pixel-level hashing (ignoring metadata differences), detects perceptual hashes for videos, and pairs media files with their Google JSON sidecars.
* **`photoprocessor/merger.py`**: The "brain" of the operation. It contains the logic to resolve conflicts between different metadata sources (e.g., if the JSON says 2 PM but the Exif says 3 PM). It handles Timezone inference and GPS coordination.
* **`photoprocessor/merge_rules.py`**: Defines tolerance rules (e.g., how close two GPS coordinates must be to be considered the same location).
* **`photoprocessor/export_arguments.py`**:  Helper classes to structure the arguments passed to ExifTool during export.

### Data & storage
* **`photoprocessor/database.py`**: Sets up the SQLite database (`photos.db`) connection.
* **`photoprocessor/models.py`**: Defines the database schema.
    * `MediaFile`: Represents the unique content (hash).
    * `Location`: Represents a specific file path on disk (linked to a MediaFile).
    * `Owner`: Represents the user who provided the file.
    * `MetadataSource` & `MetadataEntry`: Stores raw EAV (Entity-Attribute-Value) metadata.
* **`photoprocessor/google_json_finder.py`**: Efficiently caches directory contents to find matching `.json` files for images without hitting the disk repeatedly.

### Utilities
* `photoprocessor/_find_filename_formats.py`, `_check_missing_files.py`, etc.: Various helper scripts used for debugging specific dataset issues or validating imports.

## Workflow

### 1. Database Initialization
The database (`photos.db`) is automatically created the first time you run an import.

### 2. Importing Media
Run the import pipe for a specific user (Owner). This scans the files, calculates hashes, and saves raw metadata to the DB. It does **not** modify files.

**Command:**
```bash
poetry run python -m photoprocessor.import_pipe "User Name" --directory "/path/to/google_takeout_folder"
```

* **Deduplication happens here:** If "User Name" imports a file that is already in the database (same pixel hash), the database links the new file path to the existing `MediaFile` entry. This allows us to know that "User A" and "User B" both have the same photo, without storing the data twice.

### 3. Exporting & Merging
Run the export pipe to generate the final library. This reads the database, merges metadata from all available sources for a file, and exports it to a clean structure.

**Command:**
```bash
poetry run python -m photoprocessor.export_pipe "/path/to/output_folder" --owner "User Name"
```

* **Conflict Resolution:** If the script encounters unresolvable metadata conflicts (e.g., two sources claim completely different dates and no GPS data exists to verify), it exports the file to a `conflicted_files_for_review` subfolder and logs the error.

## Logic Deep Dive: Rules & Edge Cases

### Deduplication Strategy
Deduplication is based on **Cryptographic Hashing (SHA-256)** of the *image data only*.
* We decode the image (using `Pillow` or `rawpy`) to pixels and hash that.
* This means `IMG_001.JPG` and `Copy of IMG_001.JPG` are treated as the same object, even if their file modification dates differ.

### Metadata Merging (`merger.py`)
The pipeline runs a series of "Steps" to generate the final metadata:

1.  **GPS Merge:**
    * Prioritizes embedded EXIF GPS data.
    * Falls back to Google JSON `geoData`.
    * **Rule:** If both exist, they must be within ~44 meters of each other, or a conflict is flagged.

2.  **Date & Time Resolution (The Hard Part):**
    * The system distinguishes between **Aware** (Timezone included) and **Naive** (No timezone) timestamps.
    * **Step 1:** Consolidates all "UTC" sources (Google JSON is usually UTC).
    * **Step 2:** Looks at Naive sources (original EXIF often lacks timezone).
    * **Step 3:** Infers the Timezone.
        * If GPS data exists, it looks up the timezone for that lat/long.
        * It then checks if the UTC timestamp + that Timezone matches the Naive timestamp.
    * **Fallback:** If no metadata exists, it attempts to parse the date from the filename (e.g., `IMG_20200101_...`).

3.  **WhatsApp Handling:**
    * WhatsApp strips metadata. The processor uses filename regex (`IMG-YYYYMMDD-WAxxxx`) to restore the "Date Taken".
    * **Consistency Check:** It checks if multiple files with the same WhatsApp ID sequence are being merged. If inconsistent dates are found for the same ID, it flags a conflict (suggesting two different WhatsApp exports were merged).

### Export Directory Structure
The export pipe determines the final folder path based on priority:

1.  **User Suggestions:** If a specific export path was manually tagged in the DB.
2.  **WhatsApp Folders:** If the source file was in a WhatsApp folder (e.g., `WhatsApp Images/Sent`) or matches the WhatsApp naming pattern, it goes into a dedicated `Whatsapp Images` or `Whatsapp Video` folder.
3.  **Screenshots:** Detected via filename ("Screenshot_...") or path, moves to `Screenshots`.
4.  **Year-Based:** (Default) Files are sorted into folders by year (e.g., `2015/IMG_001.jpg`) based on the final resolved "Date Taken".

### Conflict Handling
Files that cannot be automatically resolved (e.g., ambiguity in timestamps greater than safety tolerances) are **not** put in the main album.
* They are copied to `export_dir/conflicted_files_for_review`.
* A log file `export_conflicts.log` details exactly why the merge failed (e.g., "Field 'gps_latitude': GPS coordinates from same source type not close enough").