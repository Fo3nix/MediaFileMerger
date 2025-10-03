import os
import json
from functools import lru_cache
from typing import List, Dict, Set
from collections import defaultdict


# Cache get_directory_contents for performance, so we only list a dir once.
@lru_cache(maxsize=256)
def get_directory_contents(directory_path: str) -> Set[str]:
    """
    Lists the contents of a directory and returns them as a set for fast lookups.
    The @lru_cache decorator automatically caches the results.
    """
    try:
        return set(os.listdir(directory_path))
    except (FileNotFoundError, NotADirectoryError):
        return set()


class GoogleJsonFinder:
    """
    Finds, parses, and caches Google Takeout JSON metadata for efficient lookup.

    This class builds a per-directory cache that maps the media filename
    (from the JSON 'title' field) to its corresponding metadata.
    """

    @lru_cache(maxsize=128)
    def _build_cache_for_dir(self, directory: str) -> Dict[str, List[Dict]]:
        """
        Scans a directory for .json files and builds a lookup map.
        The map's key is the 'title' from within the JSON (the media filename).
        The result of this method is cached.
        """
        cache: Dict[str, List[Dict]] = defaultdict(list)
        dir_contents = get_directory_contents(directory)
        if not dir_contents:
            return {}

        json_filenames = [f for f in dir_contents if f.lower().endswith('.json')]

        for filename in json_filenames:
            json_path = os.path.join(directory, filename)
            try:
                with open(json_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                # The 'title' field is the key to linking metadata to a media file.
                media_filename = data.get("title")

                # Ensure the title is a valid, non-empty string before using it.
                if isinstance(media_filename, str) and media_filename:
                    # Append the data. A media file can have multiple JSONs
                    # (e.g., photo.jpg.json and photo(1).json).
                    cache[media_filename].append(data)
            except (json.JSONDecodeError, OSError):
                # Ignore corrupted JSON files or files we can't read.
                continue

        return dict(cache)  # Convert back to a standard dict for the cache

    def get_metadata_for_file(self, media_path: str) -> List[Dict]:
        """
        Retrieves all Google JSON metadata for a given media file path.
        Returns a single merged dictionary, or None if no metadata is found.
        """
        directory = os.path.dirname(media_path)
        filename = os.path.basename(media_path)

        # Get the cached lookup map for the directory. This is the efficient part.
        dir_cache = self._build_cache_for_dir(directory)

        # Look up the filename in the directory's cache.
        json_data_list = dir_cache.get(filename)

        return json_data_list if json_data_list else []