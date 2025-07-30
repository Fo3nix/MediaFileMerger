import sqlite3
import os


class PhotoDatabase:
    """Handles all database operations for the photo library."""

    def __init__(self, db_path="photos.db"):
        """Initializes the database connection and creates the table if it doesn't exist."""
        self.db_path = db_path
        self.conn = sqlite3.connect(self.db_path)
        self._setup_database()

    def _setup_database(self):
        """Sets up the SQLite database schema and indices."""
        with self.conn:
            self.conn.execute("""
            CREATE TABLE IF NOT EXISTS photos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                filename TEXT NOT NULL,
                local_file_location TEXT NOT NULL UNIQUE,
                image_hash TEXT NOT NULL,
                metadata_hash TEXT,
                actual_metadata TEXT,
                old_metadata TEXT,
                google_takeout_metadata TEXT
            );
            """)
            self.conn.execute("CREATE INDEX IF NOT EXISTS idx_image_hash ON photos (image_hash);")

    def is_file_processed(self, file_path):
        """Checks if a file has already been added to the database."""
        cursor = self.conn.cursor()
        cursor.execute("SELECT id FROM photos WHERE local_file_location = ?", (file_path,))
        return cursor.fetchone() is not None

    def insert_photo(self, photo_data):
        """
        Inserts a single photo's data into the database.

        Args:
            photo_data (dict): A dictionary containing all photo attributes.
        """
        with self.conn:
            self.conn.execute("""
            INSERT INTO photos (
                filename, local_file_location, image_hash, metadata_hash,
                actual_metadata, old_metadata, google_takeout_metadata
            ) VALUES (:filename, :local_file_location, :image_hash, :metadata_hash,
                      :actual_metadata, :old_metadata, :google_takeout_metadata)
            """, photo_data)

    def close(self):
        """Closes the database connection."""
        if self.conn:
            self.conn.close()