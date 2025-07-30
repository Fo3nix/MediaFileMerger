from sqlalchemy import Column, Integer, String, REAL, DateTime, ForeignKey, JSON, Boolean
from sqlalchemy.orm import relationship
from photoprocessor.database import Base  # Import Base from your database module


# Many-to-many relationship table for tags
# Note: SQLAlchemy handles this directly, but defining it can be clearer
# For simplicity, we will define the main models first.

class MediaFile(Base):
    __tablename__ = 'media_files'

    id = Column(Integer, primary_key=True, index=True)
    file_hash = Column(String, unique=True, nullable=False, index=True)
    filename = Column(String, nullable=False)
    relative_path = Column(String)

    # Creates the one-to-one relationship to Metadata
    processed_metadata = relationship("Metadata", back_populates="media_file", uselist=False, cascade="all, delete-orphan")
    # Add the new one-to-one relationship
    google_metadata = relationship("GooglePhotosMetadata", uselist=False, cascade="all, delete-orphan")
    # Add these new relationships for raw data
    raw_exif = relationship("RawExif", uselist=False, cascade="all, delete-orphan")
    raw_google_json = relationship("RawGoogleJson", uselist=False, cascade="all, delete-orphan")



class Metadata(Base):
    __tablename__ = 'metadata'

    id = Column(Integer, primary_key=True)
    media_file_hash = Column(String, ForeignKey('media_files.file_hash'))
    title = Column(String)
    description = Column(String)
    date_taken = Column(DateTime)

    # New technical fields
    camera_make = Column(String)
    camera_model = Column(String)
    lens_model = Column(String)
    focal_length = Column(Integer)
    aperture = Column(REAL)
    shutter_speed = Column(REAL)
    iso = Column(Integer)

    # New location & rating fields
    gps_latitude = Column(REAL)
    gps_longitude = Column(REAL)
    city = Column(String)  # From reverse geocoding
    country = Column(String)  # From reverse geocoding
    rating = Column(Integer)  # e.g., 1-5
    is_favorite = Column(Boolean, default=False)

    # Common fields for both photo/video
    width = Column(Integer)
    height = Column(Integer)
    duration_seconds = Column(REAL)  # NULL for photos

    media_file = relationship("MediaFile", back_populates="processed_metadata")


class GooglePhotosMetadata(Base):
    __tablename__ = 'google_photos_metadata'

    # The hash is the primary key and links to the media_files table.
    media_file_hash = Column(String, ForeignKey('media_files.file_hash'), primary_key=True)

    # Key fields from Google's JSON
    title = Column(String)
    description = Column(String)
    creation_timestamp = Column(DateTime)
    modified_timestamp = Column(DateTime)
    google_url = Column(String)

    # GPS data as reported by Google
    gps_latitude = Column(REAL)
    gps_longitude = Column(REAL)

    # To track if the file was favorited in Google Photos
    is_favorited = Column(Boolean, default=False)

    media_file = relationship("MediaFile", back_populates="google_metadata")

class RawExif(Base):
    __tablename__ = 'raw_exif'

    # The hash is the primary key and links to the media_files table.
    media_file_hash = Column(String, ForeignKey('media_files.file_hash'), primary_key=True)

    # The JSON type is efficient for storing and querying JSON data.
    # It falls back to TEXT on backends that don't support it (like older SQLite).
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_exif")

class RawGoogleJson(Base):
    __tablename__ = 'raw_google_json'

    media_file_hash = Column(String, ForeignKey('media_files.file_hash'), primary_key=True)
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_google_json")