from sqlalchemy import Column, Integer, String, REAL, DateTime, ForeignKey, JSON, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from photoprocessor.database import Base  # Import Base from your database module
from sqlalchemy.types import TypeDecorator
import datetime


## CUSTOM TYPES
class AwareDateTime(TypeDecorator):
    """
    A custom SQLAlchemy type to store timezone-aware datetime objects in SQLite.

    Stores as a TEXT column in ISO 8601 format with timezone.
    e.g., "2025-09-05T23:01:31+02:00"
    """
    impl = String  # The underlying database type is TEXT (String)
    cache_ok = True

    def process_bind_param(self, value, dialect):
        # This is called when sending data TO the database.
        if value is None:
            return None
        if not isinstance(value, datetime.datetime):
            raise TypeError("AwareDateTime column requires datetime objects")
        if value.tzinfo is None:
            raise ValueError("datetime object must be timezone-aware")

        # Convert the datetime object to a standard ISO 8601 string.
        return value.isoformat()

    def process_result_value(self, value, dialect):
        # This is called when receiving data FROM the database.
        if value is None:
            return None

        # Parse the ISO 8601 string back into a timezone-aware datetime object.
        return datetime.fromisoformat(value)

## DATABASE MODELS

class Owner(Base):
    """
    Represents an owner of a media file. Storing owners separately
    prevents data duplication (e.g., spelling a name differently)
    and normalizes the database structure.
    """
    __tablename__ = 'owners'

    id = Column(Integer, primary_key=True)
    name = Column(String(20), unique=True, nullable=False, index=True)

    # Links this owner to their entries in the MediaOwnership association table.
    ownership_records = relationship("MediaOwnership", back_populates="owner")


class MediaOwnership(Base):
    """
    This is an 'association object' that connects MediaFile and Owner.
    It creates the many-to-many relationship and stores additional data
    about that relationship—specifically, the 'location' of that file instance.
    """
    __tablename__ = 'media_ownership'
    __table_args__ = (
        # A specific file can only be owned by a specific person at a specific location once.
        UniqueConstraint('media_file_id', 'owner_id', 'location', name='uq_owner_file_location'),
    )

    id = Column(Integer, primary_key=True, index=True)
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False)
    owner_id = Column(Integer, ForeignKey('owners.id'), nullable=False)
    location = Column(String, nullable=False, index=True)  # File path for this instance
    filename = Column(String, nullable=False)

    # Relationships back to the parent tables
    media_file = relationship("MediaFile", back_populates="owners")
    owner = relationship("Owner", back_populates="ownership_records")

class MediaFile(Base):
    __tablename__ = 'media_files'
    # __table_args__ is removed as the file path constraint is no longer here.

    id = Column(Integer, primary_key=True, index=True)
    # MODIFIED: file_hash is now unique. This table now stores a single record
    # for each unique piece of media content.
    file_hash = Column(String, nullable=False, index=True, unique=True)
    # REMOVED: 'relative_path' and 'base_path' are now in the MediaOwnership table.
    mime_type = Column(String, nullable=False)
    file_size = Column(Integer, nullable=False)

    # ADDED: Relationship to the MediaOwnership association table.
    owners = relationship("MediaOwnership", back_populates="media_file", cascade="all, delete-orphan")

    # Unchanged Relationships
    processed_metadata = relationship("Metadata", back_populates="media_file", uselist=False,
                                      cascade="all, delete-orphan")
    google_metadata = relationship("GooglePhotosMetadata", uselist=False, cascade="all, delete-orphan")
    raw_exif = relationship("RawExif", cascade="all, delete-orphan")
    raw_google_json = relationship("RawGoogleJson", cascade="all, delete-orphan")

class Metadata(Base):
    __tablename__ = 'metadata'

    id = Column(Integer, primary_key=True)
    # Changed from file_hash to the integer primary key of MediaFile.
    # Added unique=True to enforce the one-to-one relationship.
    media_file_id = Column(Integer, ForeignKey('media_files.id'), unique=True, nullable=False)

    title = Column(String)
    description = Column(String)
    date_taken = Column(AwareDateTime)
    camera_make = Column(String)
    camera_model = Column(String)
    lens_model = Column(String)
    focal_length = Column(Integer)
    aperture = Column(REAL)
    shutter_speed = Column(REAL)
    iso = Column(Integer)
    gps_latitude = Column(REAL)
    gps_longitude = Column(REAL)
    city = Column(String)
    country = Column(String)
    rating = Column(Integer)
    is_favorite = Column(Boolean, default=False)
    width = Column(Integer)
    height = Column(Integer)
    duration_seconds = Column(REAL)

    media_file = relationship("MediaFile", back_populates="processed_metadata")


class GooglePhotosMetadata(Base):
    __tablename__ = 'google_photos_metadata'

    # The media file's ID is now the primary key, linking directly to media_files.id.
    media_file_id = Column(Integer, ForeignKey('media_files.id'), primary_key=True)

    title = Column(String)
    description = Column(String)
    creation_timestamp = Column(DateTime)
    modified_timestamp = Column(DateTime)
    google_url = Column(String)
    gps_latitude = Column(REAL)
    gps_longitude = Column(REAL)
    is_favorited = Column(Boolean, default=False)

    media_file = relationship("MediaFile", back_populates="google_metadata")


class RawExif(Base):
    __tablename__ = 'raw_exif'

    id = Column(Integer, primary_key=True)
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False, index=True)
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_exif")


class RawGoogleJson(Base):
    __tablename__ = 'raw_google_json'

    id = Column(Integer, primary_key=True)
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False, index=True)
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_google_json")