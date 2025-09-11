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
        return datetime.datetime.fromisoformat(value)

## DATABASE MODELS
class Owner(Base):
    """Represents an owner. Links to locations via MediaOwnership."""
    __tablename__ = 'owners'
    id = Column(Integer, primary_key=True)
    name = Column(String(20), unique=True, nullable=False, index=True)

    # Relationship to the ownership association table
    locations = relationship("MediaOwnership", back_populates="owner")


class Location(Base):
    """
    NEW: Central table for file paths. Each path is unique.
    A location represents a single instance of a media file on disk.
    """
    __tablename__ = 'locations'
    id = Column(Integer, primary_key=True, index=True)
    path = Column(String, nullable=False, unique=True, index=True)
    filename = Column(String, nullable=False)

    # A location is an instance of ONE media file's content
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False)
    media_file = relationship("MediaFile", back_populates="locations")

    # A location can be owned by many people (though typically one)
    owners = relationship("MediaOwnership", back_populates="location")

    # A location can have multiple metadata entries from different sources
    metadata_entries = relationship("Metadata", back_populates="location", cascade="all, delete-orphan")


class MediaFile(Base):
    """
    Represents a unique piece of media content, identified by its hash.
    """
    __tablename__ = 'media_files'
    id = Column(Integer, primary_key=True, index=True)
    file_hash = Column(String, nullable=False, index=True, unique=True)
    mime_type = Column(String, nullable=False)
    file_size = Column(Integer, nullable=False)

    # A single media file (hash) can exist at multiple locations
    locations = relationship("Location", back_populates="media_file", cascade="all, delete-orphan")

    # A single media file can have multiple sources of metadata
    metadata_sources = relationship("Metadata", back_populates="media_file", cascade="all, delete-orphan")


class MediaOwnership(Base):
    """
    Association object linking an Owner to a Location.
    This signifies that a specific person owns the file at a specific path.
    """
    __tablename__ = 'media_ownership'
    __table_args__ = (
        UniqueConstraint('owner_id', 'location_id', name='uq_owner_location'),
    )
    id = Column(Integer, primary_key=True, index=True)
    owner_id = Column(Integer, ForeignKey('owners.id'), nullable=False)
    location_id = Column(Integer, ForeignKey('locations.id'), nullable=False)

    owner = relationship("Owner", back_populates="locations")
    location = relationship("Location", back_populates="owners")


class Metadata(Base):
    __tablename__ = 'metadata'
    # __table_args__ = (
    #     # A media file can only have one metadata entry per source (e.g., one 'exif').
    #     UniqueConstraint('media_file_id', 'source', name='uq_media_file_source'),
    # )
    id = Column(Integer, primary_key=True)
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False, index=True)
    location_id = Column(Integer, ForeignKey('locations.id'), nullable=False, index=True)

    source = Column(String, nullable=False)  # e.g., 'exif', 'google_json'

    # Key parsed fields for quick access and merging
    date_taken = Column(DateTime)
    date_modified = Column(DateTime)
    gps_latitude = Column(REAL)
    gps_longitude = Column(REAL)

    # The complete raw data from the source
    raw_data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="metadata_sources")
    location = relationship("Location", back_populates="metadata_entries")
