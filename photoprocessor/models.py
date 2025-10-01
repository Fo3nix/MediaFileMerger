from sqlalchemy import Column, Integer, String, REAL, DateTime, ForeignKey, JSON, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from photoprocessor.database import Base  # Import Base from your database module
from sqlalchemy.types import TypeDecorator
import datetime
from itertools import chain


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


class FlexibleDateTime(TypeDecorator):
    """
    A custom SQLAlchemy type that stores both timezone-aware and naive
    datetime objects.

    - Aware datetimes are stored as ISO 8601 strings with timezone.
      e.g., "2025-09-11T14:10:28+00:00"
    - Naive datetimes are stored as ISO 8601 strings without timezone.
      e.g., "2025-09-11T16:10:28"
    """
    impl = String
    cache_ok = True

    def process_bind_param(self, value, dialect):
        # This is called when sending data TO the database.
        if value is None:
            return None
        if not isinstance(value, datetime.datetime):
            raise TypeError("FlexibleDateTime column requires datetime objects")

        # This no longer raises an error if the datetime is naive
        return value.isoformat()

    def process_result_value(self, value, dialect):
        # This is called when receiving data FROM the database.
        if value is None:
            return None

        # This correctly parses both aware and naive ISO strings.
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
    file_size = Column(Integer, nullable=False)

    # A location is an instance of ONE media file's content
    media_file_id = Column(Integer, ForeignKey('media_files.id'), nullable=False)
    media_file = relationship("MediaFile", back_populates="locations")

    # A location can be owned by many people (though typically one)
    owners = relationship("MediaOwnership", back_populates="location")

    # A location can have multiple metadata entries from different sources
    metadata_sources = relationship("MetadataSource", back_populates="location", cascade="all, delete-orphan")


class MediaFile(Base):
    """
    Represents a unique piece of media content, identified by its hash.
    """
    __tablename__ = 'media_files'
    id = Column(Integer, primary_key=True, index=True)
    file_hash = Column(String, nullable=False, index=True, unique=True)
    mime_type = Column(String, nullable=False)

    # A single media file (hash) can exist at multiple locations
    locations = relationship("Location", back_populates="media_file", cascade="all, delete-orphan")

    @property
    def all_metadata_sources(self) -> list['MetadataEntry']:
        """
        Returns a flat list of all MetadataEntry objects associated with this
        media file from across all its locations and sources.
        """
        # This uses a generator expression for memory efficiency
        return list(chain.from_iterable(
            loc.metadata_sources for loc in self.locations
        ))


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


class MetadataSource(Base):
    """Stores the raw metadata blob from a single source (e.g., one exiftool run)."""
    __tablename__ = 'metadata_sources'
    __table_args__ = (
        UniqueConstraint('location_id', 'source', name='uq_location_source'),
    )
    id = Column(Integer, primary_key=True)
    location_id = Column(Integer, ForeignKey('locations.id'), nullable=False, index=True)
    source = Column(String, nullable=False)  # e.g., 'exif', 'google_json'
    raw_data = Column(JSON, nullable=False)

    location = relationship("Location")
    # A single source blob can contain many individual metadata entries
    entries = relationship("MetadataEntry", back_populates="source_info", cascade="all, delete-orphan")


class MetadataEntry(Base):
    """An EAV model for storing individual metadata points, linked to a raw source blob."""
    __tablename__ = 'metadata_entries'
    __table_args__ = (
        UniqueConstraint('source_id', 'key', name='uq_source_key'),
    )
    id = Column(Integer, primary_key=True)
    # Replaces media_file_id and location_id for a cleaner link
    source_id = Column(Integer, ForeignKey('metadata_sources.id'), nullable=False, index=True)

    key = Column(String, nullable=False, index=True)

    value_str = Column(String)
    value_dt = Column(FlexibleDateTime)
    value_real = Column(REAL)

    source_info = relationship("MetadataSource", back_populates="entries")
