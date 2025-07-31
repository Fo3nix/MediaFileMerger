from sqlalchemy import Column, Integer, String, REAL, DateTime, ForeignKey, JSON, Boolean, UniqueConstraint
from sqlalchemy.orm import relationship
from photoprocessor.database import Base  # Import Base from your database module


class MediaFile(Base):
    __tablename__ = 'media_files'
    __table_args__ = (
        # Ensures that you cannot have the same file path registered more than once.
        UniqueConstraint('base_path', 'relative_path', 'filename', name='uq_filepath'),
    )

    id = Column(Integer, primary_key=True, index=True)
    # The 'unique' constraint is removed to allow for duplicate images (e.g., in different albums/locations).
    file_hash = Column(String, nullable=False, index=True)
    filename = Column(String, nullable=False)
    relative_path = Column(String, nullable=False)  # Relative path from the base path
    base_path = Column(String, nullable=False)
    mime_type = Column(String, nullable=False)  # e.g., 'image/jpeg', 'video/mp4'
    file_size = Column(Integer, nullable=False)  # Size in bytes

    # Relationships remain the same, SQLAlchemy handles the join condition via the ForeignKey.
    processed_metadata = relationship("Metadata", back_populates="media_file", uselist=False,
                                      cascade="all, delete-orphan")
    google_metadata = relationship("GooglePhotosMetadata", uselist=False, cascade="all, delete-orphan")
    raw_exif = relationship("RawExif", uselist=False, cascade="all, delete-orphan")
    raw_google_json = relationship("RawGoogleJson", uselist=False, cascade="all, delete-orphan")


class Metadata(Base):
    __tablename__ = 'metadata'

    id = Column(Integer, primary_key=True)
    # Changed from file_hash to the integer primary key of MediaFile.
    # Added unique=True to enforce the one-to-one relationship.
    media_file_id = Column(Integer, ForeignKey('media_files.id'), unique=True, nullable=False)

    title = Column(String)
    description = Column(String)
    date_taken = Column(DateTime)
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

    # The media file's ID is now the primary key.
    media_file_id = Column(Integer, ForeignKey('media_files.id'), primary_key=True)
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_exif")


class RawGoogleJson(Base):
    __tablename__ = 'raw_google_json'

    # The media file's ID is now the primary key.
    media_file_id = Column(Integer, ForeignKey('media_files.id'), primary_key=True)
    data = Column(JSON, nullable=False)

    media_file = relationship("MediaFile", back_populates="raw_google_json")