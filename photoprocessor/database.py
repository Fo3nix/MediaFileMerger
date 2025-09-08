
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker, declarative_base

# Define the location of your SQLite database file
DATABASE_URL = "sqlite:///photos.db"

# The engine is the entry point to the database.
# `echo=True` is useful for debugging as it logs all generated SQL.
engine = create_engine(DATABASE_URL, echo=False)

# Optimize SQLite performance with PRAGMA settings on each connection.
@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Sets SQLite PRAGMA for performance on every new connection."""
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA synchronous = OFF")
    cursor.execute("PRAGMA journal_mode = MEMORY")
    # Set cache size to 1GB. The value is in KiB, so -1000000 = 1,000,000 KiB.
    # Adjust this based on your available system RAM.
    cursor.execute("PRAGMA cache_size = -1000000")
    cursor.close()

# A sessionmaker provides a factory for creating Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# This Base class will be inherited by all your ORM models.
Base = declarative_base()
