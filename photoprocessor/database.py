
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base

# Define the location of your SQLite database file
DATABASE_URL = "sqlite:///photos.db"

# The engine is the entry point to the database.
# `echo=True` is useful for debugging as it logs all generated SQL.
engine = create_engine(DATABASE_URL, echo=False)

# A sessionmaker provides a factory for creating Session objects.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# This Base class will be inherited by all your ORM models.
Base = declarative_base()
