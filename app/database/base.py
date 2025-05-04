# app/database/base.py

from sqlalchemy.ext.declarative import declarative_base

# Define the Base class for declarative models.
# All SQLAlchemy models in your application will inherit from this Base.
Base = declarative_base()

# You could also potentially define the MetaData object here if needed,
# but for now, just the Base is sufficient.
