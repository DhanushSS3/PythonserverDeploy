# app/schemas/refresh_token.py

import datetime
from typing import Optional
from pydantic import BaseModel, Field

class RefreshToken(BaseModel):
    """
    Pydantic model for Refresh Token data stored in Redis.
    Defines the structure of the data associated with a refresh token.
    """
    # The actual refresh token string (you'll likely store a hash of this in Redis)
    # Using Optional[str] and default=None if the token itself isn't stored directly in the object
    # but used as the key. However, often the token ID/hash is the key, and this is the value.
    # Let's assume the token string itself (or its hash) is the key in Redis,
    # and this model represents the value stored alongside it.
    # A common pattern is to store the user_id and expiry time.

    user_id: int = Field(..., description="The ID of the user this token belongs to.")
    expires_at: datetime.datetime = Field(..., description="The datetime when the token expires.")
    # You might also store other metadata if needed, e.g.,
    # issued_at: datetime.datetime = Field(default_factory=datetime.datetime.utcnow)
    # device_info: Optional[str] = None # Information about the device that issued the token

    # Configuration for Pydantic
    class Config:
        # This allows SQLAlchemy model instances to be converted to Pydantic models
        # Useful if you ever fetch related data from the DB and want to include it.
        # Although for RefreshToken in Redis, this might be less critical.
        # from_attributes = True # Use from_attributes instead of orm_mode in Pydantic v2+
        pass # For this simple model, Config is not strictly needed unless using from_attributes
