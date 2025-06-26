from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional
import datetime

class MoneyRequestBase(BaseModel):
    amount: Decimal = Field(..., gt=0)
    type: str = Field(..., pattern="^(deposit|withdraw|withdrawal)$")

class MoneyRequestCreate(MoneyRequestBase):
    """
    Schema for creating a new money request by a user.
    user_id will be derived from the authenticated user.
    """
    pass

class MoneyRequestUpdateStatus(BaseModel):
    """
    Schema for an admin to update the status of a money request.
    """
    status: int = Field(..., ge=0, le=2, description="New status: 0 (requested), 1 (approved), 2 (rejected)")

class MoneyRequestResponse(MoneyRequestBase):
    """
    Schema for returning money request details.
    """
    id: int
    user_id: int
    status: int
    created_at: datetime.datetime
    updated_at: datetime.datetime

    class Config:
        orm_mode = True  # For Pydantic V1
        # from_attributes = True  # Uncomment for Pydantic V2+
