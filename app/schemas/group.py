# app/schemas/group.py

import datetime
from typing import Optional
from pydantic import BaseModel, Field
from decimal import Decimal # Import Decimal

# Schema for data received when creating a group
class GroupCreate(BaseModel):
    """
    Pydantic model for creating a new group, matching the existing Group model fields.
    All fields defined as NOT NULL in the model are required here for creation.
    """
    # String fields
    symbol: Optional[str] = Field(None, description="Symbol associated with the group (optional).")
    name: str = Field(..., description="Name of the group (required).") # Name is required

    # Integer types
    commision_type: int = Field(..., description="Type of commission (integer).") # Required
    commision_value_type: int = Field(..., description="Type of commission value (integer).") # Required
    type: int = Field(..., description="Group type (integer).") # Required

    pip_currency: Optional[str] = Field("USD", description="Currency for pip calculation (optional, defaults to USD).")

    # show_points is now an integer
    show_points: Optional[int] = Field(None, description="Show points setting (optional).")

    # Decimal fields (required as per model, with default values handled by the model)
    swap_buy: Decimal = Field(..., max_digits=10, decimal_places=5, description="Swap buy value.") # Required
    swap_sell: Decimal = Field(..., max_digits=10, decimal_places=5, description="Swap sell value.") # Required
    commision: Decimal = Field(..., max_digits=10, decimal_places=4, description="Commission value.") # Required
    margin: Decimal = Field(..., max_digits=10, decimal_places=4, description="Base margin value.") # Required
    spread: Decimal = Field(..., max_digits=10, decimal_places=4, description="Spread value.") # Required
    deviation: Decimal = Field(..., max_digits=10, decimal_places=4, description="Deviation value.") # Required
    min_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Minimum lot size.") # Required
    max_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Maximum lot size.") # Required
    pips: Decimal = Field(..., max_digits=10, decimal_places=4, description="Pips value.") # Required
    spread_pip: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Spread pip value (optional).")

    # --- NEW FIELDS ---
    sending_orders: Optional[str] = Field(None, description="Where orders are sent (e.g., 'Barclays', 'Rock').")
    book: Optional[str] = Field(None, description="Book type (e.g., 'A', 'B').")
    # --- END NEW FIELDS ---


    # created_at and updated_at are handled by the database model


# Schema for data received when updating a group
class GroupUpdate(BaseModel):
    """
    Pydantic model for updating an existing group, matching the existing Group model fields.
    All fields are optional for partial updates.
    """
    # String fields
    symbol: Optional[str] = Field(None, description="Symbol associated with the group (optional).")
    name: Optional[str] = Field(None, description="Name of the group.") # Name is optional for update
    # Note: The unique constraint on '(symbol, name)' in the model means you cannot update
    # the symbol/name combination to a value that already exists in another group.

    # Integer types
    commision_type: Optional[int] = Field(None, description="Type of commission (integer).")
    commision_value_type: Optional[int] = Field(None, description="Type of commission value (integer).")
    type: Optional[int] = Field(None, description="Group type (integer).")

    pip_currency: Optional[str] = Field(None, description="Currency for pip calculation (optional).")

    # show_points is now an integer
    show_points: Optional[int] = Field(None, description="Show points setting (optional).")

    # Decimal fields
    swap_buy: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Swap buy value.")
    swap_sell: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Swap sell value.")
    commision: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Commission value.")
    margin: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Base margin value.")
    spread: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Spread value.")
    deviation: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Deviation value.")
    min_lot: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Minimum lot size.")
    max_lot: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Maximum lot size.")
    pips: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Pips value.")
    spread_pip: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Spread pip value (optional).")

    # --- NEW FIELDS ---
    sending_orders: Optional[str] = Field(None, description="Where orders are sent (e.g., 'Barclays', 'Rock').")
    book: Optional[str] = Field(None, description="Book type (e.g., 'A', 'B').")
    # --- END NEW FIELDS ---


# Schema for data returned after creating/fetching a group
# class GroupResponse(BaseModel):
#     """
#     Pydantic model for group response data, matching the existing Group model fields.
#     """
#     id: int = Field(..., description="Unique identifier of the group.")

#     # String fields
#     symbol: Optional[str] = Field(None, description="Symbol associated with the group (optional).")
#     name: str = Field(..., description="Name of the group.")

#     # Integer types
#     commision_type: int = Field(..., description="Type of commission (integer).")
#     commision_value_type: int = Field(..., description="Type of commission value (integer).")
#     type: int = Field(..., description="Group type (integer).")

#     pip_currency: Optional[str] = Field(None, description="Currency for pip calculation (optional).")

#     # show_points is now an integer
#     show_points: Optional[int] = Field(None, description="Show points setting (optional).")

#     # Decimal fields
#     swap_buy: Decimal = Field(..., max_digits=10, decimal_places=4, description="Swap buy value.")
#     swap_sell: Decimal = Field(..., max_digits=10, decimal_places=4, description="Swap sell value.")
#     commision: Decimal = Field(..., max_digits=10, decimal_places=4, description="Commission value.")
#     margin: Decimal = Field(..., max_digits=10, decimal_places=4, description="Base margin value.")
#     spread: Decimal = Field(..., max_digits=10, decimal_places=4, description="Spread value.")
#     deviation: Decimal = Field(..., max_digits=10, decimal_places=4, description="Deviation value.")
#     min_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Minimum lot size.")
#     max_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Maximum lot size.")
#     pips: Decimal = Field(..., max_digits=10, decimal_places=4, description="Pips value.")
#     spread_pip: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Spread pip value (optional).")

#     # --- NEW FIELDS ---
#     sending_orders: Optional[str] = Field(None, description="Where orders are sent (e.g., 'Barclays', 'Rock').")
#     book: Optional[str] = Field(None, description="Book type (e.g., 'A', 'B').")
#     # --- END NEW FIELDS ---


#     # Timestamps
#     created_at: datetime.datetime = Field(..., description="Timestamp when the group was created.")
#     updated_at: datetime.datetime = Field(..., description="Timestamp when the group was last updated.")

class GroupResponse(BaseModel):
    """
    Pydantic model for group response data, matching the existing Group model fields.
    """
    id: int = Field(..., description="Unique identifier of the group.")

    # String fields
    symbol: Optional[str] = Field(None, description="Symbol associated with the group (optional).")
    name: str = Field(..., description="Name of the group.")

    # Integer types
    commision_type: int = Field(..., description="Type of commission (integer).")
    commision_value_type: int = Field(..., description="Type of commission value (integer).")
    type: int = Field(..., description="Group type (integer).")

    pip_currency: Optional[str] = Field(None, description="Currency for pip calculation (optional).")

    # show_points is now an integer
    show_points: Optional[int] = Field(None, description="Show points setting (optional).")

    # Decimal fields
    swap_buy: Decimal = Field(..., max_digits=10, decimal_places=4, description="Swap buy value.")
    swap_sell: Decimal = Field(..., max_digits=10, decimal_places=4, description="Swap sell value.")
    commision: Decimal = Field(..., max_digits=10, decimal_places=4, description="Commission value.")
    margin: Decimal = Field(..., max_digits=10, decimal_places=4, description="Base margin value.")
    spread: Decimal = Field(..., max_digits=10, decimal_places=4, description="Spread value.")
    deviation: Decimal = Field(..., max_digits=10, decimal_places=4, description="Deviation value.")
    min_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Minimum lot size.")
    max_lot: Decimal = Field(..., max_digits=10, decimal_places=4, description="Maximum lot size.")
    pips: Decimal = Field(..., max_digits=10, decimal_places=4, description="Pips value.")
    spread_pip: Optional[Decimal] = Field(None, max_digits=10, decimal_places=4, description="Spread pip value (optional).")

    # --- NEW FIELDS ---
    sending_orders: Optional[str] = Field(None, description="Where orders are sent (e.g., 'Barclays', 'Rock').")
    book: Optional[str] = Field(None, description="Book type (e.g., 'A', 'B').")
    contract_size: Optional[Decimal] = Field(None, description="Contract size from external symbol info (optional).") # Added contract_size
    # --- END NEW FIELDS ---


    # Timestamps
    created_at: datetime.datetime = Field(..., description="Timestamp when the group was created.")
    updated_at: datetime.datetime = Field(..., description="Timestamp when the group was last updated.")


    class Config:
        from_attributes = True # Allow mapping from SQLAlchemy models

# Keep StatusResponse schema if it's in this file and used elsewhere
# class StatusResponse(BaseModel):
#     message: str = Field(..., description="Response message.")
