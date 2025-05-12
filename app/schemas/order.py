# app/schemas/order.py

from typing import Optional
from pydantic import BaseModel
from decimal import Decimal

# Schema for creating an order
class OrderCreate(BaseModel):
    """
    Pydantic schema for the data required to create a new order.
    """
    order_status: str
    order_user_id: int
    order_company_name: str
    order_type: str
    order_price: Decimal
    order_quantity: Decimal
    contract_value: Decimal
    margin: Decimal
    # Optional fields based on the UserOrder model
    net_profit: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    status: Optional[int] = 1 # Assuming a default status

# You might want additional schemas later, e.g., for reading orders:
# class OrderResponse(OrderCreate):
#     id: int
#     order_id: str
#     created_at: datetime
#     updated_at: datetime
#
#     class Config:
#         from_attributes = True # For Pydantic V2+
#         # or orm_mode = True # For Pydantic V1