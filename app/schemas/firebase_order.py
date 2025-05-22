# app/schemas/firebase_order.py (New File)

from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional, Literal

class FirebaseOrderPlacementRequest(BaseModel):
    symbol: str = Field(..., description="Trading symbol, e.g., EURUSD")
    order_type: Literal[
        "BUY", "SELL", # Instant
        "BUY_LIMIT", "SELL_LIMIT", # Pending Limit
        "BUY_STOP", "SELL_STOP" # Pending Stop
    ] = Field(..., description="Type of the order")
    order_quantity: Decimal = Field(..., gt=0, description="Order quantity (lots)")
    # For instant orders, this price is the current market price (e.g., from allBuySellData[symbol]?.b or .o)
    # For pending/stop orders, this is the user-defined trigger price
    price: Decimal = Field(..., description="Price for the order (market for instant, target for pending/stop)")
    user_id: int = Field(..., description="User ID placing the order") # Assuming we'll resolve this from token on backend
    
    # Optional fields that might be part of the Firebase structure,
    # but may have defaults or be calculated.
    # We'll match what charts.js sends.
    account_type: str = "live"
    is_limit: Optional[bool] = None # Will be determined based on order_type
    swap: Optional[str] = "50" # Defaulted as per charts.js
    # stop_loss: Optional[Decimal] = None # For future extension
    # take_profit: Optional[Decimal] = None # For future extension
    # timestamp: Optional[int] = None # Can be added by backend

    class Config:
        use_enum_values = True # Ensures the Literal values are used directly


class FirebaseOrderDataStructure(BaseModel):
    """
    Represents the structure to be sent to Firebase RTDB 'trade_data' path.
    Based on observations from charts.js
    """
    account_type: str
    contract_value: str # Stored as string in Firebase based on charts.js examples
    is_limit: str # Stored as string 'true' or 'false'
    margin: str # Stored as string
    order_status: str # 'open' or 'pending'
    order_type: str
    price: str # Execution price or pending price, stored as string
    qnt: str # Quantity, stored as string
    swap: str
    symbol: str
    user_id: str # User ID, stored as string
    # Optional, can be added by backend
    # order_id: Optional[str] = None # Firebase will generate a unique key when we push
    timestamp: Optional[int] = None