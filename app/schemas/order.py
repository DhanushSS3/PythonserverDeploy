from typing import Optional, Any
from pydantic import BaseModel, Field, model_validator
from decimal import Decimal

# --- Place Order ---
class OrderPlacementRequest(BaseModel):
    symbol: str # Corresponds to order_company_name from your description
    order_type: str # E.g., "MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT"
    order_quantity: Decimal = Field(..., gt=0)
    order_price: Decimal # For LIMIT/STOP. For MARKET, can be current market price or 0 if server fetches.
    user_type: str # "live" or "demo" as passed by frontend

    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None # For service accounts placing orders for other users.

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- Internal Model for Order Creation ---
class OrderCreateInternal(BaseModel):
    order_id: str
    order_status: str
    order_user_id: int
    order_company_name: str
    order_type: str
    order_price: Decimal
    order_quantity: Decimal
    contract_value: Decimal
    margin: Decimal

    # Optional financials
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    close_id: Optional[str] = None # Added for tracking closed orders


# --- Order Response Schema ---
class OrderResponse(BaseModel):
    order_id: str
    order_status: str
    order_user_id: int
    order_company_name: str
    order_type: str
    order_price: Decimal
    order_quantity: Decimal
    contract_value: Decimal
    margin: Decimal
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    created_at: Optional[Any] = None # datetime will be serialized by FastAPI/Pydantic
    updated_at: Optional[Any] = None # datetime will be serialized by FastAPI/Pydantic
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    close_order_id: Optional[str] = None # Added for tracking closed orders


# --- Close Order Request Schema ---
class CloseOrderRequest(BaseModel):
    order_id: str
    close_price: Decimal
    user_id: Optional[int] = None # For service accounts closing orders for other users.
    # Frontend might pass these for context, but backend should verify/use its own
    order_type: Optional[str] = None
    order_company_name: Optional[str] = None
    order_status: Optional[str] = None
    status: Optional[str] = None # This seems redundant with order_status, but keeping if frontend uses it

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- Update Stop Loss / Take Profit Request Schema ---
class UpdateStopLossTakeProfitRequest(BaseModel):
    order_id: str
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None # For service accounts updating orders for other users.
    modify_id: Optional[str] = None # Unique ID for this modification
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_tp_sl(self) -> 'UpdateStopLossTakeProfitRequest':
        if not self.stop_loss and not self.take_profit:
            raise ValueError("Either stop_loss or take_profit must be provided.")
        if self.stop_loss is not None and not self.stoploss_id:
            raise ValueError("stoploss_id is required when stop_loss is provided.")
        if self.take_profit is not None and not self.takeprofit_id:
            raise ValueError("takeprofit_id is required when take_profit is provided.")
        return self

    class Config:
        from_attributes = True


# --- Update Order ---
class OrderUpdateRequest(BaseModel):
    order_status: str = Field(..., description="New status of the order (OPEN, CLOSE, etc.)")
    order_price: Optional[Decimal] = Field(None, description="Updated order price if changed")
    margin: Optional[Decimal] = Field(None, description="Updated margin if changed")
    profit_loss: Optional[Decimal] = Field(None, description="Profit/loss amount if order is closed")
    close_price: Optional[Decimal] = Field(None, description="Price at which order was closed")
    close_time: Optional[str] = Field(None, description="Time when order was closed")

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- Order PATCH Update Schema ---
class OrderUpdateRequest(BaseModel):
    order_status: Optional[str] = None
    order_price: Optional[Decimal] = None
    order_quantity: Optional[Decimal] = None
    margin: Optional[Decimal] = None
    close_price: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    contract_value: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None

    # Tracking Fields
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None
    close_id: Optional[str] = None # Added for OrderActionHistory tracking