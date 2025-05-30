from typing import Optional, Any
from pydantic import BaseModel, Field, model_validator
from decimal import Decimal

# --- Place Order ---
class OrderPlacementRequest(BaseModel):
    order_id: str
    symbol: str = Field(..., alias="order_company_name")
    order_type: str
    order_price: Decimal
    order_quantity: Decimal
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None  # For service account API call

    class Config:
        from_attributes = True
        populate_by_name = True


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
    close_price: Optional[Decimal] = None
    net_profit: Optional[Decimal] = None
    swap: Optional[Decimal] = None
    commission: Optional[Decimal] = None
    cancel_message: Optional[str] = None
    close_message: Optional[str] = None
    status: Optional[int] = 1

    # --- Tracking Fields ---
    takeprofit_id: Optional[str] = None
    stoploss_id: Optional[str] = None
    cancel_id: Optional[str] = None
    close_id: Optional[str] = None
    modify_id: Optional[str] = None
    stoploss_cancel_id: Optional[str] = None
    takeprofit_cancel_id: Optional[str] = None

    class Config:
        from_attributes = True


# --- Response Schema ---
class OrderResponse(OrderCreateInternal):
    id: int
    created_at: Any
    updated_at: Any

    class Config:
        from_attributes = True


# --- Close Order Request ---
class CloseOrderRequest(BaseModel):
    order_id: str
    close_price: Decimal
    user_id: Optional[int] = None

    class Config:
        from_attributes = True


# --- Stop Loss / Take Profit Update ---
class UpdateStopLossTakeProfitRequest(BaseModel):
    order_id: str
    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    stoploss_id: Optional[str] = None
    takeprofit_id: Optional[str] = None

    @model_validator(mode="after")
    def validate_tp_sl(self) -> 'UpdateStopLossTakeProfitRequest':
        if not self.stop_loss and not self.take_profit:
            raise ValueError("Either stop_loss or take_profit must be provided.")
        if self.stop_loss and not self.stoploss_id:
            raise ValueError("stoploss_id is required when stop_loss is provided.")
        if self.take_profit and not self.takeprofit_id:
            raise ValueError("takeprofit_id is required when take_profit is provided.")
        return self

    class Config:
        from_attributes = True


# --- Order PATCH Update Schema ---
class OrderUpdateRequest(BaseModel):
    order_status: Optional[str]
    order_price: Optional[Decimal]
    order_quantity: Optional[Decimal]
    margin: Optional[Decimal]
    close_price: Optional[Decimal]
    net_profit: Optional[Decimal]
    stop_loss: Optional[Decimal]
    take_profit: Optional[Decimal]
    contract_value: Optional[Decimal]
    commission: Optional[Decimal]
    swap: Optional[Decimal]
    cancel_message: Optional[str]
    close_message: Optional[str]

    # Tracking Fields
    stoploss_id: Optional[str]
    takeprofit_id: Optional[str]
    cancel_id: Optional[str]
    close_id: Optional[str]
    modify_id: Optional[str]
    stoploss_cancel_id: Optional[str]
    takeprofit_cancel_id: Optional[str]

    class Config:
        from_attributes = True
