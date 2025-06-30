from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from decimal import Decimal

class PaymentRequest(BaseModel):
    baseCurrency: str
    settledCurrency: str
    networkSymbol: str
    baseAmount: Decimal

class PaymentResponseData(BaseModel):
    paymentUrl: str
    merchantOrderId: str
    # Add other fields from the actual API response as needed

class PaymentResponse(BaseModel):
    status: bool
    message: str
    data: Optional[PaymentResponseData] = None

class Currency(BaseModel):
    id: int
    name: str
    symbol: str
    type: str
    networks: List[Dict[str, Any]]

class CurrencyListResponse(BaseModel):
    status: bool
    message: str
    data: Optional[List[Currency]] = None

class CallbackData(BaseModel):
    type: str
    merchantOrderId: str
    status: str
    # Add other fields from the callback data as needed 