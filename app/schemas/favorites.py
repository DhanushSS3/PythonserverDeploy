from typing import List, Optional
from pydantic import BaseModel, Field, validator
from datetime import datetime


class FavoriteSymbolBase(BaseModel):
    """Base schema for favorite symbol operations"""
    symbol: str = Field(..., description="The symbol name (e.g., 'EURUSD')")


class AddFavoriteSymbol(FavoriteSymbolBase):
    """Schema for adding a favorite symbol"""
    pass


class RemoveFavoriteSymbol(FavoriteSymbolBase):
    """Schema for removing a favorite symbol"""
    pass


class FavoriteSymbolResponse(FavoriteSymbolBase):
    """Schema for favorite symbol response"""
    id: int
    symbol_id: int
    created_at: datetime
    
    class Config:
        orm_mode = True


class FavoriteSymbolListResponse(BaseModel):
    """Schema for list of favorite symbols"""
    favorites: List[FavoriteSymbolBase]
    total: int
    
    class Config:
        orm_mode = True


class SymbolDetails(BaseModel):
    """Schema for detailed symbol information"""
    id: int
    name: str
    type: int
    market_price: float
    profit_currency: str
    created_at: datetime
    updated_at: datetime
    
    @validator('created_at', 'updated_at', pre=True)
    def validate_datetime(cls, value):
        """Handle invalid datetime values like '0000-00-00 00:00:00'"""
        if value == '0000-00-00 00:00:00' or value == '0000-00-00':
            # Return a default date if the value is invalid
            return datetime.now()
        return value
    
    class Config:
        orm_mode = True


class SimpleFavoriteSymbol(BaseModel):
    """Simplified schema for favorite symbol (name only)"""
    name: str
    
    class Config:
        orm_mode = True


class FavoriteSymbolsWithDetails(BaseModel):
    """Schema for favorite symbols with detailed information"""
    favorites: List[SymbolDetails]
    total: int
    
    class Config:
        orm_mode = True


class SimpleFavoriteSymbolsList(BaseModel):
    """Simplified schema for list of favorite symbols (names only)"""
    symbols: List[str]
    
    class Config:
        orm_mode = True 