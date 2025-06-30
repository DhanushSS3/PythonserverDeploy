# # # app/crud/wallet.py

# # import datetime
# # import uuid
# # from decimal import Decimal
# # from sqlalchemy.ext.asyncio import AsyncSession
# # from sqlalchemy.exc import SQLAlchemyError
# # from app.database.models import Wallet  # Import the Wallet model from models.py
# # from app.schemas.wallet import WalletCreate # Import the WalletCreate schema

# # import logging
# # from sqlalchemy import select
# # from typing import List, Optional

# # logger = logging.getLogger(__name__)

# # async def create_wallet_record(
# #     db: AsyncSession,
# #     wallet_data: WalletCreate
# # ) -> Wallet | None:
# #     """
# #     Creates a new wallet transaction record using data from a WalletCreate schema.
# #     This function now supports associating the record with either a regular user
# #     or a demo user.

# #     Args:
# #         db: The asynchronous database session.
# #         wallet_data: A WalletCreate Pydantic schema object containing transaction details.
# #                      It should contain either user_id or demo_user_id, but not both.

# #     Returns:
# #         The newly created Wallet object, or None on error.
# #     """
# #     try:
# #         # Validate that only one of user_id or demo_user_id is provided
# #         if wallet_data.user_id and wallet_data.demo_user_id:
# #             raise ValueError("Wallet record cannot be associated with both a user and a demo user.")
# #         if not wallet_data.user_id and not wallet_data.demo_user_id:
# #             raise ValueError("Wallet record must be associated with either a user or a demo user.")

# #         transaction_id = str(uuid.uuid4())
# #         logger.debug(f"Generated transaction ID: {transaction_id}")

# #         # Create the wallet transaction object, assigning to the correct foreign key
# #         wallet_record = Wallet(
# #             user_id=wallet_data.user_id,
# #             demo_user_id=wallet_data.demo_user_id, # Assign demo_user_id if present
# #             order_quantity=wallet_data.order_quantity,
# #             symbol=wallet_data.symbol,
# #             transaction_type=wallet_data.transaction_type,
# #             is_approved=wallet_data.is_approved,
# #             order_type=wallet_data.order_type,
# #             transaction_amount=wallet_data.transaction_amount,
# #             transaction_id=transaction_id,
# #             description=wallet_data.description,
# #         )

# #         db.add(wallet_record)
# #         await db.flush()
# #         await db.refresh(wallet_record)

# #         associated_id = wallet_data.user_id if wallet_data.user_id else wallet_data.demo_user_id
# #         user_type_log = "user" if wallet_data.user_id else "demo user"
# #         logger.info(
# #             f"Wallet record created successfully for {user_type_log} {associated_id} "
# #             f"with transaction ID {transaction_id}."
# #         )
# #         return wallet_record

# #     except SQLAlchemyError as e:
# #         logger.error(
# #             f"Database error creating wallet record: {e}",
# #             exc_info=True
# #         )
# #         await db.rollback()
# #         return None
# #     except ValueError as e:
# #         logger.error(f"Validation error creating wallet record: {e}")
# #         await db.rollback()
# #         return None
# #     except Exception as e:
# #         logger.error(
# #             f"Unexpected error creating wallet record: {e}",
# #             exc_info=True
# #         )
# #         await db.rollback()
# #         return None

# # async def get_wallet_records_by_user_id(
# #     db: AsyncSession, user_id: int, skip: int = 0, limit: int = 100
# # ) -> List[Wallet]:
# #     """
# #     Retrieves wallet transaction records for a specific regular user with pagination.
# #     """
# #     result = await db.execute(
# #         select(Wallet)
# #         .filter(Wallet.user_id == user_id)
# #         .offset(skip)
# #         .limit(limit)
# #         .order_by(Wallet.created_at.desc())
# #     )
# #     return result.scalars().all()

# # async def get_wallet_records_by_demo_user_id(
# #     db: AsyncSession, demo_user_id: int, skip: int = 0, limit: int = 100
# # ) -> List[Wallet]:
# #     """
# #     Retrieves wallet transaction records for a specific demo user with pagination.
# #     """
# #     result = await db.execute(
# #         select(Wallet)
# #         .filter(Wallet.demo_user_id == demo_user_id)
# #         .offset(skip)
# #         .limit(limit)
# #         .order_by(Wallet.created_at.desc())
# #     )
# #     return result.scalars().all()

# # async def update_wallet_record_approval(
# #     db: AsyncSession, transaction_id: str, is_approved: int
# # ) -> Wallet | None:
# #     """
# #     Updates the approval status of a wallet transaction record and sets transaction_time if approved.
# #     """
# #     result = await db.execute(
# #         select(Wallet).filter(Wallet.transaction_id == transaction_id)
# #     )
# #     wallet_record = result.scalars().first()

# #     if wallet_record:
# #         wallet_record.is_approved = is_approved
# #         if is_approved == 1 and wallet_record.transaction_time is None:
# #             wallet_record.transaction_time = datetime.datetime.now()

# #         await db.commit()
# #         await db.refresh(wallet_record)
# #         logger.info(f"Wallet record {transaction_id} approval status updated to {is_approved}.")
# #         return wallet_record
# #     else:
# #         logger.warning(f"Wallet record with transaction ID {transaction_id} not found for update.")
# #         return None


# # app/schemas/wallet.py

# from typing import Optional
# from datetime import datetime
# from decimal import Decimal

# from pydantic import BaseModel, Field

# # Base schema for common wallet transaction attributes
# class WalletBase(BaseModel):
#     symbol: Optional[str] = None
#     order_quantity: Optional[Decimal] = Field(None, decimal_places=8)
#     transaction_type: str # e.g., 'deposit', 'withdrawal', 'trade_profit', 'trade_loss'
#     is_approved: int = 0 # 0: pending/not approved, 1: approved
#     order_type: Optional[str] = None # e.g., 'buy', 'sell'
#     transaction_amount: Decimal = Field(..., decimal_places=8)
#     description: Optional[str] = None
#     order_id: Optional[str] = None  # Added order_id field to identify which order's transaction it is

#     class Config:
#         from_attributes = True # For Pydantic V2+, use from_attributes instead of orm_mode = True

# # Schema for creating a new wallet transaction record
# # Allows association with either user_id or demo_user_id
# class WalletCreate(WalletBase):
#     user_id: Optional[int] = None
#     demo_user_id: Optional[int] = None

# # Schema for updating an existing wallet transaction record
# class WalletUpdate(WalletBase):
#     symbol: Optional[str] = None
#     order_quantity: Optional[Decimal] = None
#     transaction_type: Optional[str] = None
#     is_approved: Optional[int] = None
#     order_type: Optional[str] = None
#     transaction_amount: Optional[Decimal] = None
#     description: Optional[str] = None

# # Schema for wallet transaction data as stored in the database (includes IDs and timestamps)
# class WalletInDBBase(WalletBase):
#     id: int
#     transaction_id: str
#     user_id: Optional[int] = None # Can be None if demo_user_id is set
#     demo_user_id: Optional[int] = None # Can be None if user_id is set
#     transaction_time: Optional[datetime] = None
#     created_at: datetime
#     updated_at: datetime

# # Full schema for reading wallet transaction data (what you'd typically return from an API)
# class Wallet(WalletInDBBase):
#     pass

# # Assuming these schemas are needed elsewhere for API requests/responses
# class WalletTransactionRequest(BaseModel):
#     amount: Decimal = Field(..., gt=0, decimal_places=8)
#     description: Optional[str] = None

# class WalletBalanceResponse(BaseModel):
#     user_id: int
#     new_balance: Decimal = Field(..., decimal_places=8)
#     message: str
#     transaction_id: Optional[str] = None


# app/schemas/wallet.py

from typing import Optional
from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field

# Base schema for common wallet transaction attributes
class WalletBase(BaseModel):
    symbol: Optional[str] = None
    order_quantity: Optional[Decimal] = Field(None, decimal_places=8)
    transaction_type: str # e.g., 'deposit', 'withdrawal', 'trade_profit', 'trade_loss'
    is_approved: int = 0 # 0: pending/not approved, 1: approved
    order_type: Optional[str] = None # e.g., 'buy', 'sell'
    transaction_amount: Decimal = Field(..., decimal_places=8)
    description: Optional[str] = None
    order_id: Optional[str] = None  # Added order_id field to identify which order's transaction it is

    class Config:
        from_attributes = True # For Pydantic V2+, use from_attributes instead of orm_mode = True

# Schema for creating a new wallet transaction record
# Allows association with either user_id or demo_user_id
class WalletCreate(WalletBase):
    user_id: Optional[int] = None
    demo_user_id: Optional[int] = None

# Schema for updating an existing wallet transaction record
class WalletUpdate(WalletBase):
    symbol: Optional[str] = None
    order_quantity: Optional[Decimal] = None
    transaction_type: Optional[str] = None
    is_approved: Optional[int] = None
    order_type: Optional[str] = None
    transaction_amount: Optional[Decimal] = None
    description: Optional[str] = None

# Schema for wallet transaction data as stored in the database (includes IDs and timestamps)
class WalletInDBBase(WalletBase):
    id: int
    transaction_id: str
    user_id: Optional[int] = None # Can be None if demo_user_id is set
    demo_user_id: Optional[int] = None # Can be None if user_id is set
    transaction_time: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime

# Full schema for reading wallet transaction data (what you'd typically return from an API)
# Renamed from Wallet to WalletResponse to match the import in wallets.py endpoint
class WalletResponse(WalletInDBBase):
    pass

# Assuming these schemas are needed elsewhere for API requests/responses
class WalletTransactionRequest(BaseModel):
    amount: Decimal = Field(..., gt=0, decimal_places=8)
    description: Optional[str] = None

class WalletBalanceResponse(BaseModel):
    user_id: int
    new_balance: Decimal = Field(..., decimal_places=8)
    message: str
    transaction_id: Optional[str] = None

from pydantic import BaseModel, Field
from decimal import Decimal
from typing import Optional

class AdminWalletActionRequest(BaseModel):
    user_id: int
    amount: Decimal
    currency: str
    reason: Optional[str] = None

class AdminWalletActionResponse(BaseModel):
    status: bool
    message: str
    balance: Optional[Decimal] = None

# Schema for total deposit amount response
class TotalDepositResponse(BaseModel):
    user_id: int
    total_deposit_amount: Decimal = Field(..., decimal_places=8)
    message: str = "Total deposit amount retrieved successfully"
    
    class Config:
        from_attributes = True
