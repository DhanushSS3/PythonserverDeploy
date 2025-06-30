# app/database/models.py

import datetime
from decimal import Decimal # Import Decimal from the standard decimal module
from typing import List, Optional

# Import specific components from sqlalchemy
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    func # Import func for default timestamps
)
# Import DECIMAL from sqlalchemy.types and alias it as SQLDecimal
from sqlalchemy.types import DECIMAL as SQLDecimal
import uuid # Import uuid for handling UUID strings if storing the 'id'
# from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship # Import relationship for defining relationships
from sqlalchemy.sql.expression import and_
from sqlalchemy import ForeignKey # Import foreign for relationship annotations

# Assuming you have a base declarative model defined in database/base.py
from .base import Base # Assuming Base is defined in app/database/base.py


class User(Base):
    """
    SQLAlchemy model for the 'users' table.
    Represents a user in the trading application.
    Includes personal, financial, and verification details.
    """
    __tablename__ = "users"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Required Fields
    name = Column(String(255), nullable=False)
    email = Column(String(255), index=True, nullable=False) # No longer globally unique
    phone_number = Column(String(20), index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False) # Store hashed password

    # Other Fields
    user_type = Column(String(100), nullable=True) # Optional field

    # Financial Fields - Using SQLAlchemy's Decimal type
    # max_digits and decimal_places are hints, actual database constraints depend on dialect
    wallet_balance = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB
    leverage = Column(SQLDecimal(10, 2), default=Decimal("1.0"), nullable=False) # Default to 1.0, not Optional in DB
    margin = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB
    net_profit = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB

    # Unique Account Number (Platform Specific)
    account_number = Column(String(100), unique=True, index=True, nullable=True)

    # Group Name (Storing as a string as requested)
    group_name = Column(String(255), index=True, nullable=True)

    # Status (Using Integer as requested, mapping 0/1 to boolean logic in app)
    status = Column(Integer, default=0, nullable=False) # Default to 0 (inactive/pending)

    security_question = Column(String(255), nullable=True)
    security_answer = Column(String(255), nullable=True) # New field for security question answer

    # Address/Location Fields
    country = Column(String(100), nullable=True)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(Integer, nullable=True) # Storing as Integer

    fund_manager = Column(String(255), nullable=True)
    is_self_trading = Column(Integer, default=1, nullable=False) # Default to 1

    # Image Proofs (Storing paths or identifiers)
    id_proof = Column(String(255), nullable=True) # Assuming storing file path/name or identifier
    id_proof_image = Column(String(255), nullable=True) # Assuming storing file path/name

    address_proof = Column(String(255), nullable=True) # Assuming storing file path/name or identifier
    address_proof_image = Column(String(255), nullable=True) # Assuming storing file path/name

    # Bank Details
    bank_ifsc_code = Column(String(50), nullable=True)
    bank_holder_name = Column(String(255), nullable=True)
    bank_branch_name = Column(String(255), nullable=True)
    bank_account_number = Column(String(100), nullable=True)

    # isActive (Using Integer as requested, mapping 0/1 to boolean logic in app)
    isActive = Column(Integer, default=0, nullable=False) # Default to 0 (not active)

    # Referral Fields
    # Foreign key to the User who referred this user (self-referential)
    referred_by_id = Column(Integer, ForeignKey("users.id"), nullable=True) # ForeignKey references the table name

    # Unique Referral Code (Auto-generated - logic for generation needed elsewhere)
    reffered_code = Column(String(20), unique=True, index=True, nullable=True)

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('email', 'phone_number', 'user_type', name='_email_phone_user_type_uc'),
    )

    # Relationships (Define relationships to other models)
    orders = relationship("UserOrder", back_populates="user")
    wallet_transactions = relationship("Wallet", back_populates="user")
    otps = relationship("OTP", back_populates="user")
    money_requests = relationship("MoneyRequest", back_populates="user")
    rock_orders = relationship("RockUserOrder", back_populates="user")
    # Add back simple relationship without complex conditions
    user_favorites = relationship("UserFavoriteSymbol", 
                                foreign_keys="[UserFavoriteSymbol.user_id]",
                                primaryjoin="User.id == UserFavoriteSymbol.user_id")


class DemoUser(Base):
    """
    SQLAlchemy model for the 'demo_users' table.
    Represents a DEMO user in the trading application.
    This model excludes sensitive personal and financial details present in the main User model.
    """
    __tablename__ = "demo_users"

    # Primary Key

    # Financial Fields
    net_profit = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB

    # Address/Location Fields
    country = Column(String(100), nullable=True)

    id = Column(Integer, primary_key=True, index=True)

    # Required Fields (replicated from User model, excluding sensitive ones)
    name = Column(String(255), nullable=False)
    email = Column(String(255), index=True, nullable=False)
    phone_number = Column(String(20), index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False) # Store hashed password
    
    # Other Fields (replicated from User model)
    user_type = Column(String(100), nullable=True) # Optional field, e.g., 'demo'

    # Financial Fields (replicated from User model)
    wallet_balance = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False)
    leverage = Column(SQLDecimal(10, 2), default=Decimal("1.0"), nullable=False)
    margin = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False)

    # Unique Account Number (Platform Specific)
    account_number = Column(String(100), unique=True, index=True, nullable=True)

    # Group Name (replicated from User model)
    group_name = Column(String(255), index=True, nullable=True)

    # Status (replicated from User model)
    status = Column(Integer, default=0, nullable=False) # Default to 0 (inactive/pending)

    security_question = Column(String(255), nullable=True)
    security_answer = Column(String(255), nullable=True) # New field for security question answer

    # Address/Location Fields (replicated from User model)
    city = Column(String(100), nullable=True)
    state = Column(String(100), nullable=True)
    pincode = Column(Integer, nullable=True) # Storing as Integer

    # isActive (replicated from User model)
    isActive = Column(Integer, default=0, nullable=False) # Default to 0 (not active)

    # Referral Fields (replicated from User model)
    referred_by_id = Column(Integer, ForeignKey("demo_users.id"), nullable=True) # Self-referential for demo users
    reffered_code = Column(String(20), unique=True, index=True, nullable=True)

    # Timestamps (replicated from User model)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    __table_args__ = (
        UniqueConstraint('email', 'phone_number', 'user_type', name='_demo_email_phone_user_type_uc'),
    )

    # Relationships for DemoUser
    orders = relationship("DemoUserOrder", back_populates="user")
    wallet_transactions = relationship("Wallet", back_populates="demo_user") # Relationship to Wallet
    otps = relationship("OTP", back_populates="demo_user") # Relationship to OTP
    # Add back simple relationship without complex conditions 
    demo_user_favorites = relationship("UserFavoriteSymbol", 
                                     foreign_keys="[UserFavoriteSymbol.user_id]",
                                     primaryjoin="DemoUser.id == UserFavoriteSymbol.user_id",
                                     overlaps="user_favorites")


class Group(Base):
    """
    SQLAlchemy model for the 'groups' table.
    Represents a trading group or portfolio configuration.
    """
    __tablename__ = "groups"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # String fields
    symbol = Column(String(255), nullable=True) # Nullable as requested
    name = Column(String(255), index=True, nullable=False) # Name is required, but not unique on its own

    # Integer types
    commision_type = Column(Integer, nullable=False) # Changed from str to int
    commision_value_type = Column(Integer, nullable=False) # Changed from str to int
    type = Column(Integer, nullable=False) # Changed from str to int

    pip_currency = Column(String(255), default="USD", nullable=True) # Nullable with default

    # show_points is now an integer
    show_points = Column(Integer, nullable=True) # Nullable as requested

    # Decimal fields for values that can be fractional or monetary
    # Using max_digits and decimal_places appropriate for financial/trading values
    # Adjust precision as needed based on your trading instrument requirements
    swap_buy = Column(SQLDecimal(10, 4), default=Decimal("0.0"), nullable=False) # Default '0' -> Decimal
    swap_sell = Column(SQLDecimal(10, 4), default=Decimal("0.0"), nullable=False) # Default '0' -> Decimal
    commision = Column(SQLDecimal(10, 4), nullable=False) # Commission value
    margin = Column(SQLDecimal(10, 4), nullable=False) # Base margin value for the group
    spread = Column(SQLDecimal(10, 4), nullable=False)
    deviation = Column(SQLDecimal(10, 4), nullable=False)
    min_lot = Column(SQLDecimal(10, 4), nullable=False)
    max_lot = Column(SQLDecimal(10, 4), nullable=False)
    pips = Column(SQLDecimal(10, 4), nullable=False)
    spread_pip = Column(SQLDecimal(10, 4), nullable=True) # Nullable as requested

    # --- NEW COLUMNS ---
    # Column to store where orders are sent (e.g., 'Barclays', 'Rock')
    sending_orders = Column(String(255), nullable=True) # Assuming nullable, adjust if required
    # Column to store the book type (e.g., 'A', 'B')
    book = Column(String(10), nullable=True) # Assuming nullable, adjust if required
    # --- END NEW COLUMNS ---


    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Add Unique Constraint for (symbol, name) combination ---
    __table_args__ = (UniqueConstraint('symbol', 'name', name='_symbol_name_uc'),)


class Symbol(Base):
    """
    SQLAlchemy model for the 'symbols' table.
    Represents a tradable symbol (e.g., currency pair, crypto).
    """
    __tablename__ = "symbols"

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Fields based on provided structure
    name = Column(String(255), nullable=False) # Assuming name is required
    type = Column(Integer, nullable=False) # Assuming type is required
    pips = Column(SQLDecimal(18, 8), nullable=False) # Using SQLDecimal for numeric
    spread_pip = Column(SQLDecimal(18, 8), nullable=True) # Nullable numeric
    market_price = Column(SQLDecimal(18, 8), nullable=False) # Using SQLDecimal for numeric, assuming required
    show_points = Column(Integer, nullable=True) # Nullable integer
    profit_currency = Column(String(255), nullable=False) # Assuming required

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Add back simple relationship
    favorited_by = relationship("UserFavoriteSymbol", back_populates="symbol")


class Wallet(Base):
    """
    SQLAlchemy model for the 'wallets' table.
    Represents individual wallet transactions or entries for a user.
    """
    __tablename__ = "wallets" # Using 'wallets' as the table name for transaction entries

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Foreign keys to User and DemoUser tables (one should be populated)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True) # Made nullable
    demo_user_id = Column(Integer, ForeignKey("demo_users.id"), index=True, nullable=True) # New foreign key for DemoUser

    # Relationships back to User and DemoUser
    user = relationship("User", back_populates="wallet_transactions") # Define relationship back to User
    demo_user = relationship("DemoUser", back_populates="wallet_transactions") # Define relationship back to DemoUser

    # Add order_id to identify which order's transaction it is
    order_id = Column(String(64), nullable=True, index=True)  # Nullable as not all wallet transactions are tied to orders
    
    # Fields based on your list
    symbol = Column(String(255), nullable=True) # Nullable as requested
    order_quantity = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    transaction_type = Column(String(50), nullable=False) # Assuming required, e.g., 'deposit', 'withdrawal', 'trade_profit', 'trade_loss'
    is_approved = Column(Integer, default=0, nullable=False) # Using Integer as requested, default 0 (pending/not approved)
    order_type = Column(String(50), nullable=True) # e.g., 'buy', 'sell' - Nullable as requested
    transaction_amount = Column(SQLDecimal(18, 8), nullable=False) # Amount of the transaction, assuming required

    # --- NEW COLUMN ---
    description = Column(String(500), nullable=True) # Optional description for the transaction
    # --- END NEW COLUMN ---


    # transaction_time - Timestamp when is_approved changes (Logic handled in CRUD/Service)
    # We store the timestamp here. Application logic will update this field.
    transaction_time = Column(DateTime, nullable=True) # Nullable, will be set when approved

    # transaction_id - randomly generated unique 10-digit id generated in the backend
    # Logic for generation goes in CRUD/Service. Store as String to handle leading zeros if needed.
    transaction_id = Column(String(100), unique=True, index=True, nullable=False) # Assuming required and unique

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)



# Shared fields to be added:
# cancel_id, close_id, modify_id, stoploss_id, takeprofit_id, stoploss_cancel_id, takeprofit_cancel_id

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, DECIMAL as SQLDecimal, func
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, Boolean, Text, Table, Float, func, CheckConstraint
from sqlalchemy.orm import relationship
from app.database.base import Base


class UserOrder(Base):
    __tablename__ = "user_orders"

    status = Column(String(30), nullable=True, doc="Status string (max 30 chars)")

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(64), unique=True, index=True, nullable=False)
    order_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_company_name = Column(String(255), nullable=False)
    order_type = Column(String(20), nullable=False)
    order_status = Column(String(20), nullable=False)
    order_price = Column(SQLDecimal(18, 8), nullable=False)
    order_quantity = Column(SQLDecimal(18, 8), nullable=False)
    contract_value = Column(SQLDecimal(18, 8), nullable=True)
    margin = Column(SQLDecimal(18, 8), nullable=True)

    stop_loss = Column(SQLDecimal(18, 8), nullable=True)
    take_profit = Column(SQLDecimal(18, 8), nullable=True)
    close_price = Column(SQLDecimal(18, 8), nullable=True)
    net_profit = Column(SQLDecimal(18, 8), nullable=True)
    swap = Column(SQLDecimal(18, 8), nullable=True)
    commission = Column(SQLDecimal(18, 8), nullable=True)
    cancel_message = Column(String(255), nullable=True)
    close_message = Column(String(255), nullable=True)

    # Tracking fields
    cancel_id = Column(String(64), nullable=True)
    close_id = Column(String(64), nullable=True)
    modify_id = Column(String(64), nullable=True)
    stoploss_id = Column(String(64), nullable=True)
    takeprofit_id = Column(String(64), nullable=True)
    stoploss_cancel_id = Column(String(64), nullable=True)
    takeprofit_cancel_id = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("User", back_populates="orders")

    __table_args__ = (
        CheckConstraint("status IS NULL OR length(status) >= 0", name="userorder_status_min_length_0"),
        CheckConstraint("length(status) <= 30", name="userorder_status_max_length_30"),
    )


class DemoUserOrder(Base):
    __tablename__ = "demo_user_orders"

    # Status string field, must be between 10 and 30 characters
    status = Column(String(30), nullable=True, doc="Status string (10-30 chars)")

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(64), unique=True, index=True, nullable=False)
    order_user_id = Column(Integer, ForeignKey("demo_users.id"), nullable=False)
    order_company_name = Column(String(255), nullable=False)
    order_type = Column(String(20), nullable=False)
    order_status = Column(String(20), nullable=False)
    order_price = Column(SQLDecimal(18, 8), nullable=False)
    order_quantity = Column(SQLDecimal(18, 8), nullable=False)
    contract_value = Column(SQLDecimal(18, 8), nullable=True)  # Changed to nullable for pending orders
    margin = Column(SQLDecimal(18, 8), nullable=True)  # Changed to nullable for pending orders

    stop_loss = Column(SQLDecimal(18, 8), nullable=True)
    take_profit = Column(SQLDecimal(18, 8), nullable=True)
    close_price = Column(SQLDecimal(18, 8), nullable=True)
    net_profit = Column(SQLDecimal(18, 8), nullable=True)
    swap = Column(SQLDecimal(18, 8), nullable=True)
    commission = Column(SQLDecimal(18, 8), nullable=True)
    cancel_message = Column(String(255), nullable=True)
    close_message = Column(String(255), nullable=True)

    # Tracking fields
    cancel_id = Column(String(64), nullable=True)
    close_id = Column(String(64), nullable=True)
    modify_id = Column(String(64), nullable=True)
    stoploss_id = Column(String(64), nullable=True)
    takeprofit_id = Column(String(64), nullable=True)
    stoploss_cancel_id = Column(String(64), nullable=True)
    takeprofit_cancel_id = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    user = relationship("DemoUser", back_populates="orders")

    __table_args__ = (
        CheckConstraint("status IS NULL OR length(status) >= 10", name="demouserorder_status_min_length_10"),
        CheckConstraint("status IS NULL OR length(status) <= 30", name="demouserorder_status_max_length_30"),
    )



class RockUserOrder(Base):
    __tablename__ = "rock_user_orders"

    id = Column(Integer, primary_key=True, index=True)
    order_id = Column(String(64), unique=True, index=True, nullable=False)
    # This ForeignKey points to the 'users' table, assuming rock orders are also associated with a User
    order_user_id = Column(Integer, ForeignKey("users.id"), nullable=False)
    order_company_name = Column(String(255), nullable=False)
    order_type = Column(String(20), nullable=False)  # e.g., 'buy', 'sell'
    order_status = Column(String(20), nullable=False) # e.g., 'pending', 'open', 'closed', 'cancelled'
    order_price = Column(SQLDecimal(18, 8), nullable=False)
    order_quantity = Column(SQLDecimal(18, 8), nullable=False)
    contract_value = Column(SQLDecimal(18, 8), nullable=True)  # Changed to nullable for pending orders
    margin = Column(SQLDecimal(18, 8), nullable=True)  # Changed to nullable for pending orders

    stop_loss = Column(SQLDecimal(18, 8), nullable=True)
    take_profit = Column(SQLDecimal(18, 8), nullable=True)
    close_price = Column(SQLDecimal(18, 8), nullable=True)
    net_profit = Column(SQLDecimal(18, 8), nullable=True)
    swap = Column(SQLDecimal(18, 8), nullable=True)
    commission = Column(SQLDecimal(18, 8), nullable=True)
    cancel_message = Column(String(255), nullable=True)
    close_message = Column(String(255), nullable=True)

    # Tracking fields
    cancel_id = Column(String(64), nullable=True)
    close_id = Column(String(64), nullable=True)
    modify_id = Column(String(64), nullable=True)
    stoploss_id = Column(String(64), nullable=True)
    takeprofit_id = Column(String(64), nullable=True)
    stoploss_cancel_id = Column(String(64), nullable=True)
    takeprofit_cancel_id = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationship to User model.
    # This assumes you will add a corresponding 'rock_orders' relationship list
    # to your User model, like:
    # rock_orders = relationship("RockUserOrder", back_populates="user")
    user = relationship("User", back_populates="rock_orders")

class OrderActionHistory(Base):
    __tablename__ = "order_action_history"

    id = Column(Integer, primary_key=True, index=True)

    user_id = Column(Integer, nullable=False)
    user_type = Column(String(10), nullable=False)  # 'live' or 'demo'

    # Add order_id as a required field (not a foreign key since it could be from different tables)
    order_id = Column(String(64), nullable=False, index=True)
    
    # Add cancel_id
    cancel_id = Column(String(64), nullable=True)
    
    # Add close_id
    close_id = Column(String(64), nullable=True)

    # Add action_type to track what kind of action was performed - now required
    action_type = Column(String(50), nullable=False)

    # New & existing tracked action IDs
    modify_id = Column(String(64), nullable=True)
    stoploss_id = Column(String(64), nullable=True)
    takeprofit_id = Column(String(64), nullable=True)
    stoploss_cancel_id = Column(String(64), nullable=True)
    takeprofit_cancel_id = Column(String(64), nullable=True)

    created_at = Column(DateTime, server_default=func.now(), nullable=False)


class OTP(Base):
    """
    SQLAlchemy model for the 'otps' table.
    Stores One-Time Passwords for user verification.
    """
    __tablename__ = "otps"

    id = Column(Integer, primary_key=True, index=True)
    # Foreign keys to User and DemoUser tables (one should be populated)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=True) # Made nullable
    demo_user_id = Column(Integer, ForeignKey("demo_users.id"), index=True, nullable=True) # New foreign key for DemoUser

    otp_code = Column(String(10), nullable=False) # Store the OTP code (e.g., 6 digits)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    expires_at = Column(DateTime, nullable=False) # When the OTP expires

    # Relationships back to User and DemoUser
    user = relationship("User", back_populates="otps")
    demo_user = relationship("DemoUser", back_populates="otps")


class ExternalSymbolInfo(Base):
    """
    SQLAlchemy model for the 'external_symbol_info' table.
    Stores static data fetched from an external symbol API.
    """
    __tablename__ = "external_symbol_info"

    id = Column(Integer, primary_key=True, index=True) # Using integer primary key

    # Store the external API's ID if needed for reference
    external_id = Column(String(36), index=True, nullable=True) # Store external API's UUID string ID

    # fix_symbol should be unique for lookups
    fix_symbol = Column(String(255), unique=True, index=True, nullable=False)
    description = Column(String(255), nullable=True)
    # Using SQLDecimal for precise decimal values. Adjust precision and scale as needed.
    digit = Column(SQLDecimal(10, 5), nullable=True)
    base = Column(String(10), nullable=True) # Base currency/asset
    profit = Column(String(10), nullable=True) # Profit currency
    # Storing 'margin' from API as String based on your example ("BTC", "1:10")
    margin = Column(String(50), nullable=True)
    contract_size = Column(SQLDecimal(20, 8), nullable=True) # Adjust precision/scale
    # Storing 'margin_leverage' as String
    margin_leverage = Column(String(50), nullable=True)
    swap = Column(String(255), nullable=True) # Swap information
    commission = Column(String(255), nullable=True) # Commission information
    minimum_per_trade = Column(SQLDecimal(20, 8), nullable=True)
    steps = Column(SQLDecimal(20, 8), nullable=True)
    maximum_per_trade = Column(SQLDecimal(20, 8), nullable=True)
    maximum_per_login = Column(String(255), nullable=True) # Assuming string
    is_subscribed = Column(Boolean, default=False, nullable=False)
    exchange_folder_id = Column(String(36), nullable=True) # Store as string

    # The 'type' field from the API response indicates instrument type
    # Map 'type' to 'instrument_type' column
    instrument_type = Column(String(10), nullable=True) # Store as string ("1", "2", "3", "4")

    def __repr__(self):
        return f"<ExternalSymbolInfo(fix_symbol='{self.fix_symbol}', instrument_type='{self.instrument_type}', contract_size={self.contract_size})>"


class MoneyRequest(Base):
    """
    SQLAlchemy model for the 'money_requests' table.
    Represents a user's request to deposit or withdraw funds.
    """
    __tablename__ = "money_requests"

    id = Column(Integer, primary_key=True, index=True)

    # Foreign key to User table
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    # Relationship back to the User
    user = relationship("User", back_populates="money_requests")

    # Amount of the request
    # Using SQLDecimal for precision, adjust precision (e.g., 18) and scale (e.g., 8) as needed
    amount = Column(SQLDecimal(18, 8), nullable=False)

    # Type of request: 'deposit' or 'withdraw'
    type = Column(String(10), nullable=False) # 'deposit' or 'withdraw'

    # Status of the request:
    # 0: requested
    # 1: approved
    # 2: rejected
    status = Column(Integer, default=0, nullable=False)

    # Timestamps
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    def __repr__(self):
        return f"<MoneyRequest(id={self.id}, user_id={self.user_id}, type='{self.type}', amount={self.amount}, status={self.status})>"


class UserFavoriteSymbol(Base):
    """
    SQLAlchemy model for the 'user_favorite_symbols' table.
    Junction table for many-to-many relationship between users and favorite symbols.
    """
    __tablename__ = "user_favorite_symbols"
    
    # Primary Key
    id = Column(Integer, primary_key=True, index=True)
    
    # Foreign Keys - no ondelete cascade as we're not using foreign key constraints
    user_id = Column(Integer, nullable=False, index=True)
    symbol_id = Column(Integer, ForeignKey("symbols.id", ondelete="CASCADE"), nullable=False, index=True)

    # Type field to distinguish between live and demo users
    user_type = Column(String(10), nullable=False)  # 'live' or 'demo'
    
    # Timestamps - only created_at as our table only has this column
    created_at = Column(DateTime, server_default=func.now(), nullable=False)

    # Add unique constraint to prevent duplicates
    __table_args__ = (UniqueConstraint('user_id', 'symbol_id', 'user_type', name='_user_symbol_type_uc'),)

    # Simple relationship to Symbol
    symbol = relationship("Symbol", back_populates="favorited_by")


class CryptoPayment(Base):
    __tablename__ = 'crypto_payments'
    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, nullable=False, index=True)
    merchant_order_id = Column(String(255), unique=True, index=True, nullable=False)
    base_amount = Column(SQLDecimal(20, 8), nullable=False)
    base_currency = Column(String(50), nullable=False)
    settled_currency = Column(String(50), nullable=False)
    network_symbol = Column(String(50), nullable=False)
    status = Column(String(50), nullable=False, default='PENDING')  # e.g., PENDING, COMPLETED, FAILED
    transaction_details = Column(Text, nullable=True)  # Store callback data as JSON
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), default=func.now(), onupdate=func.now())
