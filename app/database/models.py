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

from sqlalchemy.orm import relationship # Import relationship for defining relationships

# Assuming you have a base declarative model defined in database/base.py
from .base import Base # Assuming Base is defined in app/database/base.py

# Import other models if they are in this file or imported here
# from .models import User, Group, Symbol # Circular import if in this file, import directly below


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
    email = Column(String(255), unique=True, index=True, nullable=False)
    phone_number = Column(String(20), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False) # Store hashed password

    # Other Fields
    user_type = Column(String(100), nullable=True) # Optional field

    # Financial Fields - Using SQLAlchemy's Decimal type
    # max_digits and decimal_places are hints, actual database constraints depend on dialect
    wallet_balance = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB
    leverage = Column(SQLDecimal(10, 2), default=Decimal("1.0"), nullable=False) # Default to 1.0, not Optional in DB
    margin = Column(SQLDecimal(18, 8), default=Decimal("0.00"), nullable=False) # Default to 0.00, not Optional in DB

    # Unique Account Number (Platform Specific)
    account_number = Column(String(100), unique=True, index=True, nullable=True)

    # Group Name (Storing as a string as requested)
    group_name = Column(String(255), index=True, nullable=True)

    # Status (Using Integer as requested, mapping 0/1 to boolean logic in app)
    status = Column(Integer, default=0, nullable=False) # Default to 0 (inactive/pending)

    security_question = Column(String(255), nullable=True)

    # Address/Location Fields
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

    # Relationships (Define relationships to other models)
    # referred_by_user = relationship("User", remote_side=[id]) # Self-referential relationship
    # referred_users = relationship("User") # Users referred by this user

    # Relationship to Refresh Tokens (Placeholder)
    # refresh_tokens = relationship("RefreshToken", back_populates="user")

    # Relationship to User Orders
    orders = relationship("UserOrder", back_populates="user")

    # Relationship to Wallet transactions
    wallet_transactions = relationship("Wallet", back_populates="user")

    # Relationship to OTPs
    otps = relationship("OTP", back_populates="user")


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
    # REMOVED unique=True from name
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

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # --- Add Unique Constraint for (symbol, name) combination ---
    __table_args__ = (UniqueConstraint('symbol', 'name', name='_symbol_name_uc'),)
    # The name='_symbol_name_uc' is optional but good practice for clarity

    # Relationships (Optional, but good practice)
    # If you decide to link users directly to groups via a foreign key on the Group model,
    # you would add a relationship here and potentially a foreign key column.
    # Since you're storing group_name as a string in the User model,
    # there isn't a direct foreign key relationship here by default.
    # If you reintroduce a foreign key in the User model, you'd add:
    # users = relationship("User", back_populates="group") # Assuming 'group' relationship in User model


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

    # Relationships (Add relationships to other models if needed, e.g., Orders)
    # orders = relationship("UserOrder", back_populates="symbol") # Assuming an Order model exists


class Wallet(Base):
    """
    SQLAlchemy model for the 'wallets' table.
    Represents individual wallet transactions or entries for a user.
    """
    __tablename__ = "wallets" # Using 'wallets' as the table name for transaction entries

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Relationship with User model primary key
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    user = relationship("User", back_populates="wallet_transactions") # Define relationship back to User

    # Fields based on your list
    symbol = Column(String(255), nullable=True) # Nullable as requested
    order_quantity = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    transaction_type = Column(String(50), nullable=False) # Assuming required, e.g., 'deposit', 'withdrawal', 'trade_profit', 'trade_loss'
    is_approved = Column(Integer, default=0, nullable=False) # Using Integer as requested, default 0 (pending/not approved)
    order_type = Column(String(50), nullable=True) # e.g., 'buy', 'sell' - Nullable as requested
    transaction_amount = Column(SQLDecimal(18, 8), nullable=False) # Amount of the transaction, assuming required

    # transaction_time - Timestamp when is_approved changes (Logic handled in CRUD/Service)
    # We store the timestamp here. Application logic will update this field.
    transaction_time = Column(DateTime, nullable=True) # Nullable, will be set when approved

    # transaction_id - randomly generated unique 10-digit id generated in the backend
    # Logic for generation goes in CRUD/Service. Store as String to handle leading zeros if needed.
    transaction_id = Column(String(100), unique=True, index=True, nullable=False) # Assuming required and unique

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships (Add relationships to other models if needed, e.g., a specific Order)
    # order_id = Column(Integer, ForeignKey("user_orders.id"), nullable=True) # Corrected ForeignKey table name
    # order = relationship("UserOrder", back_populates="wallet_transactions") # If linking to a specific order


class UserOrder(Base):
    """
    SQLAlchemy model for the 'user_orders' table.
    Represents a trading order placed by a user.
    """
    __tablename__ = "user_orders" # Using 'user_orders' as the table name

    # Primary Key
    id = Column(Integer, primary_key=True, index=True)

    # Unique Order ID (Generated by the application)
    order_id = Column(String(255), unique=True, index=True, nullable=False)

    # Link to the User who placed the order
    # Using Integer for foreign key, linking to User.id
    order_user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    # Relationship back to the User
    user = relationship("User", back_populates="orders")

    # Required Fields (as requested)
    order_status = Column(String(255), nullable=False) # e.g., "Open", "Closed", "Cancelled"
    order_company_name = Column(String(255), nullable=False) # e.g., "AAPL", "GOOGL" - Consider linking to Symbols table instead of string
    order_type = Column(String(255), nullable=False) # e.g., "Buy", "Sell", "Limit", "Market"

    # Financial values - using Decimal for precision (Required as requested)
    order_price = Column(SQLDecimal(18, 8), nullable=False) # Price of the asset
    order_quantity = Column(SQLDecimal(18, 8), nullable=False) # Number of units/lots
    margin = Column(SQLDecimal(18, 8), nullable=False) # Made required
    contract_value = Column(SQLDecimal(18, 8), nullable=False) # Made required

    # Optional Financial values - using Decimal
    net_profit = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    close_price = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    swap = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    commission = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    stop_loss = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal
    take_profit = Column(SQLDecimal(18, 8), nullable=True) # Nullable Decimal

    # Message fields
    cancel_message = Column(String(255), nullable=True) # Nullable String
    close_message = Column(String(255), nullable=True) # Nullable String

    # Status (Using Integer as requested)
    status = Column(Integer, default=1, nullable=True) # Nullable Integer, default 1 (Check meaning of 1)

    # Timestamps (Using SQLAlchemy's func.now() for database-side default)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships (Add relationships to other models if needed)
    # Linking to Symbol model would be more robust than storing company name as string
    # symbol_id = Column(Integer, ForeignKey("symbols.id"), nullable=True)
    # symbol = relationship("Symbol", back_populates="orders") # Assuming 'orders' relationship in Symbol model

    # If linking Wallet transactions to specific orders:
    # wallet_transactions = relationship("Wallet", back_populates="order")

class OTP(Base):
    """
    SQLAlchemy model for the 'otps' table.
    Stores One-Time Passwords for user verification.
    """
    __tablename__ = "otps"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), index=True, nullable=False)
    otp_code = Column(String(10), nullable=False) # Store the OTP code (e.g., 6 digits)
    created_at = Column(DateTime, server_default=func.now(), nullable=False)
    expires_at = Column(DateTime, nullable=False) # When the OTP expires

    # Relationship back to the User
    user = relationship("User", back_populates="otps")

