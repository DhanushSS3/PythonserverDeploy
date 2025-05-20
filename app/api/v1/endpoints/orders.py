# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Any, List, Dict
import uuid
import datetime
from pydantic import BaseModel, Field


from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution
)
from app.services.portfolio_calculator import _convert_to_usd

from app.crud import crud_order
from app.crud import user as crud_user

from sqlalchemy.future import select
from app.core.cache import get_adjusted_market_price_cache, get_group_symbol_settings_cache

from app.core.security import get_current_user # Ensure get_current_user is imported


logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

# CloseOrderRequest schema is now defined in app.schemas.order


# Endpoint to place a new order (kept as is)
@router.post(
    "/",
    response_model=OrderResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Place a new order",
    description="Allows an authenticated user to place a new trading order. Processing, including margin calculation and hedging, is handled by the backend service."
)
async def place_order(
    order_request: OrderPlacementRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    """
    Receives a new order request and delegates processing to the order_processing service.
    Handles service-specific exceptions and translates them to HTTP responses.
    """
    logger.info(f"Received order placement request for user {current_user.id}, symbol {order_request.symbol}")

    if order_request.order_quantity <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Order quantity must be positive."
        )

    try:
        new_db_order = await process_new_order(
            db=db,
            redis_client=redis_client,
            user=current_user, # Pass the current_user object
            order_request=order_request
        )

        logger.info(f"Order {new_db_order.order_id} successfully processed by service.")

        return OrderResponse.model_validate(new_db_order)

    except InsufficientFundsError as e:
        logger.warning(f"Order placement failed for user {current_user.id} due to insufficient funds: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except OrderProcessingError as e:
        logger.error(f"Order processing failed for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while processing the order."
        )
    except Exception as e:
        logger.error(f"Unexpected error during order placement for user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An unexpected error occurred."
        )


# Endpoint to get order by ID (kept as is)
@router.get("/{order_id}", response_model=OrderResponse)
async def read_order(
    order_id: str,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    db_order = await crud_order.get_order_by_id(db, order_id=order_id)
    if db_order is None:
        raise HTTPException(status_code=404, detail="Order not found")
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
         raise HTTPException(status_code=403, detail="Not authorized to view this order")
    return OrderResponse.model_validate(db_order)


# Endpoint to get user's orders (kept as is)
@router.get("/", response_model=List[OrderResponse])
async def read_user_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, skip=skip, limit=limit)
    return [OrderResponse.model_validate(order) for order in orders]

# --- Endpoints for Specific Order Statuses (kept as is) ---

@router.get("/open", response_model=List[OrderResponse])
async def read_user_open_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    open_orders = await crud_order.get_all_open_orders_by_user_id(db, user_id=current_user.id)
    return [OrderResponse.model_validate(order) for order in open_orders[skip:skip+limit]]

@router.get("/closed", response_model=List[OrderResponse])
async def read_user_closed_orders(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
    closed_orders = [order for order in orders if order.order_status == 'CLOSED']
    return [OrderResponse.model_validate(order) for order in closed_orders[skip:skip+limit]]


@router.post(
    "/close",
    response_model=OrderResponse,
    summary="Close an open order",
    description="Closes an open order, updates its status to 'CLOSED', and adjusts the user's overall margin based on hedging logic. Requires the order ID and closing price in the request body."
)
async def close_order(
    close_request: CloseOrderRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user) # The authenticated user
):
    # Get order_id and close_price from the request body
    order_id = close_request.order_id
    close_price = Decimal(str(close_request.close_price)) # Ensure it's a Decimal

    logger.info(f"Received request to close order {order_id} for user {current_user.id} with close price {close_price}.")

    # 1. Validate and Fetch Order
    db_order = await crud_order.get_order_by_id(db, order_id=order_id)
    if db_order is None:
        logger.warning(f"Attempted to close non-existent order: {order_id}")
        raise HTTPException(status_code=404, detail="Order not found.")
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
        logger.warning(f"User {current_user.id} attempted to close order {order_id} belonging to user {db_order.order_user_id}.")
        raise HTTPException(status_code=403, detail="Not authorized to close this order.")
    if db_order.order_status != 'OPEN':
        logger.warning(f"Attempted to close order {order_id} with status '{db_order.order_status}'.")
        raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

    order_symbol = db_order.order_company_name.upper()
    order_quantity = Decimal(str(db_order.order_quantity))
    order_price = Decimal(str(db_order.order_price)) # Entry price
    order_type = db_order.order_type.upper()
    user_group_name = getattr(current_user, 'group_name', 'default') # Get user's group name


    # 2. Lock User Account (to update margin and balance)
    # Use current_user.id to fetch and lock the authenticated user
    db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
    if db_user_locked is None:
        logger.error(f"Could not retrieve user {current_user.id} with lock during order closing.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Could not retrieve user data securely for financial update.")

    # --- Margin Recalculation Logic (Already present) ---
    # Recalculate the margin for that symbol BEFORE closure
    all_open_orders_for_symbol_before_close = await crud_order.get_open_orders_by_user_id_and_symbol(
        db=db,
        user_id=current_user.id, # Use current_user.id
        symbol=order_symbol
    )

    margin_attributed_to_symbol_before_new_order = await calculate_total_symbol_margin_contribution( # Renamed variable for clarity
        db=db,
        redis_client=redis_client,
        user_id=current_user.id, # Use current_user.id
        symbol=order_symbol,
        open_positions_for_symbol=all_open_orders_for_symbol_before_close
    )
    logger.debug(f"User {current_user.id}, Symbol {order_symbol}: Margin attributed to symbol BEFORE close: {margin_attributed_to_symbol_before_new_order}")

    # Find the difference between the user overall margin and margin for that symbol
    user_overall_margin_before_update = Decimal(str(db_user_locked.margin))
    non_symbol_margin = user_overall_margin_before_update - margin_attributed_to_symbol_before_new_order
    logger.debug(f"User {current_user.id}: Non-symbol margin portion calculated: {non_symbol_margin}")

    # Calculate the margin for that symbol AFTER closure
    remaining_open_positions_for_symbol = [
        p for p in all_open_orders_for_symbol_before_close if p.order_id != db_order.order_id
    ]

    margin_attributed_to_symbol_after_close = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=current_user.id, # Use current_user.id
        symbol=order_symbol,
        open_positions_for_symbol=remaining_open_positions_for_symbol
    )
    logger.debug(f"User {current_user.id}, Symbol {order_symbol}: Margin attributed to symbol AFTER close: {margin_attributed_to_symbol_after_close}")

    # Add that margin to the non_symbol_margin and store that value to user overall margin
    new_user_overall_margin = non_symbol_margin + margin_attributed_to_symbol_after_close
    db_user_locked.margin = max(Decimal(0), new_user_overall_margin) # Ensure overall margin doesn't go below zero
    logger.info(f"User {current_user.id}: Overall margin updated from {user_overall_margin_before_update} to {db_user_locked.margin}.")
    # --- End: Margin Recalculation Logic ---


    # --- Start: Calculate Close Price, Contract Value (for PnL), and Net Profit ---

    # 3. Use the close_price from the request body (already done above)
    if close_price is None or close_price <= 0:
        logger.error(f"Invalid close price {close_price} provided in request for order {order_id}.")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid close price provided.")
    logger.debug(f"Using close price from request for order {order_id}: {close_price}")

    # 4. Calculate contract_value for PnL calculation purposes (using user's specified formula)
    # Need contract_size from ExternalSymbolInfo
    symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
    symbol_info_result = await db.execute(symbol_info_stmt)
    ext_symbol_info = symbol_info_result.scalars().first()

    if not ext_symbol_info or not ext_symbol_info.contract_size or ext_symbol_info.contract_size <= 0:
        logger.error(f"ExternalSymbolInfo or valid contract_size not found for symbol: {order_symbol} to close order {order_id}.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Could not retrieve contract size for symbol {order_symbol}.")

    contract_size = ext_symbol_info.contract_size
    # Calculate contract value for use in PnL/commission calculations
    calculated_contract_value_for_pnl = order_quantity * contract_size
    logger.debug(f"Calculated contract value for PnL calculation for order {order_id}: {calculated_contract_value_for_pnl}")


    # 5. Calculate net_profit
    # This involves raw PnL, currency conversion, and commission.
    profit_currency = getattr(ext_symbol_info, 'profit', 'USD').upper() # Get profit currency from ExternalSymbolInfo
    logger.debug(f"Profit currency for symbol {order_symbol}: {profit_currency}")

    # Calculate raw PnL in profit currency using the PROVIDED close_price and the calculated contract value
    raw_pnl_native: Decimal
    if order_type == "BUY":
        raw_pnl_native = (close_price - order_price) * order_quantity * contract_size # Use contract_size directly for PnL
    elif order_type == "SELL":
        raw_pnl_native = (order_price - close_price) * order_quantity * contract_size # Use contract_size directly for PnL
    else:
        logger.error(f"Invalid order type '{order_type}' for order {order_id} during PnL calculation.")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Invalid order type for PnL calculation.")
    logger.debug(f"Raw PnL in native currency ({profit_currency}) for order {order_id}: {raw_pnl_native}")

    # Convert raw PnL to USD
    # Need adjusted_market_prices for conversion rates. Fetch from cache.
    # We need prices for conversion pairs (e.g., EURUSD, USDJPY) for the user's group.
    # The broadcaster task caches these. We can fetch the 'ALL' group settings to get relevant symbols,
    # then fetch their adjusted prices from cache.
    user_group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, "ALL")
    adjusted_market_prices_for_conversion: Dict[str, Dict[str, Decimal]] = {} # Corrected type hint to Decimal

    if isinstance(user_group_settings, dict):
        for symbol_key in user_group_settings.keys():
            cached_price = await get_adjusted_market_price_cache(redis_client, user_group_name, symbol_key)
            if cached_price:
                 # Ensure cached prices are Decimal for calculations
                 buy_price_dec = Decimal(str(cached_price.get('buy', 0.0)))
                 sell_price_dec = Decimal(str(cached_price.get('sell', 0.0)))
                 spread_value_dec = Decimal(str(cached_price.get('spread_value', 0.0)))

                 adjusted_market_prices_for_conversion[symbol_key] = {
                     'buy': buy_price_dec,
                     'sell': sell_price_dec,
                     'spread_value': spread_value_dec,
                 }
    logger.debug(f"Fetched {len(adjusted_market_prices_for_conversion)} adjusted market prices for conversion.")


    pnl_in_usd = await _convert_to_usd(
        amount=raw_pnl_native,
        from_currency=profit_currency,
        adjusted_market_prices=adjusted_market_prices_for_conversion,
        user_id=current_user.id, # Use current_user.id
        position_id=db_order.order_id,
        value_description="Closed Order PnL"
    )
    logger.debug(f"PnL converted to USD for order {order_id}: {pnl_in_usd}")

    # Calculate Commission for the closed order
    # Need group settings for commission type and rate for this symbol
    # Fetch specific group settings for this symbol
    group_symbol_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
    commission_usd = Decimal("0.0")

    if group_symbol_settings:
        commission_type_setting = group_symbol_settings.get('commision_type')
        commission_value_type_setting = group_symbol_settings.get('commision_value_type')
        commission_rate_setting = Decimal(str(group_symbol_settings.get('commision', 0.0)))
        # Use contract_size from ExternalSymbolInfo for commission calculation if available
        # Fallback to group setting or default if not
        commission_contract_size = contract_size if contract_size > Decimal(0) else Decimal(str(group_symbol_settings.get('contract_size', Decimal("1.0"))))


        # Apply commission if type is 'Every Trade' (0) or 'Out' (2)
        if commission_type_setting in [0, 2]:
             if commission_value_type_setting == 0: # Total Value (per lot)
                 commission_usd = order_quantity * commission_rate_setting
             elif commission_value_type_setting == 1: # Percent
                 # Commission percentage is typically applied to the contract value at entry or close.
                 # Let's apply it to the contract value at close (quantity * commission_contract_size * close_price)
                 # Use the calculated contract value for PnL which uses the correct contract_size
                 contract_value_for_commission_calc = order_quantity * commission_contract_size * close_price
                 if contract_value_for_commission_calc > Decimal("0.0"):
                      commission_usd = (commission_rate_setting / Decimal("100")) * contract_value_for_commission_calc
                 else:
                      logger.warning(f"User {current_user.id}, Order {order_id}: Contract value for commission calculation is zero, cannot calculate percentage commission.")
                      commission_usd = Decimal("0.0") # Set commission to 0 if calculation base is zero
             else:
                 logger.warning(f"User {current_user.id}, Order {order_id}: Unknown commission_value_type '{commission_value_type_setting}'. Commission set to 0.")
                 commission_usd = Decimal("0.0") # Set commission to 0 for unknown type
        else:
             logger.debug(f"User {current_user.id}, Order {order_id}: Commission Type '{commission_type_setting}' not applied on close.")
             commission_usd = Decimal("0.0") # Set commission to 0 for other types

        # Ensure commission is non-negative
        commission_usd = max(Decimal(0), commission_usd)
        logger.debug(f"Calculated commission (USD) for order {order_id}: {commission_usd}")

    else:
        logger.warning(f"No group symbol settings found for group '{user_group_name}', symbol '{order_symbol}'. Commission set to 0 for order {order_id}.")
        commission_usd = Decimal("0.0") # Set commission to 0 if no settings found


    # Calculate net_profit (PnL in USD - Commission in USD)
    calculated_net_profit = pnl_in_usd - commission_usd
    logger.info(f"Calculated net profit for order {order_id}: {calculated_net_profit} (PnL: {pnl_in_usd}, Commission: {commission_usd})")

    # Quantize the results for storage
    usd_precision = Decimal('0.00000001') # 8 decimal places for USD
    calculated_net_profit = calculated_net_profit.quantize(usd_precision, rounding=ROUND_HALF_UP)

    # --- IMPROVED HANDLING FOR SYMBOL DIGIT AND PRICE QUANTIZATION ---
    symbol_digits_int = 5 # Default value
    try:
        # Try to get the digit attribute and convert it to an integer
        raw_digits = getattr(ext_symbol_info, 'digit', 5)
        if raw_digits is not None:
            symbol_digits_int = int(raw_digits)
        else:
            logger.warning(f"Symbol digits is None for symbol {order_symbol}. Using default precision 5.")
    except (ValueError, TypeError) as e:
        logger.error(f"Could not convert symbol digit '{getattr(ext_symbol_info, 'digit', 'N/A')}' to integer for symbol {order_symbol}: {e}. Using default precision 5.", exc_info=True)
        symbol_digits_int = 5 # Fallback to default if conversion fails

    # Ensure digits is non-negative for quantization
    if symbol_digits_int < 0:
        logger.warning(f"Calculated symbol digits ({symbol_digits_int}) is negative for symbol {order_symbol}. Using default precision 5.")
        symbol_digits_int = 5

    # Construct the precision string using the validated integer
    price_precision_str = '1e-' + str(symbol_digits_int)
    try:
        price_precision = Decimal(price_precision_str)
    except InvalidOperation as e:
         logger.error(f"Failed to create Decimal precision from string '{price_precision_str}' for symbol {order_symbol}: {e}. Using default USD precision.", exc_info=True)
         price_precision = usd_precision # Fallback to USD precision if symbol precision fails

    # Now quantize the close price using the safely created precision Decimal
    close_price_quantized = close_price.quantize(price_precision, rounding=ROUND_HALF_UP)
    # --- END IMPROVED HANDLING ---

    # Contract value is NOT updated on close, so no need to quantize and assign it here
    # calculated_contract_value is only used for commission calculation base if needed.


    # --- End: Calculate Close Price, Contract Value (for PnL), and Net Profit ---


    # 6. Update UserOrder record
    # Directly update the ORM object within the locked session
    db_order.order_status = 'CLOSED'
    db_order.close_price = close_price_quantized # Use the quantized close price
    db_order.net_profit = calculated_net_profit
    db_order.commission = commission_usd
    # REMOVED: db_order.contract_value = calculated_contract_value # Contract value is set on placement
    db_order.close_message = 'Order Closed'
    # Note: Original order_price, order_quantity, margin remain unchanged.

    # 7. Update User's Wallet Balance
    user_wallet_balance_before_update = Decimal(str(db_user_locked.wallet_balance))
    db_user_locked.wallet_balance += calculated_net_profit
    logger.info(f"User {current_user.id}: Wallet balance updated from {user_wallet_balance_before_update} to {db_user_locked.wallet_balance} by adding net profit {calculated_net_profit}.")


    # 8. Create Wallet Transaction Record
    # Determine transaction type based on net profit
    transaction_type = 'trade_profit' if calculated_net_profit >= 0 else 'trade_loss'
    transaction_amount = abs(calculated_net_profit) # Store absolute value

    # --- IMPROVED WALLET TRANSACTION CREATION ---
    # Generate unique transaction ID directly for the ORM object
    transaction_id = str(uuid.uuid4())
    logger.debug(f"Generated transaction ID for Wallet: {transaction_id}")

    # Create the Wallet ORM object directly, assigning the generated ID and other fields
    new_wallet_transaction = Wallet(
        user_id=current_user.id,
        symbol=order_symbol,
        order_quantity=order_quantity,
        transaction_type=transaction_type,
        is_approved=1, # Assuming trade PnL is automatically approved
        order_type=order_type,
        transaction_amount=transaction_amount, # Use the calculated absolute amount
        description=f"Order Closed: {db_order.order_id}",
        transaction_id=transaction_id, # Assign the generated ID directly
        transaction_time=datetime.datetime.now() # Manually set time
        # created_at and updated_at should be handled by DB defaults
    )

    db.add(new_wallet_transaction)
    logger.info(f"Added wallet transaction {transaction_id} for user {current_user.id}, amount {transaction_amount}, type {transaction_type} to session.")
    # --- END IMPROVED WALLET TRANSACTION CREATION ---


    # 9. Commit the database transaction
    try:
        await db.commit()
        await db.refresh(db_user_locked) # Refresh user object to reflect updated balance and margin
        await db.refresh(db_order) # Refresh order object to reflect updated status and values
        await db.refresh(new_wallet_transaction) # Refresh wallet transaction object

        logger.info(f"Order {order_id} successfully closed, user {current_user.id}'s financials updated, and wallet transaction recorded.")

        # TODO: Publish order closure event to a queue (for audit, historical data, etc.).
        # TODO: Update user's portfolio cache in Redis and trigger WebSocket broadcast.
        # This would involve updating the cached user portfolio data (removing the closed position)
        # and potentially publishing an update to the user's specific WebSocket channel.

        return OrderResponse.model_validate(db_order)

    except Exception as e:
        await db.rollback()
        logger.error(f"Database error during order close commit for order {order_id}, user {current_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Database error during order closing.")