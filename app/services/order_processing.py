# app/services/order_processing.py

import logging
from decimal import Decimal
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import uuid # Import uuid

# Import necessary components
from app.database.models import User, UserOrder
from app.schemas.order import OrderPlacementRequest, OrderCreateInternal
# Import updated crud_order and user crud
from app.crud import crud_order
from app.crud import user as crud_user # Alias user crud
from app.core.cache import get_user_positions_from_cache, get_adjusted_market_price_cache, get_user_data_cache, get_group_symbol_settings_cache
# Import the margin calculator service
from app.services.margin_calculator import calculate_single_order_margin # Keep this import

logger = logging.getLogger(__name__)

# Helper function to calculate margin per lot (STAYS HERE for hedging calculation needs)
# This calculates margin per lot based on user leverage, group margin setting, and a given price.
# It's used to compare margin per lot for the new order and opposing positions for hedging.
# CORRECTED: Accepts user_id instead of the full user object
async def calculate_margin_per_lot(
    redis_client: Redis,
    user_id: int, # Accept user_id
    symbol: str,
    order_type: str,
    price: Decimal # Price at which margin per lot is calculated (order price or current market price)
) -> Optional[Decimal]:
    """
    Calculates the margin required per standard lot (e.g., 1 lot) for a given symbol, order type, and price,
    considering user's group settings and leverage.
    Returns the margin per lot in USD or None if calculation fails.
    """
    # Retrieve user data from cache to get group_name and leverage
    user_data = await get_user_data_cache(redis_client, user_id)
    if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
        logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
        return None

    group_name = user_data['group_name']
    # Ensure user_leverage is Decimal
    user_leverage_raw = user_data.get('leverage', 1)
    user_leverage = Decimal(str(user_leverage_raw)) if user_leverage_raw is not None else Decimal(1)


    # Retrieve group-symbol settings from cache
    # Need settings for the specific symbol
    group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
    if not group_symbol_settings or 'margin' not in group_symbol_settings:
        logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
        return None

    # Ensure margin_setting is Decimal
    margin_setting_raw = group_symbol_settings.get('margin', 0)
    margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


    if user_leverage <= 0:
         logger.error(f"User leverage is zero or negative for user {user_id}.")
         return None

    # Assuming margin calculation formula is (Margin_Setting * Price) / Leverage
    # Price used here should be the contract value price (e.g., market price bid for sell, ask for buy)
    # For margin calculation, the price used is typically the contract value price.
    # Let's use the provided 'price' parameter which could be order price or current market price.
    try:
        # Ensure price is Decimal
        price_decimal = Decimal(str(price))
        margin_per_lot_usd = (margin_setting * price_decimal) / user_leverage
        logger.debug(f"Calculated margin per lot for user {user_id}, symbol {symbol}, type {order_type}, price {price}: {margin_per_lot_usd} USD")
        return margin_per_lot_usd
    except Exception as e:
        logger.error(f"Error calculating margin per lot for user {user_id}, symbol {symbol}: {e}", exc_info=True)
        return None


async def process_new_order(
    db: AsyncSession,
    redis_client: Redis,
    user: User, # Pass the user ORM object
    order_request: OrderPlacementRequest
) -> UserOrder:
    """
    Processes a new order request, calculates margin with hedging logic,
    updates user's overall margin, and creates the order in the database.
    Uses database locking for user financial data.
    """
    logger.info(f"Processing new order for user {user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

    new_order_quantity = Decimal(str(order_request.order_quantity))
    new_order_type = order_request.order_type.upper()
    order_symbol = order_request.symbol.upper() # Use upper for consistency


    # --- Step 1: Calculate the full, non-hedged margin and contract value for the new order ---
    # Use the calculate_single_order_margin from the margin_calculator service
    full_calculated_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
        db=db, # Pass db if needed by calculate_single_order_margin (it is)
        redis_client=redis_client, # Pass redis_client
        user_id=user.id,
        order_quantity=new_order_quantity,
        order_price=order_request.order_price,
        symbol=order_symbol,
        order_type=new_order_type
    )

    if full_calculated_margin_usd is None or adjusted_order_price is None or contract_value is None:
         logger.error(f"Failed to calculate full order margin, adjusted price, or contract value for user {user.id}, symbol {order_symbol}.")
         raise OrderProcessingError("Failed to calculate order details.")

    logger.debug(f"Calculated full order margin (non-hedged): {full_calculated_margin_usd}, Adjusted price: {adjusted_order_price}, Contract value: {contract_value}")


    # --- Step 2: Fetch user's existing open positions for the same symbol (efficiently from DB) ---
    # Use the new CRUD function to get only open orders for this user and symbol
    existing_positions = await crud_order.get_open_orders_by_user_id_and_symbol(
        db=db,
        user_id=user.id,
        symbol=order_symbol # Pass the symbol
    )

    total_existing_buy_quantity = Decimal(0)
    total_existing_sell_quantity = Decimal(0)

    for position in existing_positions:
        position_quantity = Decimal(str(position.order_quantity))
        position_type = position.order_type.upper()

        if position_type == 'BUY':
            total_existing_buy_quantity += position_quantity
        elif position_type == 'SELL':
            total_existing_sell_quantity += position_quantity

    logger.debug(f"User {user.id}, symbol {order_symbol}: Existing BUY quantity: {total_existing_buy_quantity}, Existing SELL quantity: {total_existing_sell_quantity}")


    # --- Step 3: Determine the opposing quantity and calculate hedged/unhedged quantities ---
    opposing_quantity = Decimal(0)
    if new_order_type == 'BUY':
        opposing_quantity = total_existing_sell_quantity
    elif new_order_type == 'SELL':
        opposing_quantity = total_existing_buy_quantity

    hedged_quantity = min(new_order_quantity, opposing_quantity)
    unhedged_quantity = new_order_quantity - hedged_quantity

    logger.debug(f"New order quantity: {new_order_quantity}, Opposing quantity: {opposing_quantity}")
    logger.debug(f"Hedged quantity: {hedged_quantity}, Unhedged quantity: {unhedged_quantity}")


    # --- Step 4: Calculate margin per lot for the new order (at order price) and opposing type (at current market price) ---
    # Use the local calculate_margin_per_lot helper for this specific need in hedging.

    # Margin per lot for the NEW order type at the ORDER price
    margin_per_lot_new_order = await calculate_margin_per_lot(
        redis_client=redis_client,
        user_id=user.id, # Pass user.id
        symbol=order_symbol,
        order_type=new_order_type,
        price=order_request.order_price # Use the order price
    )

    if margin_per_lot_new_order is None:
         logger.error(f"Failed to calculate margin per lot for new order ({new_order_type}) at order price for user {user.id}, symbol {order_symbol}.")
         raise OrderProcessingError("Failed to calculate margin per lot for hedging.")


    # Need current market price for the opposing type for the opposing margin per lot calculation
    current_market_prices = await get_adjusted_market_price_cache(redis_client, user.group_name, order_symbol)

    margin_per_lot_opposing = Decimal(0) # Initialize to 0

    if current_market_prices:
        opposing_price_type = 'sell' if new_order_type == 'BUY' else 'buy' # If placing BUY, opposing is SELL (use SELL price)
        current_opposing_price = current_market_prices.get(opposing_price_type)

        if current_opposing_price is not None:
             # Calculate margin per lot for the opposing type at the current market price
             margin_per_lot_opposing = await calculate_margin_per_lot(
                 redis_client=redis_client,
                 user_id=user.id, # Pass user.id
                 symbol=order_symbol,
                 order_type='SELL' if new_order_type == 'BUY' else 'BUY', # Opposing type
                 price=Decimal(str(current_opposing_price)) # Use current market price
             )
             if margin_per_lot_opposing is None:
                  margin_per_lot_opposing = Decimal(0) # Default to 0 if calculation fails

    logger.debug(f"Margin per lot new order ({new_order_type}): {margin_per_lot_new_order}")
    logger.debug(f"Margin per lot opposing order ({'SELL' if new_order_type == 'BUY' else 'BUY'}): {margin_per_lot_opposing}")


    # --- Step 5: Determine the higher margin per lot and calculate total margin required for this NEW order considering hedging ---
    # This is the *additional* margin that needs to be added to the user's overall margin.

    if hedged_quantity == Decimal(0):
        # If there is no hedging, the additional margin required is simply the full margin of the new order.
        additional_margin_required = full_calculated_margin_usd
        logger.debug(f"No hedging detected. Additional margin required = full_calculated_margin_usd: {additional_margin_required}")
    else:
        # If there is hedging, calculate the contribution from hedged and unhedged portions.
        higher_margin_per_lot = max(margin_per_lot_new_order, margin_per_lot_opposing)

        hedged_margin_contribution = hedged_quantity * higher_margin_per_lot
        unhedged_margin_contribution = unhedged_quantity * margin_per_lot_new_order # Unhedged portion uses its own margin per lot at order price

        additional_margin_required = hedged_margin_contribution + unhedged_margin_contribution
        logger.debug(f"Hedging detected. Higher margin per lot: {higher_margin_per_lot}")
        logger.debug(f"Hedged margin contribution: {hedged_margin_contribution}")
        logger.debug(f"Unhedged margin contribution: {unhedged_margin_contribution}")
        logger.debug(f"Additional margin required (considering hedging): {additional_margin_required}")


    # --- Step 6: Fetch user with lock and perform margin check and update ---
    # Use the get_user_by_id_with_lock function to prevent race conditions
    # This fetch starts a transaction implicitly depending on SQLAlchemy configuration,
    # but explicit begin/commit/rollback is safer if needed.
    # For now, relying on the session's transaction management with the lock.
    db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_id=user.id)

    if db_user_locked is None:
        # This should ideally not happen if the user exists, but as a safeguard
        logger.error(f"Could not retrieve user {user.id} with lock during order placement processing.")
        raise OrderProcessingError("Could not retrieve user data securely.")

    # Ensure wallet_balance and margin are Decimal
    user_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
    # Use the existing 'margin' attribute for the user's overall margin
    user_overall_margin = Decimal(str(db_user_locked.margin))

    available_free_margin = user_wallet_balance - user_overall_margin

    logger.debug(f"User {user.id}: Wallet Balance: {user_wallet_balance}, Overall Margin: {user_overall_margin}, Available Free Margin: {available_free_margin}")


    # Check if user has sufficient free margin for the additional margin required by the new order
    if available_free_margin < additional_margin_required:
        logger.warning(f"Insufficient funds for user {user.id} to place order. Additional margin required (hedged): {additional_margin_required}, Available free margin: {available_free_margin}")
        # Raise a specific exception that the endpoint can catch and translate to HTTP 400
        raise InsufficientFundsError(f"Insufficient funds. Required additional margin: {additional_margin_required:.2f} USD.")

    # Update the user's total overall margin by adding the additional margin required for the new order
    db_user_locked.margin += additional_margin_required
    logger.info(f"User {user.id}: Updated overall margin to {db_user_locked.margin} after placing order (considering hedging).")


    # --- Step 7: Prepare data for creating the UserOrder record and save ---
    # The margin stored in the order table is the FULL, non-hedged margin
    unique_order_id = str(uuid.uuid4())
    order_status = "OPEN" # Or "PENDING_EXECUTION" depending on your flow

    order_data_internal = OrderCreateInternal(
        order_id=unique_order_id,
        order_status=order_status,
        order_user_id=user.id,
        order_company_name=order_symbol, # Storing symbol as company_name
        order_type=new_order_type,
        order_price=adjusted_order_price, # Use the adjusted price from calculate_single_order_margin
        order_quantity=new_order_quantity,
        contract_value=contract_value, # Use the contract value from calculate_single_order_margin
        margin=full_calculated_margin_usd, # Store the FULL, non-hedged margin from calculate_single_order_margin
        stop_loss=order_request.stop_loss,
        take_profit=order_request.take_profit,
        # Initialize other fields as needed
        net_profit=None,
        close_price=None,
        swap=None, # Swap might be calculated later or at end of day
        commission=None, # Commission might be calculated here or upon closing
        status=1 # Default status for an open order
    )

    # Create the new order ORM object
    new_db_order = UserOrder(**order_data_internal.model_dump())

    # Add the updated user object and the new order object to the session
    # The db_user_locked object is already tracked by the session due to the lock fetch
    db.add(new_db_order)

    try:
        # Commit the transaction. This saves the updated user and the new order.
        await db.commit()

        # Refresh objects to get database-generated fields (like id, created_at)
        await db.refresh(db_user_locked) # Refresh the user object to get latest state if needed later
        await db.refresh(new_db_order)

        logger.info(f"Order {new_db_order.order_id} placed successfully for user ID {user.id}. Full order margin: {full_calculated_margin_usd} USD. User overall margin updated to {db_user_locked.margin}.")

        # TODO: Post-order creation actions:
        # 1. Publish order event to a queue for further processing (e.g., matching engine, audit log)
        # 2. Update user's portfolio cache in Redis with the new position and updated margin/equity.
        #    This cache update should reflect the *new total* overall margin and the new position.
        #    This update should also trigger a WebSocket message to the user via the broadcaster.

        return new_db_order # Return the ORM object

    except Exception as e:
        # Any database error during commit will be caught here
        logger.error(f"Database error during order placement commit for user ID {user.id}: {e}", exc_info=True)
        # Re-raise a specific exception
        raise OrderProcessingError("Database error during order placement.")


# Define custom exceptions for the service
class OrderProcessingError(Exception):
    """Custom exception for errors during order processing."""
    pass

class InsufficientFundsError(OrderProcessingError):
    """Custom exception for insufficient funds during order placement."""
    pass
