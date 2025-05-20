# app/services/order_processing.py

import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP # Import ROUND_HALF_UP for quantization
from typing import Optional, Dict, Any, List
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import uuid # Import uuid

# Import necessary components
from app.database.models import User, UserOrder
from app.schemas.order import OrderPlacementRequest, OrderCreateInternal
# Import updated crud_order and user crud
from app.crud import crud_order
from app.crud import user as crud_user
# Import the margin calculator service and its helper
from app.services.margin_calculator import calculate_single_order_margin

logger = logging.getLogger(__name__)

# Define custom exceptions for the service
class OrderProcessingError(Exception):
    """Custom exception for errors during order processing."""
    pass

class InsufficientFundsError(Exception):
    """Custom exception for insufficient funds during order placement."""
    pass

# UPDATED HELPER FUNCTION
async def calculate_total_symbol_margin_contribution(
    db: AsyncSession,
    redis_client: Redis,
    user_id: int,
    symbol: str,
    open_positions_for_symbol: List[UserOrder]
) -> Decimal:
    """
    Calculates the total margin attributed to a specific symbol for a user,
    considering the "dominant side" hedging logic (highest margin per max of buy/sell quantity).
    This function processes a list of *existing* open positions for a given symbol.
    """
    total_buy_quantity = Decimal(0)
    total_sell_quantity = Decimal(0)
    all_margins_per_lot: List[Decimal] = []

    for position in open_positions_for_symbol:
        position_quantity = Decimal(str(position.order_quantity))
        position_type = position.order_type.upper()
        position_full_margin = Decimal(str(position.margin)) # Full non-hedged margin of the individual order

        if position_quantity > 0:
            margin_per_lot_of_position = position_full_margin / position_quantity
            all_margins_per_lot.append(margin_per_lot_of_position)
        else:
            logger.warning(f"Position {position.order_id} for user {user_id} has zero quantity. Skipping for margin calculation in total symbol margin.")

        if position_type == 'BUY':
            total_buy_quantity += position_quantity
        elif position_type == 'SELL':
            total_sell_quantity += position_quantity

    # --- CHANGE HERE: Use max(buy_qty, sell_qty) for net_quantity ---
    net_quantity = max(total_buy_quantity, total_sell_quantity)

    highest_margin_per_lot = Decimal(0)
    if all_margins_per_lot:
        highest_margin_per_lot = max(all_margins_per_lot)

    calculated_symbol_margin = highest_margin_per_lot * net_quantity
    return calculated_symbol_margin.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP) # Quantize for consistency


# MAIN PROCESSING FUNCTION FOR NEW ORDER (remains the same in logic, but will use updated helper)
async def process_new_order(
    db: AsyncSession,
    redis_client: Redis,
    user: User, # Pass the user ORM object
    order_request: OrderPlacementRequest
) -> UserOrder:
    """
    Processes a new order request, calculates the margin to add to the user's total margin
    based on the "Highest Margin per Net Quantity Increase" hedging model,
    updates user's overall margin, and creates the order in the database.
    Uses database locking for user financial data.
    """
    logger.info(f"Processing new order for user {user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

    new_order_quantity = Decimal(str(order_request.order_quantity))
    new_order_type = order_request.order_type.upper()
    order_symbol = order_request.symbol.upper() # Use upper for consistency


    # --- Step 1: Calculate the full, non-hedged margin and contract value for the new order ---
    full_calculated_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        order_quantity=new_order_quantity,
        order_price=order_request.order_price,
        symbol=order_symbol,
        order_type=new_order_type
    )

    if full_calculated_margin_usd is None or adjusted_order_price is None or contract_value is None:
         logger.error(f"Failed to calculate full order margin, adjusted price, or contract value for user {user.id}, symbol {order_symbol}.")
         raise OrderProcessingError("Failed to calculate order details.")

    logger.debug(f"Calculated full order margin (non-hedged, for order record): {full_calculated_margin_usd}, Adjusted price: {adjusted_order_price}, Contract value: {contract_value}")

    # Ensure new order quantity is positive before calculating margin per lot for it
    if new_order_quantity <= 0:
        logger.error(f"New order quantity is zero or negative for user {user.id}, symbol {order_symbol}.")
        raise OrderProcessingError("Invalid order quantity.")

    # --- Step 2: Calculate user's *current total margin* (before new order) for this symbol ---
    # Fetch ALL open orders for the user for this symbol *before* adding the new one
    existing_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
        db=db,
        user_id=user.id,
        symbol=order_symbol
    )
    margin_attributed_to_symbol_before_new_order = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        symbol=order_symbol,
        open_positions_for_symbol=existing_open_orders_for_symbol
    )
    logger.debug(f"User {user.id}, Symbol {order_symbol}: Margin attributed to symbol BEFORE new order: {margin_attributed_to_symbol_before_new_order}")


    # --- Step 3: Simulate user's *new total margin* (after new order) for this symbol ---
    # Create a dummy order object for the new order to include it in the calculation
    # Only need relevant fields for margin calculation: order_quantity, order_type, margin (full, non-hedged)
    new_order_dummy_for_calc = UserOrder(
        order_quantity=new_order_quantity,
        order_type=new_order_type,
        margin=full_calculated_margin_usd # Use the full calculated margin
    )
    # Combine existing orders with the new dummy order for the calculation
    all_open_orders_for_symbol_after_new_order = existing_open_orders_for_symbol + [new_order_dummy_for_calc]

    margin_attributed_to_symbol_after_new_order = await calculate_total_symbol_margin_contribution(
        db=db,
        redis_client=redis_client,
        user_id=user.id,
        symbol=order_symbol,
        open_positions_for_symbol=all_open_orders_for_symbol_after_new_order
    )
    logger.debug(f"User {user.id}, Symbol {order_symbol}: Margin attributed to symbol AFTER new order: {margin_attributed_to_symbol_after_new_order}")


    # --- Step 4: Calculate the ADDITIONAL margin required for this NEW order based on hedging ---
    # This is the increase in margin attributed to this symbol due to the new order
    additional_margin_required = margin_attributed_to_symbol_after_new_order - margin_attributed_to_symbol_before_new_order
    # If the net quantity decreases or remains the same but highest margin per lot decreases,
    # this could be negative or zero, meaning no *additional* margin is required.
    additional_margin_required = max(Decimal(0), additional_margin_required) # Ensure it's not negative
    logger.info(f"Calculated additional margin required (based on total symbol margin change): {additional_margin_required}")


    # --- Step 5: Fetch user with lock and perform margin check and update ---
    db_user_locked = await crud_user.get_user_by_id_with_lock(db, user.id)

    if db_user_locked is None:
        logger.error(f"Could not retrieve user {user.id} with lock during order placement processing.")
        raise OrderProcessingError("Could not retrieve user data securely.")

    user_overall_margin_before = Decimal(str(db_user_locked.margin))
    user_wallet_balance = Decimal(str(db_user_locked.wallet_balance))

    available_free_margin = user_wallet_balance - user_overall_margin_before

    logger.debug(f"User {user.id}: Wallet Balance (from DB): {user_wallet_balance}, Overall Margin BEFORE update (from DB): {user_overall_margin_before}, Available Free Margin: {available_free_margin}")
    logger.debug(f"Additional margin to ADD to user's overall margin: {additional_margin_required}")

    if available_free_margin < additional_margin_required:
        logger.warning(f"Insufficient funds for user {user.id} to place order. Additional margin required: {additional_margin_required}, Available free margin: {available_free_margin}")
        additional_margin_required_display = additional_margin_required.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        available_free_margin_display = available_free_margin.quantize(Decimal('0.00000001'), rounding=ROUND_HALF_UP)
        raise InsufficientFundsError(f"Insufficient funds. Required additional margin: {additional_margin_required_display:.8f} USD. Available free margin: {available_free_margin_display:.8f} USD.")

    db_user_locked.margin += additional_margin_required
    logger.info(f"User {user.id}: Updated overall margin to {db_user_locked.margin} by adding {additional_margin_required}.")

    # --- Step 6: Prepare data for creating the UserOrder record and save ---
    unique_order_id = str(uuid.uuid4())
    order_status = "OPEN"

    order_data_internal = OrderCreateInternal(
        order_id=unique_order_id, # Generate new UUID for the order being placed
        order_status=order_status,
        order_user_id=user.id,
        order_company_name=order_symbol,
        order_type=new_order_type,
        order_price=adjusted_order_price,
        order_quantity=new_order_quantity,
        contract_value=contract_value,
        margin=full_calculated_margin_usd, # Store the FULL, non-hedged margin
        stop_loss=order_request.stop_loss,
        take_profit=order_request.take_profit,
        net_profit=None, close_price=None, swap=None, commission=None, cancel_message=None, close_message=None, status=1
    )

    new_db_order = UserOrder(**order_data_internal.model_dump())
    db.add(new_db_order)

    try:
        await db.commit()
        await db.refresh(db_user_locked)
        await db.refresh(new_db_order)

        logger.info(f"Order {new_db_order.order_id} placed successfully for user ID {user.id}. Full order margin: {full_calculated_margin_usd} USD. User overall margin updated to {db_user_locked.margin}.")

        return new_db_order

    except Exception as e:
        await db.rollback()
        logger.error(f"Database error during order placement commit for user ID {user.id}: {e}", exc_info=True)
        raise OrderProcessingError("Database error during order placement.")