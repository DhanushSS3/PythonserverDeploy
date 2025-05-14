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
# Import the margin calculator service and its helper
from app.services.margin_calculator import calculate_single_order_margin, calculate_base_margin_per_lot # Import helper too

logger = logging.getLogger(__name__)

# Remove the duplicate calculate_margin_per_lot helper from here.
# We will use calculate_base_margin_per_lot from margin_calculator.py


# async def process_new_order(
#     db: AsyncSession,
#     redis_client: Redis,
#     user: User, # Pass the user ORM object
#     order_request: OrderPlacementRequest
# ) -> UserOrder:
#     """
#     Processes a new order request, calculates margin with hedging logic based on net positions,
#     updates user's overall margin, and creates the order in the database.
#     Uses database locking for user financial data.
#     """
#     logger.info(f"Processing new order for user {user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

#     new_order_quantity = Decimal(str(order_request.order_quantity))
#     new_order_type = order_request.order_type.upper()
#     order_symbol = order_request.symbol.upper() # Use upper for consistency


#     # --- Step 1: Calculate the full, non-hedged margin and contract value for the new order ---
#     # Use the calculate_single_order_margin from the margin_calculator service
#     # This returns the margin that will be stored with the individual order.
#     full_calculated_margin_usd, adjusted_order_price, contract_value = await calculate_single_order_margin(
#         db=db, # Pass db if needed by calculate_single_order_margin (it is)
#         redis_client=redis_client, # Pass redis_client
#         user_id=user.id,
#         order_quantity=new_order_quantity,
#         order_price=order_request.order_price,
#         symbol=order_symbol,
#         order_type=new_order_type
#     )

#     if full_calculated_margin_usd is None or adjusted_order_price is None or contract_value is None:
#          logger.error(f"Failed to calculate full order margin, adjusted price, or contract value for user {user.id}, symbol {order_symbol}.")
#          raise OrderProcessingError("Failed to calculate order details.")

#     logger.debug(f"Calculated full order margin (non-hedged, for order record): {full_calculated_margin_usd}, Adjusted price: {adjusted_order_price}, Contract value: {contract_value}")


#     # --- Step 2: Fetch user's existing open positions for the same symbol (efficiently from DB) ---
#     # Removed await db.expire_all() to avoid MissingGreenlet error.
#     # In a typical web app, a new session should see committed data from previous requests.

#     existing_positions = await crud_order.get_open_orders_by_user_id_and_symbol(
#         db=db,
#         user_id=user.id,
#         symbol=order_symbol
#     )

#     # Add logging to show the fetched existing positions
#     logger.info(f"User {user.id}, symbol {order_symbol}: Fetched existing open positions: {[p.order_id for p in existing_positions]}")


#     total_existing_buy_quantity = Decimal(0)
#     total_existing_sell_quantity = Decimal(0)

#     for position in existing_positions:
#         position_quantity = Decimal(str(position.order_quantity))
#         position_type = position.order_type.upper()

#         if position_type == 'BUY':
#             total_existing_buy_quantity += position_quantity
#         elif position_type == 'SELL':
#             total_existing_sell_quantity += position_quantity

#     logger.debug(f"User {user.id}, symbol {order_symbol}: Total Existing BUY quantity: {total_existing_buy_quantity}, Total Existing SELL quantity: {total_existing_sell_quantity}")


#     # --- Step 3: Calculate net existing position and determine net opposing quantity ---
#     net_existing_buy = max(Decimal(0), total_existing_buy_quantity - total_existing_sell_quantity)
#     net_existing_sell = max(Decimal(0), total_existing_sell_quantity - total_existing_buy_quantity)

#     logger.debug(f"User {user.id}, symbol {order_symbol}: Net Existing BUY: {net_existing_buy}, Net Existing SELL: {net_existing_sell}")

#     net_opposing_quantity = Decimal(0)
#     if new_order_type == 'BUY':
#         # If new order is BUY, the net opposing quantity from existing positions is the net SELL quantity
#         net_opposing_quantity = net_existing_sell
#     elif new_order_type == 'SELL':
#         # If new order is SELL, the net opposing quantity from existing positions is the net BUY quantity
#         net_opposing_quantity = net_existing_buy

#     logger.debug(f"New order quantity: {new_order_quantity}, Net Opposing Quantity from existing positions: {net_opposing_quantity}")


#     # --- Step 4: Calculate hedged and unhedged quantities for the NEW order ---
#     # Hedged quantity is limited by the available net opposing quantity from existing positions
#     hedged_quantity = min(new_order_quantity, net_opposing_quantity)
#     unhedged_quantity = new_order_quantity - hedged_quantity

#     logger.info(f"Hedging Calculation for New Order: Hedged Quantity: {hedged_quantity}, Unhedged Quantity: {unhedged_quantity}")


#     # --- Step 5: Calculate the additional margin required for this NEW order based on hedging ---
#     # This is the amount to ADD to the user's existing overall margin.

#     if unhedged_quantity > Decimal('0.00000001'): # Check if there's a significant unhedged portion
#         # If there's an unhedged portion, calculate its margin contribution
#         # This uses the base margin per lot of the new order type at the order price
#         margin_per_lot_new_order = await calculate_base_margin_per_lot(
#             redis_client=redis_client,
#             user_id=user.id,
#             symbol=order_symbol,
#             price=order_request.order_price # Use the order price
#         )

#         if margin_per_lot_new_order is None:
#              logger.error(f"Failed to calculate base margin per lot for unhedged portion ({new_order_type}) at order price for user {user.id}, symbol {order_symbol}.")
#              raise OrderProcessingError("Failed to calculate margin per lot for hedging.")

#         unhedged_margin_contribution = unhedged_quantity * margin_per_lot_new_order
#         logger.debug(f"Margin per lot for new order ({new_order_type}): {margin_per_lot_new_order}")
#         logger.debug(f"Unhedged margin contribution: {unhedged_margin_contribution}")
#     else:
#         unhedged_margin_contribution = Decimal(0)
#         logger.debug("No unhedged quantity, unhedged margin contribution is 0.")


#     # For the hedged portion, we need to consider the margin requirement for the hedged pair.
#     # This typically uses the higher of the margin per lot for the BUY side and SELL side
#     # of the hedged quantity, often at current market prices.
#     # Let's calculate the margin per lot for both BUY and SELL sides at current market prices
#     # to determine the higher margin per lot for the hedged quantity.

#     hedged_margin_contribution = Decimal(0) # Initialize hedged margin contribution

#     if hedged_quantity > Decimal('0.00000001'): # Only calculate if there's a significant hedged portion
#         current_market_prices = await get_adjusted_market_price_cache(redis_client, user.group_name, order_symbol)

#         margin_per_lot_buy_current = Decimal(0)
#         margin_per_lot_sell_current = Decimal(0)

#         if current_market_prices:
#             current_buy_price = current_market_prices.get('buy')
#             current_sell_price = current_market_prices.get('sell')

#             if current_buy_price is not None:
#                 margin_per_lot_buy_current = await calculate_base_margin_per_lot(
#                     redis_client=redis_client,
#                     user_id=user.id,
#                     symbol=order_symbol,
#                     price=Decimal(str(current_buy_price)) # Use current BUY price
#                 )
#                 if margin_per_lot_buy_current is None:
#                     margin_per_lot_buy_current = Decimal(0)

#             if current_sell_price is not None:
#                  margin_per_lot_sell_current = await calculate_base_margin_per_lot(
#                      redis_client=redis_client,
#                      user_id=user.id,
#                      symbol=order_symbol,
#                      price=Decimal(str(current_sell_price)) # Use current SELL price
#                  )
#                  if margin_per_lot_sell_current is None:
#                       margin_per_lot_sell_current = Decimal(0)

#         logger.debug(f"Margin per lot (current price) BUY: {margin_per_lot_buy_current}, SELL: {margin_per_lot_sell_current}")

#         # The margin for the hedged quantity is based on the higher of the BUY/SELL margin per lot
#         higher_margin_per_lot_hedged = max(margin_per_lot_buy_current, margin_per_lot_sell_current)
#         hedged_margin_contribution = hedged_quantity * higher_margin_per_lot_hedged
#         logger.debug(f"Higher margin per lot for hedged quantity: {higher_margin_per_lot_hedged}")
#         logger.debug(f"Hedged margin contribution: {hedged_margin_contribution}")
#     else:
#         logger.debug("No hedged quantity, hedged margin contribution is 0.")


#     # The total additional margin required is the sum of the unhedged and hedged contributions
#     additional_margin_required = unhedged_margin_contribution + hedged_margin_contribution
#     logger.info(f"Additional margin required (considering net hedging): {additional_margin_required}")


#     # --- Step 6: Fetch user with lock and perform margin check and update ---
#     # Use the get_user_by_id_with_lock function to prevent race conditions
#     db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_id=user.id)

#     if db_user_locked is None:
#         logger.error(f"Could not retrieve user {user.id} with lock during order placement processing.")
#         raise OrderProcessingError("Could not retrieve user data securely.")

#     # Add logging to show the user's margin immediately after fetching with lock
#     user_overall_margin_before = Decimal(str(db_user_locked.margin)) # Get margin BEFORE adding
#     logger.info(f"User {user.id}: Overall Margin AFTER fetching with lock (BEFORE update): {user_overall_margin_before}")


#     # Ensure wallet_balance and margin are Decimal
#     user_wallet_balance = Decimal(str(db_user_locked.wallet_balance))


#     available_free_margin = user_wallet_balance - user_overall_margin_before

#     logger.debug(f"User {user.id}: Wallet Balance: {user_wallet_balance}, Overall Margin BEFORE update: {user_overall_margin_before}, Available Free Margin: {available_free_margin}")
#     logger.debug(f"Additional margin to ADD to user's overall margin: {additional_margin_required}")


#     # Check if user has sufficient free margin for the additional margin required by the new order
#     if available_free_margin < additional_margin_required:
#         logger.warning(f"Insufficient funds for user {user.id} to place order. Additional margin required: {additional_margin_required}, Available free margin: {available_free_margin}")
#         raise InsufficientFundsError(f"Insufficient funds. Required additional margin: {additional_margin_required:.8f} USD.") # Use 8 decimal places for clarity


#     # Update the user's total overall margin by adding the additional margin required for the new order
#     db_user_locked.margin += additional_margin_required
#     logger.info(f"User {user.id}: Updated overall margin to {db_user_locked.margin} after placing order (considering net hedging).")


#     # --- Step 7: Prepare data for creating the UserOrder record and save ---
#     # The margin stored in the order table is the FULL, non-hedged margin
#     unique_order_id = str(uuid.uuid4())
#     order_status = "OPEN" # Or "PENDING_EXECUTION" depending on your flow

#     order_data_internal = OrderCreateInternal(
#         order_id=unique_order_id,
#         order_status=order_status,
#         order_user_id=user.id,
#         order_company_name=order_symbol, # Storing symbol as company_name
#         order_type=new_order_type,
#         order_price=adjusted_order_price, # Use the adjusted price from calculate_single_order_margin
#         order_quantity=new_order_quantity,
#         contract_value=contract_value, # Use the contract value from calculate_single_order_margin
#         margin=full_calculated_margin_usd, # Store the FULL, non-hedged margin from calculate_single_order_margin
#         stop_loss=order_request.stop_loss,
#         take_profit=order_request.take_profit,
#         # Initialize other fields as needed
#         net_profit=None,
#         close_price=None,
#         swap=None, # Swap might be calculated later or at end of day
#         commission=None, # Commission might be calculated here or upon closing
#         status=1 # Default status for an open order
#     )

#     # Create the new order ORM object
#     new_db_order = UserOrder(**order_data_internal.model_dump())

#     # Add the updated user object and the new order object to the session
#     # The db_user_locked object is already tracked by the session due to the lock fetch
#     db.add(new_db_order)

#     try:
#         # Commit the transaction. This saves the updated user and the new order.
#         await db.commit()

#         # Refresh objects to get database-generated fields (like id, created_at)
#         await db.refresh(db_user_locked) # Refresh the user object to get latest state if needed later
#         await db.refresh(new_db_order)

#         # Add logging to show the user's margin AFTER the commit
#         user_overall_margin_after_commit = Decimal(str(db_user_locked.margin))
#         logger.info(f"User {user.id}: Overall Margin AFTER commit: {user_overall_margin_after_commit}")


#         logger.info(f"Order {new_db_order.order_id} placed successfully for user ID {user.id}. Full order margin: {full_calculated_margin_usd} USD. User overall margin updated to {db_user_locked.margin}.")

#         # TODO: Post-order creation actions:
#         # 1. Publish order event to a queue for further processing (e.g., matching engine, audit log)
#         # 2. Update user's portfolio cache in Redis with the new position and updated margin/equity.
#         #    This cache update should reflect the *new total* overall margin and the new position.
#         #    This update should also trigger a WebSocket message to the user via the broadcaster.

#         return new_db_order # Return the ORM object

#     except Exception as e:
#         # Any database error during commit will be caught here
#         logger.error(f"Database error during order placement commit for user ID {user.id}: {e}", exc_info=True)
#         # Re-raise a specific exception
#         raise OrderProcessingError("Database error during order placement.")

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
# Import the margin calculator service and its helper
from app.services.margin_calculator import calculate_single_order_margin, calculate_base_margin_per_lot

logger = logging.getLogger(__name__)

# Define custom exceptions for the service
class OrderProcessingError(Exception):
    """Custom exception for errors during order processing."""
    pass

class InsufficientFundsError(Exception): # Inherit from base Exception, not OrderProcessingError
    """Custom exception for insufficient funds during order placement."""
    pass

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
    # This is the margin that will be stored with the individual order record.
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
         raise OrderProcessingError("Failed to calculate order details for new order.")

    # Ensure new order quantity is positive before calculating margin per lot for it
    if new_order_quantity <= 0:
        logger.error(f"New order quantity is zero or negative for user {user.id}, symbol {order_symbol}.")
        # This should ideally be caught by endpoint validation, but defensive check is good.
        raise OrderProcessingError("Invalid order quantity.")

    new_order_margin_per_lot = full_calculated_margin_usd / new_order_quantity

    logger.debug(f"Calculated full order margin (non-hedged, for order record): {full_calculated_margin_usd}, Adjusted price: {adjusted_order_price}, Contract value: {contract_value}, New order margin per lot: {new_order_margin_per_lot}")


    # --- Step 2: Fetch user's existing open positions for the same symbol (efficiently from DB) ---
    existing_positions = await crud_order.get_open_orders_by_user_id_and_symbol(
        db=db,
        user_id=user.id,
        symbol=order_symbol
    )

    logger.debug(f"User {user.id}, symbol {order_symbol}: Fetched {len(existing_positions)} existing open positions.")

    total_existing_buy_quantity = Decimal(0)
    total_existing_sell_quantity = Decimal(0)
    all_margins_per_lot_before: List[Decimal] = [] # Store margin/lot for EXISTING positions

    for position in existing_positions:
        position_quantity = Decimal(str(position.order_quantity))
        position_type = position.order_type.upper()
        position_margin = Decimal(str(position.margin)) # Full margin for this existing order

        # Calculate margin per lot for the existing position
        if position_quantity > 0:
            existing_pos_margin_per_lot = position_margin / position_quantity
            all_margins_per_lot_before.append(existing_pos_margin_per_lot)
        else:
            logger.warning(f"Existing position {position.order_id} for user {user.id} has zero quantity. Skipping for margin calculation.")
            # Decide how to handle zero quantity positions - might indicate data error

        if position_type == 'BUY':
            total_existing_buy_quantity += position_quantity
        elif position_type == 'SELL':
            total_existing_sell_quantity += position_quantity

    logger.debug(f"User {user.id}, symbol {order_symbol}: Total Existing BUY quantity: {total_existing_buy_quantity}, Total Existing SELL quantity: {total_existing_sell_quantity}")

    # Calculate net quantity BEFORE the new order
    net_quantity_before = abs(total_existing_buy_quantity - total_existing_sell_quantity)
    logger.debug(f"User {user.id}, symbol {order_symbol}: Net quantity before new order: {net_quantity_before}")

    # Find the highest margin per lot among existing positions (if any)
    highest_margin_per_lot_before = max(all_margins_per_lot_before) if all_margins_per_lot_before else Decimal(0)
    logger.debug(f"User {user.id}, symbol {order_symbol}: Highest margin per lot before new order: {highest_margin_per_lot_before}")

    # Calculate the old total margin attributed to this symbol based on the old rule
    # (This might be slightly different from the user's actual overall margin in the DB if they have positions in other symbols)
    # This calculation is needed to find the *change* in margin required for this symbol.
    # old_total_margin_for_symbol = highest_margin_per_lot_before * net_quantity_before
    # logger.debug(f"Old total margin for symbol '{order_symbol}' before new order (calculated): {old_total_margin_for_symbol}")


    # --- Step 3: Calculate Highest Margin Per Lot and Net Quantity AFTER including the new order ---
    all_margins_per_lot_after: List[Decimal] = all_margins_per_lot_before.copy() # Start with existing
    all_margins_per_lot_after.append(new_order_margin_per_lot) # Add the new order's margin per lot

    # Find the highest margin per lot *after* adding the new order
    highest_margin_per_lot_after = max(all_margins_per_lot_after) if all_margins_per_lot_after else Decimal(0) # Should never be empty if new order is valid
    logger.debug(f"User {user.id}, symbol {order_symbol}: Highest margin per lot after new order: {highest_margin_per_lot_after}")

    # Calculate total buy and sell quantity AFTER including the new order
    total_buy_quantity_after = total_existing_buy_quantity + (new_order_quantity if new_order_type == 'BUY' else Decimal(0))
    total_sell_quantity_after = total_existing_sell_quantity + (new_order_quantity if new_order_type == 'SELL' else Decimal(0))

    # Calculate net quantity AFTER including the new order
    net_quantity_after = abs(total_buy_quantity_after - total_sell_quantity_after)
    logger.debug(f"User {user.id}, symbol {order_symbol}: Total BUY quantity after: {total_buy_quantity_after}, Total SELL quantity after: {total_sell_quantity_after}, Net quantity after new order: {net_quantity_after}")


    # --- Step 4: Calculate the ADDITIONAL margin required based on the change in net quantity ---
    # This is based on the "Highest Margin per Net Quantity Increase" rule
    # additional_margin_required = highest_margin_per_lot_after * (net_quantity_after - net_quantity_before)
    # This calculation can be simplified. The margin required for THIS SYMBOL is now:
    # new_total_margin_for_symbol = highest_margin_per_lot_after * net_quantity_after

    # The "additional margin required" to ADD to the user's *overall* margin
    # should be the margin needed for the *increase in net quantity*
    net_quantity_increase = max(Decimal(0), net_quantity_after - net_quantity_before)
    logger.debug(f"User {user.id}, symbol {order_symbol}: Net quantity increase: {net_quantity_increase}")

    # The additional margin to add is the margin for the *increase* in net quantity,
    # calculated at the *highest margin rate* among all positions for this symbol *after* the new order.
    additional_margin_required = highest_margin_per_lot_after * net_quantity_increase
    logger.info(f"Calculated additional margin required (Highest Margin per Net Qty Increase model): {additional_margin_required}")


    # --- Step 5: Fetch user with lock and perform margin check and update ---
    # Fetch the user from the database with a row-level lock. This ensures we get the latest balance and margin.
    db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_id=user.id)

    if db_user_locked is None:
        logger.error(f"Could not retrieve user {user.id} with lock during order placement processing.")
        raise OrderProcessingError("Could not retrieve user data securely.")

    user_overall_margin_before = Decimal(str(db_user_locked.margin)) # Get current overall margin from DB
    user_wallet_balance = Decimal(str(db_user_locked.wallet_balance)) # Get current wallet balance from DB

    available_free_margin = user_wallet_balance - user_overall_margin_before

    logger.debug(f"User {user.id}: Wallet Balance (from DB): {user_wallet_balance}, Overall Margin BEFORE update (from DB): {user_overall_margin_before}, Available Free Margin: {available_free_margin}")
    logger.debug(f"Additional margin to ADD to user's overall margin: {additional_margin_required}")

    # Check if user has sufficient free margin for the additional margin required
    if available_free_margin < additional_margin_required:
        logger.warning(f"Insufficient funds for user {user.id} to place order. Additional margin required: {additional_margin_required}, Available free margin: {available_free_margin}")
        raise InsufficientFundsError(f"Insufficient funds. Required additional margin: {additional_margin_required:.8f} USD.")


    # Update the user's total overall margin by adding the additional margin required
    db_user_locked.margin += additional_margin_required
    logger.info(f"User {user.id}: Updated overall margin to {db_user_locked.margin} after placing order (considering net hedging).")


    # --- Step 6: Prepare data for creating the UserOrder record and save ---
    # The margin stored in the order table is the FULL, non-hedged margin calculated in Step 1.
    unique_order_id = str(uuid.uuid4())
    order_status = "OPEN" # Or "PENDING_EXECUTION" depending on your flow

    order_data_internal = OrderCreateInternal(
        order_id=unique_order_id,
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

    # Create the new order ORM object
    new_db_order = UserOrder(**order_data_internal.model_dump())

    # Add the updated user object and the new order object to the session
    db.add(new_db_order)

    try:
        # Commit the transaction. This saves the updated user and the new order.
        await db.commit()

        # Refresh objects to get database-generated fields (like id, created_at)
        await db.refresh(db_user_locked)
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
        await db.rollback()
        logger.error(f"Database error during order placement commit for user ID {user.id}: {e}", exc_info=True)
        # Re-raise a specific exception
        raise OrderProcessingError("Database error during order placement.")