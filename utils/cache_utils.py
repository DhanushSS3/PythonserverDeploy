# # # app/utils/cache_utils.py
# # from redis.asyncio import Redis
# # from app.core.cache import REDIS_USER_DATA_KEY_PREFIX
# # import logging

# # logger = logging.getLogger(__name__)

# # async def clear_user_data_cache(redis_client: Redis, user_id: int) -> bool:
# #     """
# #     Clears the user data cache entry for a specific user ID.

# #     Args:
# #         redis_client: The asynchronous Redis client instance.
# #         user_id: The ID of the user whose cache should be cleared.

# #     Returns:
# #         True if the key was deleted, False otherwise (e.g., key not found or error).
# #     """
# #     if not redis_client:
# #         logger.warning(f"Redis client not available to clear user data cache for user {user_id}.")
# #         return False

# #     key = f"{REDIS_USER_DATA_KEY_PREFIX}{user_id}"
# #     try:
# #         # The delete command returns the number of keys that were removed.
# #         deleted_count = await redis_client.delete(key)
# #         if deleted_count > 0:
# #             logger.info(f"User data cache cleared for user {user_id}. Key: {key}")
# #             return True
# #         else:
# #             logger.warning(f"User data cache key not found for user {user_id}. Key: {key}")
# #             return False
# #     except Exception as e:
# #         logger.error(f"Error clearing user data cache for user {user_id} (Key: {key}): {e}", exc_info=True)
# #         return False

# # # You might also want functions to clear other cache types, e.g.:
# # # async def clear_user_portfolio_cache(redis_client: Redis, user_id: int):
# # #     key = f"{app.core.cache.REDIS_USER_PORTFOLIO_KEY_PREFIX}{user_id}"
# # #     await redis_client.delete(key)
# # # async def clear_group_symbol_settings_cache(redis_client: Redis, group_name: str, symbol: str):
# # #     key = f"{app.core.cache.REDIS_GROUP_SYMBOL_SETTINGS_KEY_PREFIX}{group_name.lower()}:{symbol.upper()}"
# # #     await redis_client.delete(key)
# # app/api/v1/endpoints/orders.py

# from fastapi import APIRouter, Depends, HTTPException, status, Body
# from sqlalchemy.ext.asyncio import AsyncSession
# from redis.asyncio import Redis # Import Redis
# import uuid
# import logging
# from decimal import Decimal
# from typing import Optional, Any, List

# from app.database.session import get_db
# from app.dependencies.redis_client import get_redis_client # Import redis client dependency
# from app.database.models import UserOrder, User # Import UserOrder and User models
# # Updated schema imports
# from app.schemas.order import OrderPlacementRequest, OrderResponse, OrderCreateInternal
# from app.schemas.user import StatusResponse
# from app.core.security import get_current_user
# # Import the new margin calculator service (assuming it exists and has calculate_single_order_margin)
# from app.services.margin_calculator import calculate_single_order_margin # Keep this import
# # Import caching functions
# from app.core.cache import get_user_positions_from_cache, get_adjusted_market_price_cache, get_user_data_cache, get_group_symbol_settings_cache
# # Import the new CRUD operations for orders and user with lock
# from app.crud import crud_order
# from app.crud import user as crud_user # Alias user crud to avoid conflict

# logger = logging.getLogger(__name__)

# router = APIRouter(
#     prefix="/orders", # Add prefix for consistency
#     tags=["orders"]
# )

# # Add this helper function (or move to app/services/margin_calculator.py)
# async def calculate_margin_per_lot(
#     db: AsyncSession, # Although not used here, keep signature consistent if moved to service
#     redis_client: Redis,
#     user_id: int,
#     symbol: str,
#     order_type: str,
#     price: Decimal # Price at which margin per lot is calculated (order price or current market price)
# ) -> Optional[Decimal]:
#     """
#     Calculates the margin required per standard lot (e.g., 1 lot) for a given symbol, order type, and price,
#     considering user's group settings and leverage.
#     Returns the margin per lot in USD or None if calculation fails.
#     """
#     # Retrieve user data from cache to get group_name and leverage
#     user_data = await get_user_data_cache(redis_client, user_id)
#     if not user_data or 'group_name' not in user_data or 'leverage' not in user_data:
#         logger.error(f"User data or group_name/leverage not found in cache for user {user_id}.")
#         return None

#     group_name = user_data['group_name']
#     # Ensure user_leverage is Decimal
#     user_leverage_raw = user_data.get('leverage', 1)
#     user_leverage = Decimal(str(user_leverage_raw)) if user_leverage_raw is not None else Decimal(1)


#     # Retrieve group-symbol settings from cache
#     # Need settings for the specific symbol
#     group_symbol_settings = await get_group_symbol_settings_cache(redis_client, group_name, symbol)
#     if not group_symbol_settings or 'margin' not in group_symbol_settings:
#         logger.error(f"Group symbol settings or margin setting not found in cache for group '{group_name}', symbol '{symbol}'.")
#         return None

#     # Ensure margin_setting is Decimal
#     margin_setting_raw = group_symbol_settings.get('margin', 0)
#     margin_setting = Decimal(str(margin_setting_raw)) if margin_setting_raw is not None else Decimal(0)


#     if user_leverage <= 0:
#          logger.error(f"User leverage is zero or negative for user {user_id}.")
#          return None

#     # Assuming margin calculation formula is (Margin_Setting * Price) / Leverage
#     # Price used here should be the contract value price (e.g., market price bid for sell, ask for buy)
#     # For margin calculation, the price used is typically the contract value price.
#     # Let's use the provided 'price' parameter which could be order price or current market price.
#     try:
#         # Ensure price is Decimal
#         price_decimal = Decimal(str(price))
#         margin_per_lot_usd = (margin_setting * price_decimal) / user_leverage
#         logger.debug(f"Calculated margin per lot for user {user_id}, symbol {symbol}, type {order_type}, price {price}: {margin_per_lot_usd} USD")
#         return margin_per_lot_usd
#     except Exception as e:
#         logger.error(f"Error calculating margin per lot for user {user_id}, symbol {symbol}: {e}", exc_info=True)
#         return None


# # Endpoint to place a new order
# @router.post(
#     "/",
#     response_model=OrderResponse, # Use the new OrderResponse schema
#     status_code=status.HTTP_201_CREATED,
#     summary="Place a new order",
#     description="Allows an authenticated user to place a new trading order. Margin and contract value are calculated by the backend, and user's total margin is updated considering hedging."
# )
# async def place_order(
#     order_request: OrderPlacementRequest, # Use the new request schema
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client), # Inject Redis client
#     current_user: User = Depends(get_current_user)
# ):
#     """
#     Places a new order for the authenticated user.
#     Calculates margin and contract value, applies hedging logic to the user's total margin,
#     and updates the user's used margin.
#     """
#     logger.info(f"Attempting to place order for user {current_user.id}, symbol {order_request.symbol}, type {order_request.order_type}, quantity {order_request.order_quantity}")

#     # Validate order quantity (example: must be positive)
#     if order_request.order_quantity <= 0:
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail="Order quantity must be positive."
#         )

#     # Ensure order_request.order_quantity is Decimal for calculations
#     new_order_quantity = Decimal(str(order_request.order_quantity))
#     new_order_type = order_request.order_type.upper()
#     order_symbol = order_request.symbol.upper() # Use upper for consistency

#     # 1. Calculate the full, non-hedged margin for the new order
#     # This margin value will be stored with the individual order record.
#     # We need the margin per lot for the new order type/price for hedging calculation later.
#     # Let's call calculate_single_order_margin first to get the total margin and contract value.
#     # We might need to adjust calculate_single_order_margin or call calculate_margin_per_lot separately.
#     # Assuming calculate_single_order_margin gives us the total margin for the requested quantity.
#     # We'll call calculate_margin_per_lot separately to get the per-lot value for hedging.

#     # Get the margin per lot for the new order's type and price
#     margin_per_lot_new_order = await calculate_margin_per_lot(
#         db=db,
#         redis_client=redis_client,
#         user_id=current_user.id,
#         symbol=order_symbol,
#         order_type=new_order_type,
#         price=order_request.order_price # Use the order price for this calculation
#     )

#     if margin_per_lot_new_order is None:
#          logger.error(f"Failed to calculate margin per lot for new order for user {current_user.id}, symbol {order_symbol}.")
#          raise HTTPException(
#              status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#              detail="Failed to calculate margin per lot for the new order."
#          )

#     # Calculate the full margin for the new order (to be stored in the order table)
#     full_calculated_margin_usd = margin_per_lot_new_order * new_order_quantity

#     # We also need the contract value. Let's assume calculate_single_order_margin also returns this.
#     # If not, you'll need to calculate it here based on order_request.order_price and quantity.
#     # For now, let's make a placeholder call or assume a structure for calculate_single_order_margin
#     # If calculate_single_order_margin is designed to return total margin, adjusted price, and contract value:
#     # calculated_margin_usd_total, adjusted_order_price, contract_value = await calculate_single_order_margin(...)
#     # We will use full_calculated_margin_usd for the order table and the hedging logic will determine the *additional* margin needed overall.

#     # Let's calculate contract value here for clarity
#     contract_value = order_request.order_price * new_order_quantity
#     # Note: adjusted_order_price is not directly needed for margin calculation here, but is needed for the order record.
#     # You would get the adjusted price from calculate_single_order_margin or a similar function.
#     # For simplicity in this hedging logic example, we'll focus on margin calculation.
#     # Assume you have adjusted_order_price from a prior step or calculate_single_order_margin call.
#     # Placeholder for adjusted_order_price - replace with actual calculation if needed
#     adjusted_order_price = order_request.order_price # Simplified for this example

#     # 2. Fetch user's existing open positions for the same symbol
#     # Assuming get_user_positions_from_cache returns a list of position dictionaries
#     existing_positions = await get_user_positions_from_cache(redis_client, current_user.id)

#     total_existing_buy_quantity = Decimal(0)
#     total_existing_sell_quantity = Decimal(0)

#     # Iterate through existing positions to find those for the same symbol
#     for position in existing_positions:
#         # Assuming position dictionary has keys like 'symbol', 'order_type', 'order_quantity'
#         if position.get('order_company_name', '').upper() == order_symbol: # Use order_company_name as symbol
#             position_quantity = Decimal(str(position.get('order_quantity', 0)))
#             position_type = position.get('order_type', '').upper()

#             if position_type == 'BUY':
#                 total_existing_buy_quantity += position_quantity
#             elif position_type == 'SELL':
#                 total_existing_sell_quantity += position_quantity

#     logger.debug(f"User {current_user.id}, symbol {order_symbol}: Existing BUY quantity: {total_existing_buy_quantity}, Existing SELL quantity: {total_existing_sell_quantity}")

#     # 3. Determine the opposing quantity and calculate hedged/unhedged quantities
#     opposing_quantity = Decimal(0)
#     if new_order_type == 'BUY':
#         opposing_quantity = total_existing_sell_quantity
#     elif new_order_type == 'SELL':
#         opposing_quantity = total_existing_buy_quantity

#     hedged_quantity = min(new_order_quantity, opposing_quantity)
#     unhedged_quantity = new_order_quantity - hedged_quantity

#     logger.debug(f"New order quantity: {new_order_quantity}, Opposing quantity: {opposing_quantity}")
#     logger.debug(f"Hedged quantity: {hedged_quantity}, Unhedged quantity: {unhedged_quantity}")


#     # 4. Calculate margin per lot for the opposing type (based on current market price)
#     # Need current market price for the opposing type
#     current_market_prices = await get_adjusted_market_price_cache(redis_client, current_user.group_name, order_symbol)

#     margin_per_lot_opposing = Decimal(0) # Initialize to 0

#     if current_market_prices:
#         opposing_price_type = 'sell' if new_order_type == 'BUY' else 'buy' # If placing BUY, opposing is SELL (use SELL price)
#         current_opposing_price = current_market_prices.get(opposing_price_type)

#         if current_opposing_price is not None:
#              # Calculate margin per lot for the opposing type at the current market price
#              margin_per_lot_opposing = await calculate_margin_per_lot(
#                  db=db, # Pass db even if not used in this version, for consistency
#                  redis_client=redis_client,
#                  user_id=current_user.id,
#                  symbol=order_symbol,
#                  order_type='SELL' if new_order_type == 'BUY' else 'BUY', # Opposing type
#                  price=Decimal(str(current_opposing_price)) # Use current market price
#              )
#              if margin_per_lot_opposing is None:
#                   margin_per_lot_opposing = Decimal(0) # Default to 0 if calculation fails

#     logger.debug(f"Margin per lot new order ({new_order_type}): {margin_per_lot_new_order}")
#     logger.debug(f"Margin per lot opposing order ({'SELL' if new_order_type == 'BUY' else 'BUY'}): {margin_per_lot_opposing}")


#     # 5. Determine the higher margin per lot and calculate total margin considering hedging
#     higher_margin_per_lot = max(margin_per_lot_new_order, margin_per_lot_opposing)

#     hedged_margin_contribution = hedged_quantity * higher_margin_per_lot
#     unhedged_margin_contribution = unhedged_quantity * margin_per_lot_new_order # Unhedged portion uses its own margin per lot

#     total_margin_for_new_order_considering_hedging = hedged_margin_contribution + unhedged_margin_contribution

#     logger.debug(f"Higher margin per lot: {higher_margin_per_lot}")
#     logger.debug(f"Hedged margin contribution: {hedged_margin_contribution}")
#     logger.debug(f"Unhedged margin contribution: {unhedged_margin_contribution}")
#     logger.debug(f"Total margin required for new order (considering hedging): {total_margin_for_new_order_considering_hedging}")


#     # 6. Fetch user with lock and perform margin check
#     # Use the get_user_by_id_with_lock function to prevent race conditions
#     db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_id=current_user.id)

#     if db_user_locked is None:
#         # This should ideally not happen if get_current_user works, but as a safeguard
#         await db.rollback()
#         logger.error(f"Could not retrieve user {current_user.id} with lock during order placement.")
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="Could not retrieve user data securely."
#         )

#     # Ensure wallet_balance and used_margin are Decimal
#     user_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
#     user_used_margin = Decimal(str(db_user_locked.used_margin))

#     available_free_margin = user_wallet_balance - user_used_margin

#     logger.debug(f"User {current_user.id}: Wallet Balance: {user_wallet_balance}, Used Margin: {user_used_margin}, Available Free Margin: {available_free_margin}")


#     # Check if user has sufficient free margin for the new order's margin requirement (considering hedging)
#     if available_free_margin < total_margin_for_new_order_considering_hedging:
#         await db.rollback() # Rollback the transaction if funds are insufficient
#         logger.warning(f"Insufficient funds for user {current_user.id} to place order. Required margin (hedged): {total_margin_for_new_order_considering_hedging}, Available free margin: {available_free_margin}")
#         raise HTTPException(
#             status_code=status.HTTP_400_BAD_REQUEST,
#             detail=f"Insufficient funds. Required margin: {total_margin_for_new_order_considering_hedging:.2f} USD."
#         )

#     # 7. Update the user's total used margin
#     db_user_locked.used_margin += total_margin_for_new_order_considering_hedging
#     logger.info(f"User {current_user.id}: Updated used_margin to {db_user_locked.used_margin} after placing order (considering hedging).")


#     # 8. Prepare data for creating the UserOrder record
#     # The margin stored in the order table is the FULL, non-hedged margin
#     unique_order_id = str(uuid.uuid4())
#     order_status = "OPEN" # Or "PENDING_EXECUTION" depending on your flow

#     order_data_internal = OrderCreateInternal(
#         order_id=unique_order_id,
#         order_status=order_status,
#         order_user_id=current_user.id,
#         order_company_name=order_symbol, # Storing symbol as company_name
#         order_type=new_order_type,
#         order_price=adjusted_order_price, # Use the adjusted price (assuming you get this)
#         order_quantity=new_order_quantity,
#         contract_value=contract_value, # Store calculated contract value
#         margin=full_calculated_margin_usd, # Store the FULL, non-hedged margin here
#         stop_loss=order_request.stop_loss,
#         take_profit=order_request.take_profit,
#         # Initialize other fields as needed
#         net_profit=None,
#         close_price=None,
#         swap=None, # Swap might be calculated later or at end of day
#         commission=None, # Commission might be calculated here or upon closing
#         status=1 # Default status for an open order
#     )

#     try:
#         # Add the updated user object to the session (it's already tracked due to the lock fetch)
#         # Add the new order object
#         db.add(db_user_locked) # Add the locked user object to the session (or it might be tracked already)
#         new_db_order = UserOrder(**order_data_internal.model_dump()) # Create ORM object from schema dict
#         db.add(new_db_order) # Add the new order to the session

#         # Commit the transaction
#         await db.commit()

#         # Refresh objects to get database-generated fields (like id, created_at)
#         await db.refresh(db_user_locked)
#         await db.refresh(new_db_order)


#         # TODO: Post-order creation actions:
#         # 1. Publish order event to a queue for further processing (e.g., matching engine, audit log)
#         # 2. Update user's portfolio cache in Redis with the new position and updated margin/equity.

#         logger.info(f"Order {new_db_order.order_id} placed successfully for user ID {current_user.id}. Full order margin: {full_calculated_margin_usd} USD. User overall used_margin updated to {db_user_locked.used_margin}.")

#         # Return the created order details using OrderResponse
#         return OrderResponse.model_validate(new_db_order) # For Pydantic V2


#     except HTTPException as http_exc: # Re-raise known HTTP exceptions
#         await db.rollback() # Ensure rollback on HTTP exceptions too
#         raise http_exc
#     except Exception as e:
#         await db.rollback() # Ensure rollback on any other exception
#         logger.error(f"Error placing order for user ID {current_user.id}, symbol {order_symbol}: {e}", exc_info=True)
#         raise HTTPException(
#             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
#             detail="An error occurred while placing the order."
#         )

# # Example: Endpoint to get order by ID (using new CRUD)
# @router.get("/{order_id}", response_model=OrderResponse)
# async def read_order(
#     order_id: str,
#     db: AsyncSession = Depends(get_db),
#     current_user: User = Depends(get_current_user) # Ensure user owns order or is admin
# ):
#     db_order = await crud_order.get_order_by_id(db, order_id=order_id)
#     if db_order is None:
#         raise HTTPException(status_code=404, detail="Order not found")
#     # Add authorization: check if current_user.id == db_order.order_user_id or if user is admin
#     # Assuming 'is_admin' is an attribute on the User model
#     if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False): # Example admin check
#          raise HTTPException(status_code=403, detail="Not authorized to view this order")
#     return OrderResponse.model_validate(db_order)


# # Example: Endpoint to get user's orders (using new CRUD)
# @router.get("/", response_model=List[OrderResponse])
# async def read_user_orders(
#     skip: int = 0,
#     limit: int = 100,
#     db: AsyncSession = Depends(get_db),
#     current_user: User = Depends(get_current_user)
# ):
#     orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, skip=skip, limit=limit)
#     # Convert ORM objects to Pydantic models for the response
#     return [OrderResponse.model_validate(order) for order in orders]


# # You would also add other endpoints like cancel_order, modify_order, etc.
# # These would use functions from crud_order.py and potentially margin_calculator.py if needed.
