from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, Any, List, Dict
import uuid
import datetime
from pydantic import BaseModel, Field
import json # Import json for publishing


from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
# Import Wallet model
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet
# Import WalletCreate schema
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate # Import WalletCreate schema

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
from app.core.cache import (
    get_adjusted_market_price_cache,
    get_group_symbol_settings_cache,
    set_user_data_cache, # Import for cache update
    set_user_portfolio_cache, # Import for cache update
    DecimalEncoder # Import for JSON serialization
)

from app.core.security import get_current_user
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # Import the channel name


logger = logging.getLogger(__name__)


# CloseOrderRequest schema is now defined in app.schemas.order


router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

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
            user=current_user,
            order_request=order_request
        )

        logger.info(f"Order {new_db_order.order_id} successfully processed by service.")

        # --- START: New logic for WebSocket responsiveness ---

        # Refresh the user object to get the latest balance and margin after order processing
        await db.refresh(current_user)

        # 1. Update user_data in Redis cache
        # This includes wallet_balance, leverage, and overall margin
        user_data_to_cache = {
            "id": current_user.id,
            "group_name": getattr(current_user, 'group_name', 'default'),
            "leverage": current_user.leverage,
            "wallet_balance": current_user.wallet_balance,
            "margin": current_user.margin
        }
        await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
        logger.debug(f"Updated user data cache for user {current_user.id} after order placement.")

        # # 2. Fetch all current open positions for this user (including the new one)
        # open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, current_user.id)
        # updated_positions_data = []
        # for pos in open_positions_orm:
        #     pos_dict = {}
        #     for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
        #         value = getattr(pos, attr, None)
        #         if isinstance(value, Decimal):
        #             pos_dict[attr] = str(value) # Store Decimal as string for JSON safety
        #         else:
        #             pos_dict[attr] = value
        #     pos_dict['profit_loss'] = "0.0" # Placeholder, will be calculated by broadcaster
        #     updated_positions_data.append(pos_dict)

        # 2. Fetch all current open positions and pending positions
        open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, current_user.id)
        all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)

        updated_open_positions = []
        updated_pending_positions = []
        total_margin = Decimal("0.0")

        for pos in all_orders:
            pos_dict = {}
            for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
                value = getattr(pos, attr, None)
                pos_dict[attr] = str(value) if isinstance(value, Decimal) else value
            pos_dict['profit_loss'] = "0.0"

            if pos.order_status == "OPEN":
                updated_open_positions.append(pos_dict)
            elif pos.order_status == "PENDING":
                updated_pending_positions.append(pos_dict)

            total_margin += Decimal(str(pos.margin or 0.0))

        # 3. Update user_portfolio in Redis cache with separate pending_positions
        user_portfolio_data = {
            "balance": str(current_user.wallet_balance),
            "equity": "0.0",
            "margin": str(total_margin),
            "free_margin": str(current_user.wallet_balance - total_margin),
            "profit_loss": "0.0",
            "positions": updated_open_positions,
            "pending_positions": updated_pending_positions
        }
        await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)
        logger.debug(f"Updated user portfolio cache with pending orders for user {current_user.id} after order placement.")


        # # 3. Update user_portfolio in Redis cache
        # # This includes balance, overall margin, and the latest list of positions
        # user_portfolio_data = {
        #     "balance": str(current_user.wallet_balance),
        #     "equity": "0.0", # Will be recalculated by broadcaster
        #     "margin": str(current_user.margin), # Overall user margin
        #     "free_margin": "0.0", # Will be recalculated by broadcaster
        #     "profit_loss": "0.0", # Will be recalculated by broadcaster (total PnL)
        #     "positions": updated_positions_data
        # }
        # await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)
        # logger.debug(f"Updated user portfolio cache for user {current_user.id} after order placement.")

        # 4. Signal the broadcaster to send account updates for this specific user
        # Publish a message to the market data channel, but with a special type.
        account_update_signal = {
            "type": "account_update_signal", # New message type
            "user_id": current_user.id
        }
        await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal))
        logger.info(f"Published account update signal for user {current_user.id} after order placement.")

        # --- END: New logic for WebSocket responsiveness ---

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

# --- Endpoints for Specific Order Statuses ---

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
    # Fetch all orders for the user first
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
    # Filter them in Python to get only 'CLOSED' orders
    closed_orders = [order for order in orders if order.order_status == 'CLOSED']
    # Apply pagination to the filtered list
    return [OrderResponse.model_validate(order) for order in closed_orders[skip:skip+limit]]

@router.get("/other-statuses", response_model=List[OrderResponse])
async def read_user_orders_excluding_statuses(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Retrieves orders for the authenticated user excluding 'REJECTED', 'OPEN', and 'CLOSED' statuses.
    """
    excluded_statuses = ['REJECTED', 'OPEN', 'CLOSED']
    orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
    filtered_orders = [
        order for order in orders
        if order.order_status not in excluded_statuses
    ]
    return [OrderResponse.model_validate(order) for order in filtered_orders[skip:skip+limit]]


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
import json

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet #
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest #
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate #

from app.services.order_processing import ( #
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution #
)
# Import the specific _convert_to_usd function that now uses raw prices
from app.services.portfolio_calculator import _convert_to_usd #

from app.crud import crud_order #
from app.crud import user as crud_user #

from sqlalchemy.future import select #
from app.core.cache import ( #
    get_adjusted_market_price_cache, # Still needed for other parts potentially, but not for PnL conversion in close_order
    get_group_symbol_settings_cache, #
    set_user_data_cache,
    set_user_portfolio_cache,
    DecimalEncoder
)
# Import get_latest_market_data for checking raw rate availability during critical error handling
from app.firebase_stream import get_latest_market_data #


from app.core.security import get_current_user #
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL #


logger = logging.getLogger(__name__)




# ... (other endpoints like place_order, read_order, etc. would be here) ...

# @router.post(
#     "/close",
#     response_model=OrderResponse,
#     summary="Close an open order",
#     description="Closes an open order, updates its status to 'CLOSED', and adjusts the user's overall margin based on hedging logic. Requires the order ID and closing price in the request body."
# )
# async def close_order(
#     close_request: CloseOrderRequest,
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user: User = Depends(get_current_user)
# ):
#     # Removed internal imports for cache, portfolio_calculator, order_processing as they are top-level now or covered.

#     order_id = close_request.order_id
#     try:
#         close_price = Decimal(str(close_request.close_price))
#         if close_price <= Decimal("0"):
#             raise HTTPException(status_code=400, detail="Close price must be positive.")
#     except InvalidOperation:
#         raise HTTPException(status_code=400, detail="Invalid close price format.")

#     logger.info(f"Received request to close order {order_id} for user {current_user.id} with close price {close_price}.")

#     async with db.begin_nested(): # Using nested transaction for the main logic
#         # 1. Validate and Fetch Order
#         db_order = await crud_order.get_order_by_id(db, order_id=order_id) #
#         if db_order is None:
#             raise HTTPException(status_code=404, detail="Order not found.")
#         if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False): #
#             raise HTTPException(status_code=403, detail="Not authorized to close this order.")
#         if db_order.order_status != 'OPEN': #
#             raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

#         order_symbol = db_order.order_company_name.upper() #
#         quantity = Decimal(str(db_order.order_quantity)) #
#         entry_price = Decimal(str(db_order.order_price)) #
#         order_type = db_order.order_type.upper() #
#         user_group_name = getattr(current_user, 'group_name', 'default') #

#         # 2. Lock user record
#         db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id) #
#         if db_user_locked is None:
#             # This should ideally not happen if current_user is valid, but good for safety
#             logger.error(f"Could not retrieve and lock user record for user ID: {current_user.id}")
#             raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

#         # 3. Margin Recalculation Logic
#         all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol( #
#             db=db,
#             user_id=current_user.id,
#             symbol=order_symbol
#         )
#         margin_before_recalc = await calculate_total_symbol_margin_contribution( #
#             db=db, # calculate_total_symbol_margin_contribution does not use db directly in provided code.
#             redis_client=redis_client,
#             user_id=current_user.id,
#             symbol=order_symbol,
#             open_positions_for_symbol=all_open_orders_for_symbol
#         )

#         # Current total margin of the user before this specific symbol's contribution is adjusted
#         current_overall_margin = Decimal(str(db_user_locked.margin)) #
#         non_symbol_margin = current_overall_margin - margin_before_recalc #

#         remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id] #
#         margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution( #
#             db=db, # As above, not used by the function based on provided code.
#             redis_client=redis_client,
#             user_id=current_user.id,
#             symbol=order_symbol,
#             open_positions_for_symbol=remaining_orders_for_symbol_after_close
#         )
        
#         # New overall margin: non-symbol part + new calculated margin for this symbol
#         db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)) #
#         logger.debug(f"Order {order_id} close: User {current_user.id} margin updated from {current_overall_margin} to {db_user_locked.margin}. Symbol margin change: {margin_before_recalc} -> {margin_after_symbol_recalc}")


#         # 4. Fetch symbol info (ExternalSymbolInfo)
#         symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol)) #
#         symbol_info_result = await db.execute(symbol_info_stmt)
#         ext_symbol_info = symbol_info_result.scalars().first()
#         if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None: #
#             logger.error(f"Missing critical ExternalSymbolInfo (contract_size, profit currency) for symbol: {order_symbol}")
#             raise HTTPException(status_code=500, detail=f"Missing contract size or profit currency for symbol {order_symbol}.")
#         contract_size = Decimal(str(ext_symbol_info.contract_size)) #
#         profit_currency = ext_symbol_info.profit.upper() #
#         if contract_size <= Decimal("0"):
#              logger.error(f"Invalid contract size ({contract_size}) for symbol: {order_symbol}")
#              raise HTTPException(status_code=500, detail=f"Invalid contract size for symbol {order_symbol}.")


#         # 5. Calculate commission for closing (if applicable, though typically on open or both)
#         # The provided code calculates commission based on group settings.
#         # Assuming commission on close is type "0" (Every Trade) or "2" (Out)
#         group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol) #
#         if not group_settings:
#             logger.error(f"Group settings not found for group '{user_group_name}', symbol '{order_symbol}'. Cannot calculate commission for close.")
#             # Depending on policy, you might allow closing without commission or raise error.
#             # Raising error for safety, as commission affects PnL.
#             raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")

#         commission_type = int(group_settings.get('commision_type', -1)) # Default to invalid if not present
#         commission_value_type = int(group_settings.get('commision_value_type', -1)) #
#         commission_rate = Decimal(str(group_settings.get('commision', "0.0"))) #

#         exit_commission = Decimal("0.0") #
#         if commission_type in [0, 2]: # 0: Every Trade, 2: Out
#             if commission_value_type == 0: # Per lot
#                 exit_commission = quantity * commission_rate #
#             elif commission_value_type == 1: # Percent of closing price
#                 # Calculate contract value at close for percentage commission
#                 calculated_exit_contract_value = quantity * contract_size * close_price #
#                 if calculated_exit_contract_value <= Decimal("0.0"): #
#                      logger.warning(f"Calculated exit contract value is zero or negative ({calculated_exit_contract_value}) for order {order_id}. Cannot calculate percentage exit commission.")
#                      exit_commission = Decimal("0.0") #
#                 else:
#                      exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value #
#             else:
#                 logger.warning(f"Unknown commission_value_type: {commission_value_type} for order {order_id}")
        
#         # The original order already had an entry commission stored if applicable.
#         # The `db_order.commission` should reflect total commission for the trade lifecycle if updated here.
#         # Or, wallet entries handle separate commissions.
#         # The provided code adds `entry_commission + exit_commission` to `db_order.commission`.
#         # Let's assume `db_order.commission` stores entry commission, and we add exit_commission to it.
#         # Or, if `db_order.commission` should be *only* the exit commission for this transaction, adjust accordingly.
#         # Based on the provided code, it seems `db_order.commission` is set to `total_commission` which sums entry and exit.
#         # This means entry commission was not stored on the order, or it's recalculated.
#         # The provided code has `total_commission = (entry_commission + exit_commission)` and sets `db_order.commission = total_commission`.
#         # For simplicity in "close_order", we only calculate exit_commission. If db_order.commission was entry, then:
#         # total_commission_for_trade = (db_order.commission or Decimal("0.0")) + exit_commission
#         # However, the provided `close_order` calculates both entry and exit to sum them up. Let's replicate that.
        
#         entry_commission_recalc = Decimal("0.0")
#         if commission_type in [0, 1]: # Recalculate entry commission for total
#             if commission_value_type == 0: # Per lot
#                 entry_commission_recalc = quantity * commission_rate #
#             elif commission_value_type == 1: # Percent of entry price
#                 entry_commission_recalc = ((commission_rate * entry_price) / Decimal("100")) * quantity #
        
#         total_commission_for_trade = (entry_commission_recalc + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) #


#         # 6. Calculate raw PnL (in native profit currency)
#         if order_type == "BUY": #
#             profit = (close_price - entry_price) * quantity * contract_size #
#         elif order_type == "SELL": #
#             profit = (entry_price - close_price) * quantity * contract_size #
#         else:
#             logger.error(f"Invalid order type '{order_type}' found for order {order_id}")
#             raise HTTPException(status_code=500, detail="Invalid order type found in database.")

#         # 7. Convert profit to USD using the updated _convert_to_usd (raw prices)
#         logger.debug(f"Calling _convert_to_usd for PnL for order {db_order.order_id}. Amount: {profit}, Currency: {profit_currency}")
#         profit_usd = await _convert_to_usd( #
#             amount=profit,
#             from_currency=profit_currency,
#             user_id=current_user.id,
#             position_id=db_order.order_id,
#             value_description="PnL on Close"
#         )
        
#         # Critical check: If conversion was needed (profit_currency != USD) but failed (profit_usd is still original `profit`)
#         # and no raw rates were available, then raise an error.
#         if profit_currency != "USD" and profit_usd == profit:
#             # Check if raw rates were actually missing, _convert_to_usd logs details
#             # Simple check to see if any relevant raw price exists for the conversion path
#             direct_raw_symbol = f"{profit_currency}USD"
#             indirect_raw_symbol = f"USD{profit_currency}"
#             direct_data = get_latest_market_data(direct_raw_symbol) #
#             indirect_data = get_latest_market_data(indirect_raw_symbol) #

#             has_direct_rate = direct_data and 'b' in direct_data and direct_data['b'] is not None #
#             has_indirect_rate = indirect_data and 'o' in indirect_data and indirect_data['o'] is not None #

#             if not (has_direct_rate or has_indirect_rate):
#                 logger.error(f"Order {db_order.order_id}: PnL conversion from {profit_currency} to USD failed. Raw market rates for conversion appear to be missing. Original PnL: {profit}")
#                 raise HTTPException(status_code=500, detail=f"Critical error: Could not convert PnL from {profit_currency} to USD due to missing raw market rates.")
#             else:
#                 logger.warning(f"Order {db_order.order_id}: PnL conversion from {profit_currency} returned original amount, but raw rates might exist. Original PnL: {profit}, Converted: {profit_usd}. Proceeding with this value.")


#         # 8. Final updates to order and user balance
#         db_order.order_status = "CLOSED" #
#         db_order.close_price = close_price #
#         db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) #
#         db_order.swap = db_order.swap or Decimal("0.0") # Ensure swap is not None
#         db_order.commission = total_commission_for_trade # Store total commission for the trade's lifecycle

#         # Update user's wallet balance
#         # PnL is already in USD. Commission is calculated in USD.
#         original_wallet_balance = Decimal(str(db_user_locked.wallet_balance)) #
#         # Net effect on wallet: Profit/Loss - Total Commission for this trade
#         # The `total_commission_for_trade` includes entry and exit.
#         # If entry commission was already deducted from balance at order open, then only deduct exit_commission here.
#         # The provided code's `place_order` does not show balance deduction for commission.
#         # It updates user's margin. Wallet transactions would handle balance changes.
#         # The `close_order` `WalletCreate` uses `transaction_amount = -db_order.commission` (which is total_commission_for_trade).
#         # And `transaction_amount = db_order.net_profit`.
#         # So, balance update should be: balance + PnL - total_commission.
#         # This implies commissions are settled from balance at close.
        
#         db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP) #
#         logger.info(f"Order {order_id} close: User {current_user.id} balance updated from {original_wallet_balance} to {db_user_locked.wallet_balance}. PnL(USD): {db_order.net_profit}, TotalComm(USD): {total_commission_for_trade}")


#         # 9. Create Wallet Transaction Records
#         transaction_time = datetime.datetime.now(datetime.timezone.utc)

#         if db_order.net_profit is not None and db_order.net_profit != Decimal("0.0"): #
#             profit_loss_wallet_entry_data = WalletCreate( #
#                 user_id=current_user.id,
#                 symbol=order_symbol,
#                 order_quantity=quantity,
#                 transaction_type="Profit/Loss", #
#                 is_approved=1, #
#                 order_type=order_type, # Or perhaps "CLOSE"? The original has order_type
#                 transaction_amount=db_order.net_profit, #
#                 description=f"P/L for closing order {db_order.order_id}", #
#                 transaction_time=transaction_time
#             )
#             wallet_profit_loss = Wallet(**profit_loss_wallet_entry_data.model_dump()) #
#             wallet_profit_loss.transaction_id = str(uuid.uuid4()) #
#             db.add(wallet_profit_loss)
#             logger.info(f"Prepared Wallet record for Profit/Loss for order {db_order.order_id}, amount {db_order.net_profit}, transaction_id {wallet_profit_loss.transaction_id}.")

#         # Commission for the whole trade (entry + exit) is recorded as one transaction here.
#         if total_commission_for_trade is not None and total_commission_for_trade > Decimal("0.0"): #
#              commission_wallet_entry_data = WalletCreate( #
#                 user_id=current_user.id,
#                 symbol=order_symbol,
#                 order_quantity=quantity,
#                 transaction_type="Commission", #
#                 is_approved=1, #
#                 order_type=order_type, #
#                 transaction_amount=-total_commission_for_trade, # Negative as it's a debit
#                 description=f"Total commission for order {db_order.order_id}", #
#                 transaction_time=transaction_time
#             )
#              wallet_commission = Wallet(**commission_wallet_entry_data.model_dump()) #
#              wallet_commission.transaction_id = str(uuid.uuid4()) #
#              db.add(wallet_commission)
#              logger.info(f"Prepared Wallet record for Total Commission for order {db_order.order_id}, amount {-total_commission_for_trade}, transaction_id {wallet_commission.transaction_id}.")
        
#         # Commit the transaction (order update, user balance/margin update, and new wallet records)
#         # The commit will happen when the `async with db.begin_nested():` block exits successfully.
#         # Or use `await db.commit()` if not using `begin_nested` for some reason for this part.
#         # Since we locked user, it's good to commit soon after updates.
#         # The outer db session from Depends(get_db) will handle the main commit.
#         # begin_nested helps ensure this group of operations is atomic.

#     # Refresh the order and user objects after the (presumed) commit by the session manager
#     # This might be tricky with nested transactions; often, refresh is done after the top-level commit.
#     # For now, we assume the session management handles this. If explicit commit was here, then refresh.
#     # Let's add explicit commit and refresh for clarity within this logical unit of work.
#     await db.commit() # Commit changes for order, user, wallet
    
#     await db.refresh(db_order)
#     await db.refresh(db_user_locked)

#     logger.info(f"Order {db_order.order_id} closed successfully for user {current_user.id}. Wallet balance: {db_user_locked.wallet_balance}, Margin: {db_user_locked.margin}.")

#     # --- START: WebSocket responsiveness logic ---

#     # 1. Update user_data in Redis cache with the latest balance and margin
#     user_data_to_cache = { #
#         "id": db_user_locked.id,
#         "group_name": getattr(db_user_locked, 'group_name', 'default'),
#         "leverage": db_user_locked.leverage, # This is Decimal
#         "wallet_balance": db_user_locked.wallet_balance, # This is Decimal
#         "margin": db_user_locked.margin # This is Decimal
#     }
#     # set_user_data_cache expects data that can be JSON serialized (Decimals become strings)
#     await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache) #
#     logger.debug(f"Updated user data cache for user {db_user_locked.id} after order closing.")

#     # 2. Fetch all current open positions for this user (excluding the one just closed)
#     open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_locked.id) #
#     updated_positions_data = [] #
#     for pos in open_positions_orm: #
#          pos_dict = {} #
#          for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']: #
#               value = getattr(pos, attr, None) #
#               # Decimal values are already handled by DecimalEncoder in set_user_portfolio_cache or by client
#               pos_dict[attr] = value #
#          pos_dict['profit_loss'] = "0.0" # Placeholder, will be calculated by broadcaster
#          updated_positions_data.append(pos_dict) #

#     # 3. Update user_portfolio in Redis cache
#     user_portfolio_data = { #
#          "balance": db_user_locked.wallet_balance, # Decimal
#          "equity": "0.0", # Placeholder
#          "margin": db_user_locked.margin, # Decimal
#          "free_margin": "0.0", # Placeholder
#          "profit_loss": "0.0", # Placeholder
#          "positions": updated_positions_data # List of dicts, Decimals will be handled by encoder
#     }
#     await set_user_portfolio_cache(redis_client, db_user_locked.id, user_portfolio_data) #
#     logger.debug(f"Updated user portfolio cache for user {db_user_locked.id} after order closing.")

#     # 4. Signal the broadcaster to send account updates for this specific user
#     account_update_signal = { #
#         "type": "account_update_signal",
#         "user_id": db_user_locked.id
#     }
#     await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal, cls=DecimalEncoder)) # Use DecimalEncoder for publishing if data contains Decimals
#     logger.info(f"Published account update signal for user {db_user_locked.id} after order closing.")

#     # --- END: WebSocket responsiveness logic ---

    # return OrderResponse.model_validate(db_order)

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
    current_user: User = Depends(get_current_user)
):
    # Removed internal imports for cache, portfolio_calculator, order_processing as they are top-level now or covered.

    order_id = close_request.order_id
    try:
        close_price = Decimal(str(close_request.close_price))
        if close_price <= Decimal("0"):
            raise HTTPException(status_code=400, detail="Close price must be positive.")
    except InvalidOperation:
        raise HTTPException(status_code=400, detail="Invalid close price format.")

    logger.info(f"Received request to close order {order_id} for user {current_user.id} with close price {close_price}.")

    async with db.begin_nested(): # Using nested transaction for the main logic
        # 1. Validate and Fetch Order
        db_order = await crud_order.get_order_by_id(db, order_id=order_id)
        if db_order is None:
            raise HTTPException(status_code=404, detail="Order not found.")
        if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
            raise HTTPException(status_code=403, detail="Not authorized to close this order.")
        if db_order.order_status != 'OPEN':
            raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

        order_symbol = db_order.order_company_name.upper()
        quantity = Decimal(str(db_order.order_quantity))
        entry_price = Decimal(str(db_order.order_price))
        order_type = db_order.order_type.upper()
        user_group_name = getattr(current_user, 'group_name', 'default')

        # 2. Lock user record
        db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
        if db_user_locked is None:
            # This should ideally not happen if current_user is valid, but good for safety
            logger.error(f"Could not retrieve and lock user record for user ID: {current_user.id}")
            raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")

        # 3. Margin Recalculation Logic
        all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
            db=db,
            user_id=current_user.id,
            symbol=order_symbol
        )
        margin_before_recalc = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=current_user.id,
            symbol=order_symbol,
            open_positions_for_symbol=all_open_orders_for_symbol
        )

        # Current total margin of the user before this specific symbol's contribution is adjusted
        current_overall_margin = Decimal(str(db_user_locked.margin))
        non_symbol_margin = current_overall_margin - margin_before_recalc

        remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
        margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution(
            db=db,
            redis_client=redis_client,
            user_id=current_user.id,
            symbol=order_symbol,
            open_positions_for_symbol=remaining_orders_for_symbol_after_close
        )
        
        # New overall margin: non-symbol part + new calculated margin for this symbol
        db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
        logger.debug(f"Order {order_id} close: User {current_user.id} margin updated from {current_overall_margin} to {db_user_locked.margin}. Symbol margin change: {margin_before_recalc} -> {margin_after_symbol_recalc}")


        # 4. Fetch symbol info (ExternalSymbolInfo)
        symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
        symbol_info_result = await db.execute(symbol_info_stmt)
        ext_symbol_info = symbol_info_result.scalars().first()
        if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
            logger.error(f"Missing critical ExternalSymbolInfo (contract_size, profit currency) for symbol: {order_symbol}")
            raise HTTPException(status_code=500, detail=f"Missing contract size or profit currency for symbol {order_symbol}.")
        contract_size = Decimal(str(ext_symbol_info.contract_size))
        profit_currency = ext_symbol_info.profit.upper()
        if contract_size <= Decimal("0"):
             logger.error(f"Invalid contract size ({contract_size}) for symbol: {order_symbol}")
             raise HTTPException(status_code=500, detail=f"Invalid contract size for symbol {order_symbol}.")


        # 5. Calculate commission for closing (if applicable, though typically on open or both)
        group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
        if not group_settings:
            logger.error(f"Group settings not found for group '{user_group_name}', symbol '{order_symbol}'. Cannot calculate commission for close.")
            raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")

        commission_type = int(group_settings.get('commision_type', -1))
        commission_value_type = int(group_settings.get('commision_value_type', -1))
        commission_rate = Decimal(str(group_settings.get('commision', "0.0")))

        exit_commission = Decimal("0.0")
        if commission_type in [0, 2]: # 0: Every Trade, 2: Out
            if commission_value_type == 0: # Per lot
                exit_commission = quantity * commission_rate
            elif commission_value_type == 1: # Percent of closing price
                calculated_exit_contract_value = quantity * contract_size * close_price
                if calculated_exit_contract_value <= Decimal("0.0"):
                     logger.warning(f"Calculated exit contract value is zero or negative ({calculated_exit_contract_value}) for order {order_id}. Cannot calculate percentage exit commission.")
                     exit_commission = Decimal("0.0")
                else:
                     exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
            else:
                logger.warning(f"Unknown commission_value_type: {commission_value_type} for order {order_id}")
        
        entry_commission_recalc = Decimal("0.0")
        if commission_type in [0, 1]: # Recalculate entry commission for total
            if commission_value_type == 0: # Per lot
                entry_commission_recalc = quantity * commission_rate
            elif commission_value_type == 1: # Percent of entry price
                entry_commission_recalc = ((commission_rate * entry_price) / Decimal("100")) * quantity
        
        total_commission_for_trade = (entry_commission_recalc + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


        # 6. Calculate raw PnL (in native profit currency)
        if order_type == "BUY":
            profit = (close_price - entry_price) * quantity * contract_size
        elif order_type == "SELL":
            profit = (entry_price - close_price) * quantity * contract_size
        else:
            logger.error(f"Invalid order type '{order_type}' found for order {order_id}")
            raise HTTPException(status_code=500, detail="Invalid order type found in database.")

        # 7. Convert profit to USD using the updated _convert_to_usd (raw prices)
        logger.debug(f"Calling _convert_to_usd for PnL for order {db_order.order_id}. Amount: {profit}, Currency: {profit_currency}")
        profit_usd = await _convert_to_usd(
            amount=profit,
            from_currency=profit_currency,
            user_id=current_user.id,
            position_id=db_order.order_id,
            value_description="PnL on Close"
        )
        
        # Critical check: If conversion was needed (profit_currency != USD) but failed (profit_usd is still original `profit`)
        # and no raw rates were available, then raise an error.
        if profit_currency != "USD" and profit_usd == profit:
            direct_raw_symbol = f"{profit_currency}USD"
            indirect_raw_symbol = f"USD{profit_currency}"
            direct_data = get_latest_market_data(direct_raw_symbol)
            indirect_data = get_latest_market_data(indirect_raw_symbol)

            has_direct_rate = direct_data and 'b' in direct_data and direct_data['b'] is not None
            has_indirect_rate = indirect_data and 'o' in indirect_data and indirect_data['o'] is not None

            if not (has_direct_rate or has_indirect_rate):
                logger.error(f"Order {db_order.order_id}: PnL conversion from {profit_currency} to USD failed. Raw market rates for conversion appear to be missing. Original PnL: {profit}")
                raise HTTPException(status_code=500, detail=f"Critical error: Could not convert PnL from {profit_currency} to USD due to missing raw market rates.")
            else:
                logger.warning(f"Order {db_order.order_id}: PnL conversion from {profit_currency} returned original amount, but raw rates might exist. Original PnL: {profit}, Converted: {profit_usd}. Proceeding with this value.")


        # 8. Final updates to order and user balance
        db_order.order_status = "CLOSED"
        db_order.close_price = close_price
        db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        db_order.swap = db_order.swap or Decimal("0.0") # Ensure swap is not None
        db_order.commission = total_commission_for_trade # Store total commission for the trade's lifecycle

        # Update user's wallet balance
        # PnL is already in USD. Commission is calculated in USD.
        original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
        
        # Deduct swap value from the balance.
        # Ensure swap is not None before subtraction.
        swap_amount = db_order.swap if db_order.swap is not None else Decimal("0.0")

        db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00001"), rounding=ROUND_HALF_UP)
        logger.info(f"Order {order_id} close: User {current_user.id} balance updated from {original_wallet_balance} to {db_user_locked.wallet_balance}. PnL(USD): {db_order.net_profit}, TotalComm(USD): {total_commission_for_trade}, Swap(USD): {swap_amount}")


        # 9. Create Wallet Transaction Records
        transaction_time = datetime.datetime.now(datetime.timezone.utc)

        if db_order.net_profit is not None and db_order.net_profit != Decimal("0.0"):
            profit_loss_wallet_entry_data = WalletCreate(
                user_id=current_user.id,
                symbol=order_symbol,
                order_quantity=quantity,
                transaction_type="Profit/Loss",
                is_approved=1,
                order_type=order_type,
                transaction_amount=db_order.net_profit,
                description=f"P/L for closing order {db_order.order_id}",
                transaction_time=transaction_time
            )
            wallet_profit_loss = Wallet(**profit_loss_wallet_entry_data.model_dump())
            wallet_profit_loss.transaction_id = str(uuid.uuid4())
            db.add(wallet_profit_loss)
            logger.info(f"Prepared Wallet record for Profit/Loss for order {db_order.order_id}, amount {db_order.net_profit}, transaction_id {wallet_profit_loss.transaction_id}.")

        # Commission for the whole trade (entry + exit) is recorded as one transaction here.
        if total_commission_for_trade is not None and total_commission_for_trade > Decimal("0.0"):
             commission_wallet_entry_data = WalletCreate(
                user_id=current_user.id,
                symbol=order_symbol,
                order_quantity=quantity,
                transaction_type="Commission",
                is_approved=1,
                order_type=order_type,
                transaction_amount=-total_commission_for_trade, # Negative as it's a debit
                description=f"Total commission for order {db_order.order_id}",
                transaction_time=transaction_time
            )
             wallet_commission = Wallet(**commission_wallet_entry_data.model_dump())
             wallet_commission.transaction_id = str(uuid.uuid4())
             db.add(wallet_commission)
             logger.info(f"Prepared Wallet record for Total Commission for order {db_order.order_id}, amount {-total_commission_for_trade}, transaction_id {wallet_commission.transaction_id}.")
        
        # Add a Wallet transaction for the swap amount
        if swap_amount is not None and swap_amount != Decimal("0.0"):
            swap_wallet_entry_data = WalletCreate(
                user_id=current_user.id,
                symbol=order_symbol,
                order_quantity=quantity, # Quantity of the order
                transaction_type="Swap",
                is_approved=1,
                order_type=order_type,
                transaction_amount=-swap_amount, # Negative as it's a debit from the user's balance
                description=f"Swap charges for order {db_order.order_id}",
                transaction_time=transaction_time
            )
            wallet_swap = Wallet(**swap_wallet_entry_data.model_dump())
            wallet_swap.transaction_id = str(uuid.uuid4())
            db.add(wallet_swap)
            logger.info(f"Prepared Wallet record for Swap for order {db_order.order_id}, amount {-swap_amount}, transaction_id {wallet_swap.transaction_id}.")

    await db.commit()
    
    await db.refresh(db_order)
    await db.refresh(db_user_locked)

    logger.info(f"Order {db_order.order_id} closed successfully for user {current_user.id}. Wallet balance: {db_user_locked.wallet_balance}, Margin: {db_user_locked.margin}.")

    # --- START: WebSocket responsiveness logic ---

    # 1. Update user_data in Redis cache
    user_data_to_cache = {
        "id": db_user_locked.id,
        "group_name": getattr(db_user_locked, 'group_name', 'default'),
        "leverage": db_user_locked.leverage,
        "wallet_balance": db_user_locked.wallet_balance,
        "margin": db_user_locked.margin
    }
    await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)
    logger.debug(f"Updated user data cache for user {db_user_locked.id} after order closing.")

    # 2. Fetch all current open positions for this user (excluding the one just closed)
    open_positions_orm = await crud_order.get_all_open_orders_by_user_id(db, db_user_locked.id)
    updated_positions_data = []
    for pos in open_positions_orm:
         pos_dict = {}
         for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']:
              value = getattr(pos, attr, None)
              pos_dict[attr] = value
         pos_dict['profit_loss'] = "0.0"
         updated_positions_data.append(pos_dict)

    # 3. Update user_portfolio in Redis cache
    user_portfolio_data = {
         "balance": db_user_locked.wallet_balance,
         "equity": "0.0",
         "margin": db_user_locked.margin,
         "free_margin": "0.0",
         "profit_loss": "0.0",
         "positions": updated_positions_data
    }
    await set_user_portfolio_cache(redis_client, db_user_locked.id, user_portfolio_data)
    logger.debug(f"Updated user portfolio cache for user {db_user_locked.id} after order closing.")

    # 4. Signal the broadcaster to send account updates for this specific user
    account_update_signal = {
        "type": "account_update_signal",
        "user_id": db_user_locked.id
    }
    await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal, cls=DecimalEncoder))
    logger.info(f"Published account update signal for user {db_user_locked.id} after order closing.")

    # --- END: WebSocket responsiveness logic ---

    return OrderResponse.model_validate(db_order)

# --- NEW Cancel Pending Order Endpoint ---
from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation
from typing import Optional
from pydantic import BaseModel

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, UserOrder
from app.schemas.user import StatusResponse
from app.core.security import get_current_user
from app.crud import crud_order, user as crud_user
from app.services.order_processing import calculate_total_symbol_margin_contribution
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL
import json

logger = logging.getLogger(__name__)

class CancelOrderRequest(BaseModel):
    order_id: str
    cancel_message: Optional[str] = None

@router.post(
    "/cancel",
    response_model=StatusResponse,
    summary="Cancel a pending order",
    description="Cancels a PENDING order, sets its status to 'CANCELED', updates margin accordingly."
)
async def cancel_pending_order(
    cancel_request: CancelOrderRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    order_id = cancel_request.order_id
    cancel_message = cancel_request.cancel_message or "Cancelled by user"

    db_order = await crud_order.get_order_by_id(db, order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    if db_order.order_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Unauthorized to cancel this order")
    if db_order.order_status not in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
        raise HTTPException(status_code=400, detail="Only pending orders (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP) can be cancelled")

    try:
        async with db.begin_nested():
            symbol = db_order.order_company_name
            existing_orders = await crud_order.get_open_and_pending_orders_by_user_id_and_symbol(
                db, current_user.id, symbol
            )

            margin_before = await calculate_total_symbol_margin_contribution(
                db, redis_client, current_user.id, symbol, existing_orders
            )

            updated_orders = [o for o in existing_orders if o.order_id != order_id]
            margin_after = await calculate_total_symbol_margin_contribution(
                db, redis_client, current_user.id, symbol, updated_orders
            )

            margin_released = max(Decimal("0.0"), margin_before - margin_after)
            logger.info(f"User {current_user.id}: Cancelled pending order {order_id}, margin released: {margin_released}")

            db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
            if db_user_locked.margin >= margin_released:
                db_user_locked.margin -= margin_released
            else:
                db_user_locked.margin = Decimal("0.0")

            db_order.order_status = "CANCELED"
            db_order.cancel_message = cancel_message

        # Update Redis cache
        await db.refresh(db_user_locked)
        user_data_to_cache = {
            "id": current_user.id,
            "group_name": db_user_locked.group_name,
            "leverage": db_user_locked.leverage,
            "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin
        }
        await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)

        all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
        open_positions = []
        pending_positions = []
        total_margin = Decimal("0.0")
        for pos in all_orders:
            pos_dict = {k: str(getattr(pos, k)) if isinstance(getattr(pos, k), Decimal) else getattr(pos, k)
                        for k in ["order_id", "order_company_name", "order_type", "order_quantity",
                                  "order_price", "margin", "contract_value", "stop_loss", "take_profit"]}
            pos_dict["profit_loss"] = "0.0"
            if pos.order_status == "OPEN":
                open_positions.append(pos_dict)
            elif pos.order_status in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
                pending_positions.append(pos_dict)
            total_margin += Decimal(str(pos.margin or 0.0)) if pos.order_status == "OPEN" or pos.order_status in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"] else Decimal("0.0")

        user_portfolio_data = {
            "balance": str(db_user_locked.wallet_balance),
            "equity": "0.0",
            "margin": str(total_margin),
            "free_margin": str(db_user_locked.wallet_balance - total_margin),
            "profit_loss": "0.0",
            "positions": open_positions,
            "pending_positions": pending_positions
        }
        await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)

        # Signal frontend
        account_update_signal = {
            "type": "account_update_signal",
            "user_id": current_user.id
        }
        await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal))

        return StatusResponse(status="success", message="Pending order canceled successfully")

    except Exception as e:
        logger.error(f"Failed to cancel pending order {order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error canceling the pending order")

# --- Modify Pending Order Endpoint ---
from app.schemas.user import StatusResponse

class ModifyOrderRequest(BaseModel):
    order_id: str
    new_price: Decimal
    new_quantity: Decimal

@router.post(
    "/modify",
    response_model=StatusResponse,
    summary="Modify a pending order",
    description="Modifies a PENDING order (BUY_LIMIT, SELL_LIMIT, BUY_STOP, SELL_STOP), recalculates margin and updates user state."
)
async def modify_pending_order(
    request: ModifyOrderRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    db_order = await crud_order.get_order_by_id(db, request.order_id)
    if not db_order:
        raise HTTPException(status_code=404, detail="Order not found")
    if db_order.order_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Unauthorized")
    if db_order.order_status not in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
        raise HTTPException(status_code=400, detail="Only pending orders can be modified")

    try:
        async with db.begin_nested():
            symbol = db_order.order_company_name
            existing_orders = await crud_order.get_open_and_pending_orders_by_user_id_and_symbol(db, current_user.id, symbol)

            # Margin before update
            margin_before = await calculate_total_symbol_margin_contribution(
                db, redis_client, current_user.id, symbol, existing_orders
            )

            # Temporarily update order values for margin-after simulation
            dummy_updated_order = UserOrder(
                order_quantity=request.new_quantity,
                order_type=db_order.order_type,
                margin=Decimal("0.0")  # Will be replaced
            )

            # Recalculate margin and contract value
            from app.services.margin_calculator import calculate_single_order_margin
            margin_usd, adjusted_price, contract_value = await calculate_single_order_margin(
                db=db,
                redis_client=redis_client,
                user_id=current_user.id,
                order_quantity=request.new_quantity,
                order_price=request.new_price,
                symbol=symbol,
                order_type=db_order.order_type
            )

            if margin_usd is None or adjusted_price is None or contract_value is None:
                raise HTTPException(status_code=400, detail="Failed to recalculate margin or price")

            dummy_updated_order.margin = margin_usd
            updated_orders = [o if o.order_id != request.order_id else dummy_updated_order for o in existing_orders]

            margin_after = await calculate_total_symbol_margin_contribution(
                db, redis_client, current_user.id, symbol, updated_orders
            )

            delta_margin = margin_after - margin_before
            delta_margin = delta_margin.quantize(Decimal("0.00000001"))

            db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id)
            if delta_margin > 0:
                if db_user_locked.wallet_balance < db_user_locked.margin + delta_margin:
                    raise HTTPException(status_code=400, detail="Insufficient free margin for modification")
                db_user_locked.margin += delta_margin
            else:
                db_user_locked.margin += delta_margin  # negative value releases margin
                if db_user_locked.margin < 0:
                    db_user_locked.margin = Decimal("0.0")

            # Update the DB order
            db_order.order_price = adjusted_price
            db_order.order_quantity = request.new_quantity
            db_order.contract_value = contract_value
            db_order.margin = margin_usd

        # Update Redis
        await db.refresh(db_user_locked)
        user_data_to_cache = {
            "id": current_user.id,
            "group_name": db_user_locked.group_name,
            "leverage": db_user_locked.leverage,
            "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin
        }
        await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)

        all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
        open_positions = []
        pending_positions = []
        total_margin = Decimal("0.0")

        for pos in all_orders:
            pos_dict = {k: str(getattr(pos, k)) if isinstance(getattr(pos, k), Decimal) else getattr(pos, k)
                        for k in ["order_id", "order_company_name", "order_type", "order_quantity",
                                  "order_price", "margin", "contract_value", "stop_loss", "take_profit"]}
            pos_dict["profit_loss"] = "0.0"
            if pos.order_status == "OPEN":
                open_positions.append(pos_dict)
            elif pos.order_status in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
                pending_positions.append(pos_dict)
            total_margin += Decimal(str(pos.margin or 0.0)) if pos.order_status == "OPEN" or pos.order_status in ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"] else Decimal("0.0")

        user_portfolio_data = {
            "balance": str(db_user_locked.wallet_balance),
            "equity": "0.0",
            "margin": str(total_margin),
            "free_margin": str(db_user_locked.wallet_balance - total_margin),
            "profit_loss": "0.0",
            "positions": open_positions,
            "pending_positions": pending_positions
        }
        await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)

        await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps({
            "type": "account_update_signal",
            "user_id": current_user.id
        }))

        return StatusResponse(status="success", message="Pending order modified successfully")

    except Exception as e:
        logger.error(f"Error modifying pending order {request.order_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error modifying the pending order")


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
import json # Import json for publishing


from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
# Import Wallet model
from app.database.models import User, UserOrder, ExternalSymbolInfo, Wallet
# Import WalletCreate schema
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate # Import WalletCreate schema

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
from app.core.cache import (
    get_adjusted_market_price_cache,
    get_group_symbol_settings_cache,
    set_user_data_cache, # Import for cache update
    set_user_portfolio_cache, # Import for cache update
    DecimalEncoder # Import for JSON serialization
)

from app.core.security import get_current_user
from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # Import the channel name


logger = logging.getLogger(__name__)



# ... (other existing endpoints) ...

@router.patch(
    "/update-tp-sl",
    response_model=OrderResponse,
    summary="Update Stop Loss / Take Profit for an Order",
    description="Updates stop loss and take profit values and their respective IDs for a specific order."
)
async def update_stoploss_takeprofit(
    request: UpdateStopLossTakeProfitRequest,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client), # Inject Redis client
    current_user: User = Depends(get_current_user)
):
    # Retrieve the order
    db_order = await crud_order.get_order_by_id(db, request.order_id)
    if db_order is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Order not found.")

    # Authorization check
    if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Not authorized to update this order.")

    # Call the CRUD function to update TP/SL
    updated_order = await crud_order.update_tp_sl_for_order(
        db=db,
        order_id=request.order_id,
        stop_loss=request.stop_loss,
        take_profit=request.take_profit,
        stoploss_id=request.stoploss_id,
        takeprofit_id=request.takeprofit_id
    )

    # Check if the update was successful
    if updated_order is None:
        # This case handles scenarios where the CRUD function might return None
        # even after the order was initially found (e.g., if there's an internal DB error during update)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to update order's Stop Loss/Take Profit.")

    # Ensure to refresh the current_user object to get the latest margin/wallet_balance
    await db.refresh(current_user)

    # After successful update, update user portfolio/data cache
    # and publish a signal for real-time updates via WebSocket.

    # 1. Update user_data in Redis cache
    user_data_to_cache = {
        "id": current_user.id,
        "group_name": getattr(current_user, 'group_name', 'default'),
        "leverage": current_user.leverage,
        "wallet_balance": current_user.wallet_balance,
        "margin": current_user.margin
    }
    await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
    logger.debug(f"Updated user data cache for user {current_user.id} after TP/SL update.")

    # 2. Fetch all current open and pending positions for this user
    all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id)
    updated_open_positions = []
    updated_pending_positions = []
    total_margin = Decimal("0.0")

    for pos in all_orders:
        pos_dict = {}
        # Ensure all fields expected by OrderResponse are included,
        # converting Decimals to string for JSON serialization
        for attr in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price',
                     'contract_value', 'margin', 'net_profit', 'close_price', 'swap', 'commission',
                     'stop_loss', 'take_profit', 'cancel_message', 'close_message', 'status',
                     'takeprofit_id', 'stoploss_id', 'order_user_id', 'order_status', 'created_at', 'updated_at', 'id']:
            value = getattr(pos, attr, None)
            if isinstance(value, Decimal):
                pos_dict[attr] = str(value)
            elif isinstance(value, datetime.datetime):
                pos_dict[attr] = value.isoformat()
            else:
                pos_dict[attr] = value

        # 'profit_loss' is typically a calculated field and might not be stored directly
        # You'd calculate it based on current market price vs. order.order_price if needed for display.
        # For cached positions, it's often dynamically calculated on the frontend or a separate service.
        # For simplicity, if not directly from DB, set to '0.0' or calculate if logic is available.
        pos_dict['profit_loss'] = "0.0" # Placeholder, calculate if needed

        if pos.order_status == "OPEN":
            updated_open_positions.append(pos_dict)
        elif pos.order_status == "PENDING":
            updated_pending_positions.append(pos_dict)

        total_margin += Decimal(str(pos.margin or 0.0))

    # 3. Update user_portfolio in Redis cache
    user_portfolio_data = {
        "balance": str(current_user.wallet_balance),
        "equity": "0.0", # This should be calculated real-time or updated by a dedicated service
        "margin": str(total_margin),
        "free_margin": str(current_user.wallet_balance - total_margin),
        "profit_loss": "0.0", # This should be calculated real-time or updated by a dedicated service
        "positions": updated_open_positions,
        "pending_positions": updated_pending_positions
    }
    await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)
    logger.debug(f"Updated user portfolio cache for user {current_user.id} after TP/SL update.")

    # 4. Signal the broadcaster to send account updates for this specific user
    account_update_signal = {
        "type": "account_update_signal",
        "user_id": current_user.id
    }
    await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, json.dumps(account_update_signal))
    logger.info(f"Published account update signal for user {current_user.id} after TP/SL update.")

    # Return the updated order as a response
    return OrderResponse.model_validate(updated_order)


from app.services.margin_calculator import (
    get_live_adjusted_buy_price_for_pair,
    get_live_adjusted_sell_price_for_pair
)
from app.schemas.order import CloseOrderRequest

@router.post("/close-all", response_model=List[OrderResponse], summary="Close all open orders with live prices")
async def close_all_orders_with_live_prices(
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_user)
):
    """
    Closes all open orders for the authenticated user using real live prices.
    Each order is closed with the proper buy/sell adjusted price.
    """
    open_orders = await crud_order.get_all_open_orders_by_user_id(db, current_user.id)
    if not open_orders:
        raise HTTPException(status_code=404, detail="No open orders found to close.")

    closed_orders = []

    for order in open_orders:
        symbol = order.order_company_name.upper()
        user_group = getattr(current_user, "group_name", "default")

        # Determine correct live close price based on order type
        if order.order_type.upper() == "BUY":
            close_price = await get_live_adjusted_sell_price_for_pair(redis_client, symbol, user_group)
        elif order.order_type.upper() == "SELL":
            close_price = await get_live_adjusted_buy_price_for_pair(redis_client, symbol, user_group)
        else:
            continue  # Skip unknown order types

        if not close_price or close_price <= 0:
            logger.warning(f"Skipping order {order.order_id}: no valid live price found for {symbol}")
            continue

        close_request = CloseOrderRequest(
            order_id=order.order_id,
            close_price=close_price
        )

        try:
            closed_order = await close_order(
                close_request=close_request,
                db=db,
                redis_client=redis_client,
                current_user=current_user
            )
            closed_orders.append(closed_order)
        except Exception as e:
            logger.error(f"Error closing order {order.order_id}: {e}")

    return closed_orders

