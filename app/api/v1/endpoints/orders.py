# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any
import json
import uuid
import datetime
import time

from app.database.models import Group, ExternalSymbolInfo

from app.schemas.order import OrderUpdateRequest 
# from app.utils.firebase_push import send_order_to_firebase # Keep if used elsewhere

from app.core.cache import set_user_data_cache, set_user_portfolio_cache, DecimalEncoder, get_group_symbol_settings_cache

from app.utils.validation import enforce_service_user_id_restriction

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest
from app.schemas.user import StatusResponse # For cancel/modify
from app.schemas.wallet import WalletCreate

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution
)
from app.services.portfolio_calculator import _convert_to_usd, calculate_user_portfolio
from app.services.margin_calculator import calculate_single_order_margin

from app.crud import crud_order, user as crud_user
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
from app.crud.group import get_all_symbols_for_group
from app.firebase_stream import get_latest_market_data

from app.core.cache import set_user_data_cache, set_user_portfolio_cache, DecimalEncoder, get_group_symbol_settings_cache
from app.core.security import get_user_from_service_or_user_token, get_current_user
# from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # No longer directly used for account signals here

from sqlalchemy import select

from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

from decimal import Decimal
from typing import Optional, List
from pydantic import BaseModel, Field

class OrderPlacementRequest(BaseModel):
    symbol: str # Corresponds to order_company_name from your description
    order_type: str # E.g., "MARKET", "LIMIT", "STOP", "BUY", "SELL", "BUY_LIMIT", "SELL_LIMIT"
    order_quantity: Decimal = Field(..., gt=0)
    order_price: Decimal # For LIMIT/STOP. For MARKET, can be current market price or 0 if server fetches.
    user_type: str # "live" or "demo" as passed by frontend

    stop_loss: Optional[Decimal] = None
    take_profit: Optional[Decimal] = None
    user_id: Optional[int] = None # For service accounts placing orders for other users.

    class Config:
        json_encoders = {
            Decimal: lambda v: str(v),
        }


# --- orders.py (fully updated with service account support, demo user support, and new margin rules) ---

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request, BackgroundTasks # Added BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, cast
import json
import uuid
import datetime
import time

# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, cast
import json
import uuid
import datetime
import time

from app.utils.firebase_push import send_order_to_firebase
from app.utils.validation import enforce_service_user_id_restriction
from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client
from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet, Group
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest, OrderCreateInternal # Ensure OrderCreateInternal is imported
from app.schemas.user import StatusResponse
from app.schemas.wallet import WalletCreate
from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    # calculate_total_symbol_margin_contribution # Not directly used in place_order
)
# from app.services.portfolio_calculator import _convert_to_usd, calculate_user_portfolio # Not directly used in place_order
# from app.services.margin_calculator import calculate_single_order_margin # Not directly used in place_order

from app.crud import crud_order, user as crud_user, group as crud_group
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
# from app.firebase_stream import get_latest_market_data # Not directly used in place_order

# CORRECTED IMPORT: Added get_user_portfolio_cache
from app.core.cache import (
    set_user_data_cache, get_user_data_cache,
    set_user_portfolio_cache, get_user_portfolio_cache, # Added get_user_portfolio_cache
    DecimalEncoder, get_group_symbol_settings_cache,
    # get_adjusted_market_price_cache # Not directly used in place_order
)
from app.core.security import get_user_from_service_or_user_token
# from app.api.v1.endpoints.market_data_ws import REDIS_MARKET_DATA_CHANNEL # Replaced with user-specific

# from sqlalchemy import select # Not directly used in place_order

# oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token") # Already defined in your snippet if needed elsewhere

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/orders",
    tags=["orders"]
)

# Helper to publish account structure change event (ensure this is defined or imported correctly)
async def publish_account_structure_changed_event(redis_client: Redis, user_id: int):
    user_channel = f"user_updates:{user_id}"
    event_payload = {
        "event_type": "ACCOUNT_STRUCTURE_CHANGED",
        "user_id": user_id,
        "timestamp": time.time()
    }
    await redis_client.publish(user_channel, json.dumps(event_payload))
    logger.info(f"Published ACCOUNT_STRUCTURE_CHANGED event to {user_channel} for user_id {user_id}")

# Helper to determine order model based on user type (ensure this is defined or imported correctly)
def get_order_model(user_or_user_type: User | DemoUser | str):
    user_type_str = ""
    if isinstance(user_or_user_type, str):
        user_type_str = user_or_user_type.lower()
    elif isinstance(user_or_user_type, DemoUser):
        user_type_str = "demo"
    elif isinstance(user_or_user_type, User):
        user_type_str = "live"

    if user_type_str == "demo":
        return DemoUserOrder
    return UserOrder


# app/api/v1/endpoints/orders.py

from fastapi import APIRouter, Depends, HTTPException, status, Body, Request, BackgroundTasks
from sqlalchemy.ext.asyncio import AsyncSession
from redis.asyncio import Redis
import logging
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Optional, List, Dict, Any, cast
import json
import uuid
import datetime
import time

from app.schemas.order import OrderUpdateRequest
from app.utils.firebase_push import send_order_to_firebase # Activated this import

from app.core.cache import (
    set_user_data_cache,
    set_user_portfolio_cache,
    DecimalEncoder,
    get_group_symbol_settings_cache,
    publish_account_structure_changed_event # Assuming this function is in app.core.cache
)

from app.utils.validation import enforce_service_user_id_restriction

from app.database.session import get_db
from app.dependencies.redis_client import get_redis_client

from app.database.models import User, DemoUser, UserOrder, DemoUserOrder, ExternalSymbolInfo, Wallet
from app.schemas.order import OrderPlacementRequest, OrderResponse, CloseOrderRequest, UpdateStopLossTakeProfitRequest
from app.schemas.user import StatusResponse # For cancel/modify
from app.schemas.wallet import WalletCreate

from app.services.order_processing import (
    process_new_order,
    OrderProcessingError,
    InsufficientFundsError,
    calculate_total_symbol_margin_contribution,
    generate_10_digit_id
)
from app.services.portfolio_calculator import _convert_to_usd, calculate_user_portfolio

# Import crud operations
from app.crud import user as crud_user
from app.crud import group as crud_group
from app.crud import crud_order
from app.crud.crud_order import get_order_model # Explicitly import get_order_model # Assuming this function is in app.crud.group
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol


@router.post("/", response_model=OrderResponse, status_code=status.HTTP_201_CREATED)
async def place_order(
    order_request: OrderPlacementRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
):
    logger.info(f"Received order placement request for symbol {order_request.symbol}, user_type: {order_request.user_type} by authenticated user {current_user.id}")

    if order_request.order_quantity <= 0:
        raise HTTPException(status_code=400, detail="Order quantity must be positive.")

    # Initialize target_user to None to satisfy Pylance
    target_user: User | DemoUser | None = None

    if order_request.user_id is not None and hasattr(current_user, 'is_service_account') and current_user.is_service_account:
        logger.info(f"Service account {current_user.id} placing order for target user_id {order_request.user_id} with type {order_request.user_type}")
        if order_request.user_type == "live":
            fetched_user = await crud_user.get_user_by_id(db, order_request.user_id, user_type="live")
            if not fetched_user:
                raise HTTPException(status_code=404, detail=f"Target live user {order_request.user_id} (type: live) not found.")
            target_user = fetched_user
        elif order_request.user_type == "demo":
            fetched_demo_user = await crud_user.get_demo_user_by_id(db, order_request.user_id, user_type="demo")
            if not fetched_demo_user:
                raise HTTPException(status_code=404, detail=f"Target demo user {order_request.user_id} (type: demo) not found.")
            target_user = fetched_demo_user
        else:
            raise HTTPException(status_code=400, detail=f"Invalid user_type '{order_request.user_type}' for service account operation.")
    else:
        target_user = current_user
        if (order_request.user_type == "live" and not isinstance(target_user, User)) or \
           (order_request.user_type == "demo" and not isinstance(target_user, DemoUser)):
            raise HTTPException(status_code=400, detail=f"Mismatch between requested user_type '{order_request.user_type}' and authenticated user type.")

    # At this point, target_user should be assigned if no HTTPException was raised.
    # Adding an explicit check, though logically redundant if HTTPExceptions halt execution.
    if target_user is None:
        logger.error("Critical internal error: target_user was not assigned prior to use.")
        raise HTTPException(status_code=500, detail="Internal server error processing user context.")


    # --- Barclays Flow Check (Only for Live Users) ---
    if order_request.user_type == "live":
        # Since order_request.user_type == "live", target_user must be a User instance here
        # if initial checks passed.
        live_target_user = cast(User, target_user)
        
        group_list = await crud_group.get_group_by_name(db, live_target_user.group_name)
        if not group_list:
            logger.error(f"Group '{live_target_user.group_name}' not found for live user {live_target_user.id}.")
            raise HTTPException(status_code=500, detail="User group configuration not found.")
        group = group_list[0] # Get the first (and presumably only) group from the list

        if not group: # This check is redundant given the above `if not group_list`
            logger.error(f"Group '{live_target_user.group_name}' not found for live user {live_target_user.id}.")
            raise HTTPException(status_code=500, detail="User group configuration not found.")

        sending_orders_value = getattr(group, 'sending_orders', None)
        logger.info(f"User {live_target_user.id} in group '{group.name}', sending_orders: '{sending_orders_value}'")

        if sending_orders_value == "barclays":
            logger.info(f"Barclays flow activated for user {live_target_user.id}, order for {order_request.symbol}.")
            db_order_status = "PROCESSING"
            
            ext_symbol_info = await get_external_symbol_info_by_symbol(db, fix_symbol=order_request.symbol)
            contract_size = Decimal("100000") 
            if ext_symbol_info and ext_symbol_info.contract_size is not None:
                contract_size = Decimal(str(ext_symbol_info.contract_size))
            else:
                group_symbol_settings = await get_group_symbol_settings_cache(redis_client, live_target_user.group_name, order_request.symbol.upper())
                if group_symbol_settings and 'contract_size' in group_symbol_settings:
                    contract_size = Decimal(str(group_symbol_settings['contract_size']))
                else:
                    logger.warning(f"Contract size not found for {order_request.symbol} in ExternalSymbolInfo or GroupSettings. Using default {contract_size} for Barclays PROCESSING order.")
            
            calculated_contract_value = order_request.order_quantity * contract_size

            # --- Barclays Margin Calculation (do not add to user margin) ---
            adjusted_price = await get_live_adjusted_buy_price_for_pair(redis_client, order_request.symbol, live_target_user.group_name)
            if adjusted_price is None:
                # fallback to order price if adjusted price not found
                adjusted_price = Decimal(str(order_request.order_price))

            user_leverage = getattr(live_target_user, 'leverage', None)
            if user_leverage is None or Decimal(user_leverage) <= 0:
                logger.error(f"User {live_target_user.id} has invalid leverage for margin calculation.")
                user_leverage = Decimal(1)
            else:
                user_leverage = Decimal(str(user_leverage))

            barclays_margin = (contract_size * Decimal(str(order_request.order_quantity)) * adjusted_price) / user_leverage
            barclays_margin = barclays_margin.quantize(Decimal('0.01'), rounding=ROUND_HALF_UP)

            order_data_internal = OrderCreateInternal(
                order_id=generate_10_digit_id(),
                order_user_id=live_target_user.id,
                order_company_name=order_request.symbol,
                order_type=order_request.order_type,
                order_status=db_order_status,
                order_price=order_request.order_price,
                order_quantity=order_request.order_quantity,
                contract_value=calculated_contract_value,
                margin=barclays_margin,
                stop_loss=order_request.stop_loss,
                take_profit=order_request.take_profit,
            )
            
            new_db_order = await crud_order.create_order(db=db, order_data=order_data_internal.model_dump(), order_model=UserOrder)
            await db.commit()
            await db.refresh(new_db_order)

            background_tasks.add_task(send_order_to_firebase, new_db_order, account_type="live")
            
            # Corrected use of get_user_portfolio_cache
            user_portfolio = await get_user_portfolio_cache(redis_client, live_target_user.id)
            if user_portfolio is None: 
                user_portfolio = {"balance": str(live_target_user.wallet_balance), "equity":str(live_target_user.wallet_balance), "margin": str(live_target_user.margin), "free_margin":str(live_target_user.wallet_balance - live_target_user.margin), "profit_loss": "0.0", "positions": [], "pending_positions": [], "processing_positions": []}
            if "processing_positions" not in user_portfolio: 
                user_portfolio["processing_positions"] = []
            
            processing_order_dict = {
                k: str(v) if isinstance(v := getattr(new_db_order, k), Decimal) else v
                for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']
            }
            processing_order_dict["order_status"] = db_order_status
            user_portfolio["processing_positions"].append(processing_order_dict)
            
            # Update other portfolio fields if necessary based on this non-margin-affecting order
            user_portfolio["balance"] = str(live_target_user.wallet_balance) # Balance unchanged by processing order
            user_portfolio["margin"] = str(live_target_user.margin) # Total margin unchanged by processing order
            user_portfolio["free_margin"] = str(live_target_user.wallet_balance - live_target_user.margin)
            # Equity and P&L are not directly affected by a new "PROCESSING" order with 0 margin

            await set_user_portfolio_cache(redis_client, live_target_user.id, user_portfolio)
            
            await publish_account_structure_changed_event(redis_client, live_target_user.id)
            return OrderResponse.model_validate(new_db_order, from_attributes=True) # Applied the fix here

    # --- Standard Flow (Demo Users OR Live Users not on Barclays flow) ---
    # Ensure target_user is not None before proceeding (should be guaranteed by checks above)
    if target_user is None: # Should be unreachable
        raise HTTPException(status_code=500, detail="Internal Server Error: User context lost in standard flow.")

    logger.info(f"Standard order processing flow for user {target_user.id} ({order_request.user_type}), symbol {order_request.symbol}.")
    order_model_class = get_order_model(target_user) 

    try:
        if isinstance(target_user, DemoUser):
            from app.crud.user import get_demo_user_by_id_with_lock
            from app.crud.crud_order import create_order
            new_db_order = await process_new_order(
                db=db,
                redis_client=redis_client,
                user=target_user,
                order_request=order_request,
                order_model=DemoUserOrder,
                create_order_fn=create_order,
                get_user_by_id_with_lock_fn=get_demo_user_by_id_with_lock
            )
        else:
            from app.crud.user import get_user_by_id_with_lock
            from app.crud.crud_order import create_order
            new_db_order = await process_new_order(
                db=db,
                redis_client=redis_client,
                user=target_user,
                order_request=order_request,
                order_model=UserOrder,
                create_order_fn=create_order,
                get_user_by_id_with_lock_fn=get_user_by_id_with_lock
            )
        # process_new_order is expected to commit DB changes for user and order
        await db.refresh(target_user) # Refresh to get latest user.margin and user.wallet_balance

        user_data_to_cache = {
            "id": target_user.id,
            "group_name": getattr(target_user, 'group_name', 'default'),
            "leverage": target_user.leverage,
            "wallet_balance": target_user.wallet_balance,
            "margin": target_user.margin 
        }
        await set_user_data_cache(redis_client, target_user.id, user_data_to_cache)

        all_orders = await crud_order.get_orders_by_user_id(db, user_id=target_user.id, order_model=order_model_class, limit=1000)
        updated_open_positions = []
        updated_pending_positions = []
        
        for pos in all_orders:
            pos_dict = {k: str(v) if isinstance(v := getattr(pos, k), Decimal) else v
                        for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit']}
            pos_dict['profit_loss'] = "0.0" 

            if pos.order_status == "OPEN":
                pos_dict['margin'] = str(pos.margin) 
                updated_open_positions.append(pos_dict)
            elif pos.order_status in ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
                pos_dict['margin'] = "0.0" 
                updated_pending_positions.append(pos_dict)
        
        # Construct portfolio data based on refreshed target_user and order lists
        current_total_margin = target_user.margin
        current_balance = target_user.wallet_balance
        # A simple P&L sum could be added here if needed, or rely on WebSocket for live P&L
        # For equity: balance + P&L of open positions. P&L is complex here, so often calculated live.
        # For simplicity, portfolio cache here will have P/L as 0 and equity based on that.
        
        user_portfolio_data = {
            "balance": str(current_balance),
            "equity": str(current_balance), # Simplified: Equity = Balance if P/L is not calculated here
            "margin": str(current_total_margin), 
            "free_margin": str(current_balance - current_total_margin),
            "profit_loss": "0.0", 
            "positions": updated_open_positions,
            "pending_positions": updated_pending_positions
        }
        # Preserve processing_positions if they exist from a previous Barclays flow for the same user
        cached_portfolio_before = await get_user_portfolio_cache(redis_client, target_user.id)
        if cached_portfolio_before and "processing_positions" in cached_portfolio_before:
            user_portfolio_data["processing_positions"] = cached_portfolio_before["processing_positions"]
        
        await set_user_portfolio_cache(redis_client, target_user.id, user_portfolio_data)


        if isinstance(target_user, User): 
            background_tasks.add_task(send_order_to_firebase, new_db_order, account_type="live")
            logger.info(f"Standard live order {new_db_order.order_id} sent to Firebase BG task for user {target_user.id}.")

        await publish_account_structure_changed_event(redis_client, target_user.id)
        return OrderResponse.model_validate(new_db_order, from_attributes=True)

    except InsufficientFundsError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except OrderProcessingError as e:
        logger.error(f"Standard order processing error for user {target_user.id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="An error occurred while processing the order.")
    except Exception as e:
        logger.error(f"Unexpected error during standard order placement for user {target_user.id}: {e}", exc_info=True)
        await db.rollback() 
        raise HTTPException(status_code=500, detail="An unexpected error occurred.")
    
@router.post(
    "/close",
    response_model=OrderResponse,
    summary="Close an open order",
    description="Closes an open order, updates its status to 'CLOSED', and adjusts the user's overall margin. Requires the order ID and closing price."
)
async def close_order(
    close_request: CloseOrderRequest,
    background_tasks: BackgroundTasks,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User | DemoUser = Depends(get_user_from_service_or_user_token),
    token: str = Depends(oauth2_scheme)
):
    target_user_id_to_operate_on = current_user.id
    user_to_operate_on = current_user

    if close_request.user_id is not None:
        is_service_account = getattr(current_user, 'is_service_account', False)
        if is_service_account:
            enforce_service_user_id_restriction(close_request.user_id, token)
            _user = await crud_user.get_user_by_id(db, close_request.user_id)
            if _user:
                user_to_operate_on = _user
            else:
                _demo_user = await crud_user.get_demo_user_by_id(db, close_request.user_id)
                if _demo_user:
                    user_to_operate_on = _demo_user
                else:
                    raise HTTPException(status_code=404, detail="Target user not found for service op.")
            target_user_id_to_operate_on = close_request.user_id
        else:
            if close_request.user_id != current_user.id:
                 raise HTTPException(status_code=403, detail="Not authorized to specify user_id.")
    
    order_model_class = get_order_model(user_to_operate_on)
    order_id = close_request.order_id

    try:
        close_price = Decimal(str(close_request.close_price))
        if close_price <= Decimal("0"):
            raise HTTPException(status_code=400, detail="Close price must be positive.")
    except InvalidOperation:
        raise HTTPException(status_code=400, detail="Invalid close price format.")

    logger.info(f"Request to close order {order_id} for user {user_to_operate_on.id} ({type(user_to_operate_on).__name__}) with price {close_price}. Frontend provided type: {close_request.order_type}, company: {close_request.order_company_name}, status: {close_request.order_status}, frontend_status: {close_request.status}.")

    # Generate a unique 10-digit random close_order_id
    import random

    close_order_id = generate_10_digit_id() # Generate close_order_id

    if isinstance(user_to_operate_on, User):
        user_group = await crud_group.get_group_by_name(db, user_to_operate_on.group_name)
        if user_group and user_group.sending_orders == "barclays":
            logger.info(f"Live user {user_to_operate_on.id} from group '{user_group.group_name}' has 'sending_orders' set to 'barclays'. Pushing close request to Firebase and skipping local DB update.")
            
            firebase_close_data = {
                "order_id": close_request.order_id,
                "close_price": str(close_request.close_price),
                "user_id": user_to_operate_on.id,
                "order_type": close_request.order_type,
                "order_company_name": close_request.order_company_name,
                "order_status": close_request.order_status,
                "status": close_request.status,
                "action": "close_order",
                "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "close_order_id": close_order_id # Include close_order_id for Firebase
            }
            background_tasks.add_task(send_order_to_firebase, firebase_close_data, "live")
            
            db_order_for_response = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
            if db_order_for_response:
                db_order_for_response.order_status = "PENDING_CLOSE"
                db_order_for_response.close_message = "Order sent to service provider for closure."
                db_order_for_response.close_order_id = close_order_id # Save close_order_id in DB
                await db.commit()
                await db.refresh(db_order_for_response)
                
                # Log action in OrderActionHistory
                user_type_str = "live" if isinstance(user_to_operate_on, User) else "demo"
                update_fields_for_history = OrderUpdateRequest(
                    order_status="PENDING_CLOSE",
                    close_message="Order sent to service provider for closure.",
                    close_id=close_order_id, # Log the close_order_id here
                    close_price=close_price, # Log the requested close price
                ).model_dump(exclude_unset=True)
                await crud_order.update_order_with_tracking(
                    db,
                    db_order_for_response,
                    update_fields_for_history,
                    user_id=user_to_operate_on.id,
                    user_type=user_type_str,
                    action_type="CLOSE_REQUESTED" # New action type for history
                )
                await db.commit() # Commit history tracking
                await db.refresh(db_order_for_response)
                return OrderResponse.model_validate(db_order_for_response)
            else:
                 raise HTTPException(status_code=404, detail="Order not found for external closure processing.")
        else:
            logger.info(f"Live user {user_to_operate_on.id} from group '{user_group.group_name}' ('sending_orders' is NOT 'barclays'). Processing close locally.")
            async with db.begin_nested():
                db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
                if db_order is None:
                    raise HTTPException(status_code=404, detail="Order not found.")
                if db_order.order_user_id != user_to_operate_on.id and not getattr(current_user, 'is_admin', False):
                    raise HTTPException(status_code=403, detail="Not authorized to close this order.")
                if db_order.order_status != 'OPEN':
                    raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

                order_symbol = db_order.order_company_name.upper()
                quantity = Decimal(str(db_order.order_quantity))
                entry_price = Decimal(str(db_order.order_price))
                order_type_db = db_order.order_type.upper()
                user_group_name = getattr(user_to_operate_on, 'group_name', 'default')

                db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_to_operate_on.id, model_class=type(user_to_operate_on))
                if db_user_locked is None:
                    logger.error(f"Could not retrieve and lock user record for user ID: {user_to_operate_on.id}")
                    raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")
                
                all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                    db=db, user_id=db_user_locked.id, symbol=order_symbol, order_model=order_model_class
                )
                margin_before_recalc = await calculate_total_symbol_margin_contribution(
                    db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                    open_positions_for_symbol=all_open_orders_for_symbol, order_model_for_calc=order_model_class
                )
                current_overall_margin = Decimal(str(db_user_locked.margin))
                non_symbol_margin = current_overall_margin - margin_before_recalc
                remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
                margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution(
                    db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                    open_positions_for_symbol=remaining_orders_for_symbol_after_close, order_model_for_calc=order_model_class
                )
                db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
                
                symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
                symbol_info_result = await db.execute(symbol_info_stmt)
                ext_symbol_info = symbol_info_result.scalars().first()
                if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                    raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
                contract_size = Decimal(str(ext_symbol_info.contract_size))
                profit_currency = ext_symbol_info.profit.upper()

                group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
                if not group_settings:
                    raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
                
                commission_type = int(group_settings.get('commision_type', -1))
                commission_value_type = int(group_settings.get('commision_value_type', -1))
                commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
                exit_commission = Decimal("0.0")
                if commission_type in [0, 2]:
                    if commission_value_type == 0: exit_commission = quantity * commission_rate
                    elif commission_value_type == 1:
                        calculated_exit_contract_value = quantity * contract_size * close_price
                        if calculated_exit_contract_value > Decimal("0.0"):
                             exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
                entry_commission_for_total = Decimal("0.0")
                if commission_type in [0, 1]:
                    if commission_value_type == 0: entry_commission_for_total = quantity * commission_rate
                    elif commission_value_type == 1:
                        initial_contract_value = quantity * contract_size * entry_price
                        if initial_contract_value > Decimal("0.0"):
                            entry_commission_for_total = (commission_rate / Decimal("100")) * initial_contract_value
                total_commission_for_trade = (entry_commission_for_total + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

                if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
                elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
                else: raise HTTPException(status_code=500, detail="Invalid order type.")
                
                profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db) 
                if profit_currency != "USD" and profit_usd == profit: 
                    logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                    raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

                db_order.order_status = "CLOSED"
                db_order.close_price = close_price
                db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                db_order.swap = db_order.swap or Decimal("0.0")
                db_order.commission = total_commission_for_trade
                db_order.close_order_id = close_order_id # Save close_order_id in DB

                original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
                swap_amount = db_order.swap
                db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

                transaction_time = datetime.datetime.now(datetime.timezone.utc)
                wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time}
                if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
                else: wallet_common_data["user_id"] = db_user_locked.id
                if db_order.net_profit != Decimal("0.0"):
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                if total_commission_for_trade > Decimal("0.0"):
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Total commission for order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                if swap_amount != Decimal("0.0"):
                    db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap charges for order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
                
                await db.commit()

            await db.refresh(db_order)
            await db.refresh(db_user_locked)
            logger.info(f"Order {db_order.order_id} closed successfully for user {db_user_locked.id}.")

            user_data_to_cache = {
                "id": db_user_locked.id, "group_name": getattr(db_user_locked, 'group_name', 'default'),
                "leverage": db_user_locked.leverage, "wallet_balance": db_user_locked.wallet_balance,
                "margin": db_user_locked.margin
            }
            await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)

            all_user_orders_after_close = await crud_order.get_orders_by_user_id(db, user_id=db_user_locked.id, order_model=order_model_class, limit=1000)
            updated_open_positions_cache = []
            updated_pending_positions_cache = []
            for pos in all_user_orders_after_close:
                pos_dict_cache = {k: str(v) if isinstance(v := getattr(pos, k), Decimal) else v for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'close_order_id']} # Include close_order_id
                pos_dict_cache['profit_loss'] = "0.0"
                if pos.order_status == "OPEN":
                    pos_dict_cache['margin'] = str(pos.margin)
                    updated_open_positions_cache.append(pos_dict_cache)
                elif pos.order_status in ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
                    pos_dict_cache['margin'] = "0.0"
                    updated_pending_positions_cache.append(pos_dict_cache)
            user_portfolio_data = {
                "balance": str(db_user_locked.wallet_balance), "equity": "0.0", "margin": str(db_user_locked.margin),
                "free_margin": str(db_user_locked.wallet_balance - db_user_locked.margin), "profit_loss": "0.0",
                "positions": updated_open_positions_cache, "pending_positions": updated_pending_positions_cache
            }
            await set_user_portfolio_cache(redis_client, db_user_locked.id, user_portfolio_data)

            # Log action in OrderActionHistory
            user_type_str = "live" if isinstance(user_to_operate_on, User) else "demo"
            update_fields_for_history = OrderUpdateRequest(
                order_status="CLOSED",
                close_price=close_price,
                net_profit=db_order.net_profit,
                commission=db_order.commission,
                swap=db_order.swap,
                close_id=close_order_id, # Log the close_order_id here
                close_message="Order closed successfully."
            ).model_dump(exclude_unset=True)
            await crud_order.update_order_with_tracking(
                db,
                db_order,
                update_fields_for_history,
                user_id=user_to_operate_on.id,
                user_type=user_type_str,
                action_type="ORDER_CLOSED" # New action type for history
            )
            await db.commit() # Commit history tracking
            await db.refresh(db_order) # Refresh again after history is updated

            await publish_account_structure_changed_event(redis_client, db_user_locked.id)
            background_tasks.add_task(send_order_to_firebase, db_order.model_dump(), "live") # Send updated order data to Firebase
            return OrderResponse.model_validate(db_order)

    else:
        logger.info(f"Demo user {user_to_operate_on.id}. Processing close locally.")
        async with db.begin_nested():
            db_order = await crud_order.get_order_by_id(db, order_id=order_id, order_model=order_model_class)
            if db_order is None:
                raise HTTPException(status_code=404, detail="Order not found.")
            if db_order.order_user_id != user_to_operate_on.id and not getattr(current_user, 'is_admin', False):
                raise HTTPException(status_code=403, detail="Not authorized to close this order.")
            if db_order.order_status != 'OPEN':
                raise HTTPException(status_code=400, detail=f"Order status is '{db_order.order_status}'. Only 'OPEN' orders can be closed.")

            order_symbol = db_order.order_company_name.upper()
            quantity = Decimal(str(db_order.order_quantity))
            entry_price = Decimal(str(db_order.order_price))
            order_type_db = db_order.order_type.upper()
            user_group_name = getattr(user_to_operate_on, 'group_name', 'default')

            db_user_locked = await crud_user.get_user_by_id_with_lock(db, user_to_operate_on.id, model_class=type(user_to_operate_on))
            if db_user_locked is None:
                logger.error(f"Could not retrieve and lock user record for user ID: {user_to_operate_on.id}")
                raise HTTPException(status_code=500, detail="Could not retrieve user data securely.")
            
            all_open_orders_for_symbol = await crud_order.get_open_orders_by_user_id_and_symbol(
                db=db, user_id=db_user_locked.id, symbol=order_symbol, order_model=order_model_class
            )
            margin_before_recalc = await calculate_total_symbol_margin_contribution(
                db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                open_positions_for_symbol=all_open_orders_for_symbol, order_model_for_calc=order_model_class
            )
            current_overall_margin = Decimal(str(db_user_locked.margin))
            non_symbol_margin = current_overall_margin - margin_before_recalc
            remaining_orders_for_symbol_after_close = [o for o in all_open_orders_for_symbol if o.order_id != order_id]
            margin_after_symbol_recalc = await calculate_total_symbol_margin_contribution(
                db=db, redis_client=redis_client, user_id=db_user_locked.id, symbol=order_symbol,
                open_positions_for_symbol=remaining_orders_for_symbol_after_close, order_model_for_calc=order_model_class
            )
            db_user_locked.margin = max(Decimal(0), (non_symbol_margin + margin_after_symbol_recalc).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
            
            symbol_info_stmt = select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol.ilike(order_symbol))
            symbol_info_result = await db.execute(symbol_info_stmt)
            ext_symbol_info = symbol_info_result.scalars().first()
            if not ext_symbol_info or ext_symbol_info.contract_size is None or ext_symbol_info.profit is None:
                raise HTTPException(status_code=500, detail=f"Missing critical ExternalSymbolInfo for symbol {order_symbol}.")
            contract_size = Decimal(str(ext_symbol_info.contract_size))
            profit_currency = ext_symbol_info.profit.upper()

            group_settings = await get_group_symbol_settings_cache(redis_client, user_group_name, order_symbol)
            if not group_settings:
                raise HTTPException(status_code=500, detail="Group settings not found for commission calculation.")
            
            commission_type = int(group_settings.get('commision_type', -1))
            commission_value_type = int(group_settings.get('commision_value_type', -1))
            commission_rate = Decimal(str(group_settings.get('commision', "0.0")))
            exit_commission = Decimal("0.0")
            if commission_type in [0, 2]:
                if commission_value_type == 0: exit_commission = quantity * commission_rate
                elif commission_value_type == 1:
                    calculated_exit_contract_value = quantity * contract_size * close_price
                    if calculated_exit_contract_value > Decimal("0.0"):
                         exit_commission = (commission_rate / Decimal("100")) * calculated_exit_contract_value
            entry_commission_for_total = Decimal("0.0")
            if commission_type in [0, 1]:
                if commission_value_type == 0: entry_commission_for_total = quantity * commission_rate
                elif commission_value_type == 1:
                    initial_contract_value = quantity * contract_size * entry_price
                    if initial_contract_value > Decimal("0.0"):
                        entry_commission_for_total = (commission_rate / Decimal("100")) * initial_contract_value
            total_commission_for_trade = (entry_commission_for_total + exit_commission).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

            if order_type_db == "BUY": profit = (close_price - entry_price) * quantity * contract_size
            elif order_type_db == "SELL": profit = (entry_price - close_price) * quantity * contract_size
            else: raise HTTPException(status_code=500, detail="Invalid order type.")
            
            profit_usd = await _convert_to_usd(profit, profit_currency, db_user_locked.id, db_order.order_id, "PnL on Close", db=db) 
            if profit_currency != "USD" and profit_usd == profit: 
                logger.error(f"Order {db_order.order_id}: PnL conversion failed. Rates missing for {profit_currency}/USD.")
                raise HTTPException(status_code=500, detail=f"Critical: Could not convert PnL from {profit_currency} to USD.")

            db_order.order_status = "CLOSED"
            db_order.close_price = close_price
            db_order.net_profit = profit_usd.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
            db_order.swap = db_order.swap or Decimal("0.0")
            db_order.commission = total_commission_for_trade
            db_order.close_order_id = close_order_id # Save close_order_id in DB

            original_wallet_balance = Decimal(str(db_user_locked.wallet_balance))
            swap_amount = db_order.swap
            db_user_locked.wallet_balance = (original_wallet_balance + db_order.net_profit - total_commission_for_trade - swap_amount).quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP)

            transaction_time = datetime.datetime.now(datetime.timezone.utc)
            wallet_common_data = {"symbol": order_symbol, "order_quantity": quantity, "is_approved": 1, "order_type": db_order.order_type, "transaction_time": transaction_time}
            if isinstance(db_user_locked, DemoUser): wallet_common_data["demo_user_id"] = db_user_locked.id
            else: wallet_common_data["user_id"] = db_user_locked.id
            if db_order.net_profit != Decimal("0.0"):
                db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Profit/Loss", transaction_amount=db_order.net_profit, description=f"P/L for closing order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
            if total_commission_for_trade > Decimal("0.0"):
                db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Commission", transaction_amount=-total_commission_for_trade, description=f"Total commission for order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
            if swap_amount != Decimal("0.0"):
                db.add(Wallet(**WalletCreate(**wallet_common_data, transaction_type="Swap", transaction_amount=-swap_amount, description=f"Swap charges for order {db_order.order_id}").model_dump(exclude_none=True), transaction_id=generate_10_digit_id()))
            
            await db.commit()

        await db.refresh(db_order)
        await db.refresh(db_user_locked)
        logger.info(f"Order {db_order.order_id} closed successfully for user {db_user_locked.id}.")

        user_data_to_cache = {
            "id": db_user_locked.id, "group_name": getattr(db_user_locked, 'group_name', 'default'),
            "leverage": db_user_locked.leverage, "wallet_balance": db_user_locked.wallet_balance,
            "margin": db_user_locked.margin
        }
        await set_user_data_cache(redis_client, db_user_locked.id, user_data_to_cache)

        all_user_orders_after_close = await crud_order.get_orders_by_user_id(db, user_id=db_user_locked.id, order_model=order_model_class, limit=1000)
        updated_open_positions_cache = []
        updated_pending_positions_cache = []
        for pos in all_user_orders_after_close:
            pos_dict_cache = {k: str(v) if isinstance(v := getattr(pos, k), Decimal) else v for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'close_order_id']} # Include close_order_id
            pos_dict_cache['profit_loss'] = "0.0"
            if pos.order_status == "OPEN":
                pos_dict_cache['margin'] = str(pos.margin)
                updated_open_positions_cache.append(pos_dict_cache)
            elif pos.order_status in ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
                pos_dict_cache['margin'] = "0.0"
                updated_pending_positions_cache.append(pos_dict_cache)
        user_portfolio_data = {
            "balance": str(db_user_locked.wallet_balance), "equity": "0.0", "margin": str(db_user_locked.margin),
            "free_margin": str(db_user_locked.wallet_balance - db_user_locked.margin), "profit_loss": "0.0",
            "positions": updated_open_positions_cache, "pending_positions": updated_pending_positions_cache
        }
        await set_user_portfolio_cache(redis_client, db_user_locked.id, user_portfolio_data)

        # Log action in OrderActionHistory
        user_type_str = "live" if isinstance(user_to_operate_on, User) else "demo"
        update_fields_for_history = OrderUpdateRequest(
            order_status="CLOSED",
            close_price=close_price,
            net_profit=db_order.net_profit,
            commission=db_order.commission,
            swap=db_order.swap,
            close_id=close_order_id, # Log the close_order_id here
            close_message="Order closed successfully."
        ).model_dump(exclude_unset=True)
        await crud_order.update_order_with_tracking(
            db,
            db_order,
            update_fields_for_history,
            user_id=user_to_operate_on.id,
            user_type=user_type_str,
            action_type="ORDER_CLOSED" # New action type for history
        )
        await db.commit() # Commit history tracking
        await db.refresh(db_order) # Refresh again after history is updated

        await publish_account_structure_changed_event(redis_client, db_user_locked.id)
        return OrderResponse.model_validate(db_order)
    
# @router.post("/cancel", response_model=StatusResponse)
# async def cancel_pending_order(
#     cancel_request: CancelOrderRequest, # Defined in your original code
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
# ):
#     # ... (logic as before, no margin change occurs for pending orders)
#     order_model_class = get_order_model(current_user)
#     order_id = cancel_request.order_id
#     cancel_message = cancel_request.cancel_message or "Cancelled by user"
#     logger.info(f"Request to cancel pending order {order_id} for user {current_user.id} ({type(current_user).__name__}).")

#     db_order = await crud_order.get_order_by_id(db, order_id, order_model=order_model_class)
#     if not db_order: raise HTTPException(status_code=404, detail="Order not found")
#     if db_order.order_user_id != current_user.id: raise HTTPException(status_code=403, detail="Unauthorized")
    
#     allowed_pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
#     if db_order.order_status not in allowed_pending_statuses:
#         raise HTTPException(status_code=400, detail=f"Only pending orders can be cancelled. Status: {db_order.order_status}")

#     try:
#         async with db.begin_nested():
#             db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id, model_class=type(current_user))
#             if not db_user_locked: raise HTTPException(status_code=500, detail="User data lock failed.")
#             db_order.order_status = "CANCELED"
#             db_order.cancel_message = cancel_message
#             await db.commit()
        
#         # Update caches and publish event (as done in place_order and close_order)
#         # (user_data_to_cache, user_portfolio_data construction, set caches)
#         await db.refresh(db_user_locked) 
#         user_data_to_cache = {
#             "id": current_user.id, "group_name": getattr(db_user_locked, 'group_name', 'default'),
#             "leverage": db_user_locked.leverage, "wallet_balance": db_user_locked.wallet_balance,
#             "margin": db_user_locked.margin 
#         }
#         await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
#         # ... (construct and set user_portfolio_cache similarly, removing the cancelled order from pending list) ...
#         all_orders = await crud_order.get_orders_by_user_id(db, user_id=current_user.id, order_model=order_model_class, limit=1000)
#         open_positions_cache = []
#         pending_positions_cache = []
#         for pos in all_orders:
#             # ... (same logic as in place_order/close_order to build pos_dict and populate caches)
#             if pos.order_status == "OPEN": # ...
#                 open_positions_cache.append({k: str(v) if isinstance(v := getattr(pos, k), Decimal) else v for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'profit_loss']})
#             elif pos.order_status in allowed_pending_statuses and pos.order_id != order_id : # Exclude the cancelled one
#                  pending_positions_cache.append({k: str(v) if isinstance(v := getattr(pos, k), Decimal) else v for k in ['order_id', 'order_company_name', 'order_type', 'order_quantity', 'order_price', 'margin', 'contract_value', 'stop_loss', 'take_profit', 'profit_loss']})


#         user_portfolio_data = {
#             "balance": str(db_user_locked.wallet_balance), "equity": "0.0", 
#             "margin": str(db_user_locked.margin), 
#             "free_margin": str(db_user_locked.wallet_balance - db_user_locked.margin),
#             "profit_loss": "0.0", 
#             "positions": open_positions_cache, "pending_positions": pending_positions_cache
#         }
#         await set_user_portfolio_cache(redis_client, current_user.id, user_portfolio_data)
#         await publish_account_structure_changed_event(redis_client, current_user.id)
#         return StatusResponse(status="success", message="Pending order canceled successfully")

#     except Exception as e:
#         logger.error(f"Failed to cancel pending order {order_id}: {e}", exc_info=True)
#         await db.rollback()
#         raise HTTPException(status_code=500, detail="Error canceling order.")

# @router.post("/modify", response_model=StatusResponse)
# async def modify_pending_order(
#     request: ModifyOrderRequest, # Defined in your original code
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
# ):
#     # ... (logic as before, check free margin against potential new margin, user's actual margin is unchanged)
#     # ... at the end, call publish_account_structure_changed_event(redis_client, current_user.id)
#     order_model_class = get_order_model(current_user)
#     logger.info(f"Request to modify pending order {request.order_id} for user {current_user.id} ({type(current_user).__name__}).")
#     if request.new_price <= 0 or request.new_quantity <= 0:
#         raise HTTPException(status_code=400, detail="New price and quantity must be positive.")

#     db_order = await crud_order.get_order_by_id(db, request.order_id, order_model=order_model_class)
#     if not db_order: raise HTTPException(status_code=404, detail="Order not found")
#     if db_order.order_user_id != current_user.id: raise HTTPException(status_code=403, detail="Unauthorized")
    
#     allowed_pending_statuses = ["BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP", "PENDING"]
#     if db_order.order_status not in allowed_pending_statuses:
#         raise HTTPException(status_code=400, detail="Only pending orders can be modified.")

#     try:
#         async with db.begin_nested():
#             db_user_locked = await crud_user.get_user_by_id_with_lock(db, current_user.id, model_class=type(current_user))
#             if not db_user_locked: raise HTTPException(status_code=500, detail="User lock failed.")

#             potential_new_margin_usd, adj_new_price, new_contract_val = await calculate_single_order_margin(
#                 db, redis_client, current_user.id, request.new_quantity, request.new_price, 
#                 db_order.order_company_name, db_order.order_type
#             )
#             if potential_new_margin_usd is None: # Error in calculation
#                 raise HTTPException(status_code=400, detail="Failed to calculate potential margin.")

#             available_free_margin = db_user_locked.wallet_balance - db_user_locked.margin
#             if potential_new_margin_usd > available_free_margin:
#                 raise HTTPException(status_code=400, detail=f"Insufficient free margin. Required: {potential_new_margin_usd:.2f}, Available: {available_free_margin:.2f}")

#             db_order.order_price = adj_new_price
#             db_order.order_quantity = request.new_quantity
#             db_order.contract_value = new_contract_val
#             db_order.margin = Decimal("0.0") # Still pending
#             await db.commit()

#         # Update caches and publish event (as above)
#         await db.refresh(db_user_locked)
#         await db.refresh(db_order)
#         # ... (set_user_data_cache, set_user_portfolio_cache with updated pending order details) ...
#         user_data_to_cache = {
#             "id": current_user.id, "group_name": getattr(db_user_locked, 'group_name', 'default'),
#             "leverage": db_user_locked.leverage, "wallet_balance": db_user_locked.wallet_balance,
#             "margin": db_user_locked.margin 
#         }
#         await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
#         # (Rebuild and set user_portfolio_cache with the modified pending order's new price/qty)
#         # ... Full portfolio cache update logic from above ...
#         await publish_account_structure_changed_event(redis_client, current_user.id)
#         return StatusResponse(status="success", message="Pending order modified successfully")

#     except Exception as e:
#         logger.error(f"Error modifying pending order {request.order_id}: {e}", exc_info=True)
#         await db.rollback()
#         raise HTTPException(status_code=500, detail="Error modifying order.")


# @router.patch("/update-tp-sl", response_model=OrderResponse)
# async def update_stoploss_takeprofit(
#     request: UpdateStopLossTakeProfitRequest, # Defined in your original code
#     db: AsyncSession = Depends(get_db),
#     redis_client: Redis = Depends(get_redis_client),
#     current_user: User | DemoUser = Depends(get_user_from_service_or_user_token)
# ):
#     # ... (logic as before)
#     # ... at the end, call publish_account_structure_changed_event(redis_client, current_user.id)
#     # ... (This also affects how the "pending_positions" or "positions" list is cached and sent)
#     order_model_class = get_order_model(current_user)
#     logger.info(f"Request to update TP/SL for order {request.order_id} for user {current_user.id} ({type(current_user).__name__}).")

#     db_order = await crud_order.get_order_by_id(db, request.order_id, order_model=order_model_class)
#     if not db_order: raise HTTPException(status_code=404, detail="Order not found")
#     if db_order.order_user_id != current_user.id and not getattr(current_user, 'is_admin', False):
#         raise HTTPException(status_code=403, detail="Unauthorized")
#     if db_order.order_status in ["CLOSED", "CANCELED", "REJECTED"]:
#         raise HTTPException(status_code=400, detail=f"Cannot update TP/SL for order status {db_order.order_status}.")

#     updated_order = await crud_order.update_tp_sl_for_order(
#         db=db, db_order=db_order, stop_loss=request.stop_loss, take_profit=request.take_profit,
#         stoploss_id=request.stoploss_id, takeprofit_id=request.takeprofit_id
#     )
#     if not updated_order: raise HTTPException(status_code=500, detail="Failed to update TP/SL.")
    
#     # Update caches and publish event
#     await db.refresh(current_user) # Margin/balance unchanged by TP/SL itself
#     # ... (set_user_data_cache, set_user_portfolio_cache with updated TP/SL in the specific order) ...
#     user_data_to_cache = {
#             "id": current_user.id, "group_name": getattr(current_user, 'group_name', 'default'),
#             "leverage": current_user.leverage, "wallet_balance": current_user.wallet_balance,
#             "margin": current_user.margin 
#     }
#     await set_user_data_cache(redis_client, current_user.id, user_data_to_cache)
#     # (Rebuild and set user_portfolio_cache with the modified TP/SL for the order)
#     # ... Full portfolio cache update logic from above ...
#     await publish_account_structure_changed_event(redis_client, current_user.id)
    
#     response_data = OrderResponse.model_validate(updated_order).model_dump()
#     if updated_order.order_status in ["PENDING", "BUY_LIMIT", "SELL_LIMIT", "BUY_STOP", "SELL_STOP"]:
#         response_data["margin"] = Decimal("0.0")
#     return OrderResponse(**response_data)

# # close_all_orders_with_live_prices - this endpoint would repeatedly call the refactored close_order logic.
# # Each successful close within its loop would trigger its own cache updates and Redis publish.
# # The endpoint itself might publish one final "REFRESH_ALL_DATA_IF_ANY_ERRORS" or rely on individual signals.
# # For simplicity, each close_order call will handle its own signal.

# # ... (patch_order endpoint remains as is, but consider if it also needs to publish an event if patched fields affect portfolio view)
# # Assuming patch_order is for backend confirmations and doesn't directly alter data critical for immediate client portfolio view
# # without a corresponding more specific event (like close, place etc.)