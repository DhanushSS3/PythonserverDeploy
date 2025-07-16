# app/api/v1/endpoints/groups.py

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError # Import IntegrityError
from typing import List, Optional
from typing import Dict
from redis.asyncio import Redis

from app.database.session import get_db
from app.database.models import Group, User, DemoUser # Import Group, User and DemoUser models
from app.schemas.group import GroupCreate, GroupUpdate, GroupResponse # Import Group schemas
from app.schemas.user import StatusResponse # Import StatusResponse from user schema (assuming it's defined there)
from app.crud import group as crud_group # Import crud_group
from app.core.security import get_current_admin_user # Import the admin dependency
from app.crud.external_symbol_info import get_external_symbol_info_by_symbol
from app.dependencies.redis_client import get_redis_client
from decimal import Decimal
import datetime
from typing import Any

import logging
from app.core.cache import (
    set_group_settings_cache, set_group_symbol_settings_cache,
    delete_group_settings_cache, delete_all_group_symbol_settings_cache
)

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["groups"]
)

from app.core.security import get_current_user # Ensure this is correctly imported and used
from app.crud.group import get_group_by_name


@router.get(
    "/my-group-all-symbols",
    response_model=List[Dict[str, Any]],
    summary="Get all group records for the current user's group_name",
    description="Retrieves all group records with detailed info for the authenticated user's group name, including contract size."
)
async def get_all_group_records_with_contract_size(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user)
):
    if not current_user.group_name:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User is not assigned to any group."
        )

    groups = await crud_group.get_groups_by_name(db, group_name=current_user.group_name)

    if not groups:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No group entries found for the assigned group name."
        )

    group_data = []

    for group in groups:
        contract_size = None
        if group.symbol:
            external_info = await get_external_symbol_info_by_symbol(db, group.symbol)
            if external_info and external_info.contract_size is not None:
                contract_size = str(external_info.contract_size)

        # Calculate half_spread using the formula (spread * spread_pip)/2
        half_spread = None
        if group.spread is not None and group.spread_pip is not None:
            half_spread = (group.spread * group.spread_pip) / 2

        group_data.append({
            "id": group.id,
            "symbol": group.symbol,
            "name": group.name,
            "swap_buy": str(group.swap_buy),
            "swap_sell": str(group.swap_sell),
            "commision": str(group.commision),
            "commision_type": group.commision_type,
            "commision_value_type": group.commision_value_type,
            "margin": str(group.margin),
            "spread": str(group.spread),
            "deviation": str(group.deviation),
            "min_lot": str(group.min_lot),
            "max_lot": str(group.max_lot),
            "type": group.type,
            "pips": str(group.pips),
            "spread_pip": str(group.spread_pip),
            "show_points": str(group.show_points),
            "pip_currency": group.pip_currency,
            "created_at": group.created_at.isoformat() if hasattr(group.created_at, 'isoformat') else str(group.created_at),
            "updated_at": group.updated_at.isoformat() if hasattr(group.updated_at, 'isoformat') else str(group.updated_at),
            "contract_size": contract_size,
            "half_spread": str(half_spread) if half_spread is not None else None
        })

    return group_data



# Endpoint to create a new group (Admin Only)
@router.post(
    "/",
    response_model=GroupResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new group (Admin Only)",
    description="Creates a new group with the provided details (requires admin authentication)."
)
async def create_new_group(
    group_create: GroupCreate,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Creates a new group.
    """
    try:
        new_group = await crud_group.create_group(db=db, group_create=group_create)
        logger.info(f"Group '{new_group.name}' (Symbol: {new_group.symbol}) created successfully by admin {current_user.id}.")
        # Update group settings cache
        settings = {
            "sending_orders": getattr(new_group, 'sending_orders', None),
            # Add more group-level settings here if needed
        }
        await set_group_settings_cache(redis_client, new_group.name, settings)
        # Update group-symbol settings cache for this symbol if present
        if new_group.symbol:
            symbol_settings = {k: getattr(new_group, k) for k in new_group.__table__.columns.keys()}
            await set_group_symbol_settings_cache(redis_client, new_group.name, new_group.symbol, symbol_settings)
        return new_group
    except IntegrityError as e:
        await db.rollback()
        logger.warning(f"Attempted to create group with existing symbol/name combination by admin {current_user.id}. Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating group by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the group."
        )

def fix_datetime(dt):
    if dt is None or str(dt) == "0000-00-00 00:00:00":
        return None  # or datetime.datetime.utcnow()
    return dt

# Endpoint to get all groups with search and pagination (Admin Only)
@router.get(
    "/",
    response_model=List[GroupResponse],
    summary="Get all groups (Admin Only)",
    description="Retrieves a list of all groups (requires admin authentication)."
)
async def read_groups(
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Retrieves a list of all groups (no pagination).
    """
    groups = await crud_group.get_groups(db)
    for group in groups:
        group.created_at = fix_datetime(group.created_at)
        group.updated_at = fix_datetime(group.updated_at)
    return groups


@router.get("/my-group-spreads", response_model=dict)
async def get_my_group_spreads(
    db: AsyncSession = Depends(get_db),
    current_user: User | DemoUser = Depends(get_current_user),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Get spread values for all symbols in the user's group.
    Calculates half_spread for each symbol using the formula (spread * spread_pip)/2.
    """
    try:
        # Get the user's group name
        group_name = current_user.group_name
        if not group_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User is not assigned to any group"
            )
        
        # Get all group records for this group name
        group_records = await crud_group.get_groups_by_name(db, group_name)
        if not group_records:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Group '{group_name}' not found"
            )
        
        # Calculate spread values for each symbol
        spread_values = {}
        for group_record in group_records:
            if group_record.symbol:  # Make sure symbol is not None
                # Calculate half_spread using the formula (spread * spread_pip)/2
                if group_record.spread is not None and group_record.spread_pip is not None:
                    half_spread = (group_record.spread * group_record.spread_pip) / 2
                    # Convert symbol to lowercase and only include half_spread
                    symbol_key = group_record.symbol.lower() if group_record.symbol else ""
                    if symbol_key:  # Only add if we have a valid symbol key
                        spread_values[symbol_key] = float(half_spread)
        
        return {
            "group_name": group_name,
            "spreads": spread_values
        }
    
    except Exception as e:
        logger.error(f"Error fetching group spreads: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching group spreads: {str(e)}"
        )

# Endpoint to get a single group by ID (Admin Only)
@router.get(
    "/{group_id}",
    response_model=GroupResponse,
    summary="Get group by ID (Admin Only)",
    description="Retrieves a specific group by ID (requires admin authentication)."
)
async def read_group_by_id(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Retrieves a group by its ID.
    """
    # Admin check is handled by get_current_admin_user dependency

    group = await crud_group.get_group_by_id(db, group_id=group_id)
    if group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found"
        )
    return group

# Endpoint to update a group by ID (Admin Only)
@router.patch(
    "/{group_id}", # Using PATCH for partial updates
    response_model=GroupResponse,
    summary="Update a group by ID (Admin Only)",
    description="Updates the details of a specific group by ID (requires admin authentication)."
)
async def update_existing_group(
    group_id: int,
    group_update: GroupUpdate,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Updates a group's information.
    """
    db_group = await crud_group.get_group_by_id(db, group_id=group_id)
    if db_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found"
        )
    try:
        updated_group = await crud_group.update_group(db=db, db_group=db_group, group_update=group_update)
        logger.info(f"Group ID {group_id} updated successfully by admin {current_user.id}.")
        # Update group settings cache
        settings = {
            "sending_orders": getattr(updated_group, 'sending_orders', None),
            # Add more group-level settings here if needed
        }
        await set_group_settings_cache(redis_client, updated_group.name, settings)
        # Update group-symbol settings cache for this symbol if present
        if updated_group.symbol:
            symbol_settings = {k: getattr(updated_group, k) for k in updated_group.__table__.columns.keys()}
            await set_group_symbol_settings_cache(redis_client, updated_group.name, updated_group.symbol, symbol_settings)
        return updated_group
    except IntegrityError as e:
        await db.rollback()
        logger.warning(f"Attempted to update group ID {group_id} with existing symbol/name combination by admin {current_user.id}. Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error updating group ID {group_id} by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while updating the group."
        )

# Endpoint to delete a group by ID (Admin Only)
@router.delete(
    "/{group_id}",
    response_model=StatusResponse,
    summary="Delete a group by ID (Admin Only)",
    description="Deletes a specific group by ID (requires admin authentication)."
)
async def delete_existing_group(
    group_id: int,
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Deletes a group.
    """
    db_group = await crud_group.get_group_by_id(db, group_id=group_id)
    if db_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found"
        )
    try:
        group_name = db_group.name
        await crud_group.delete_group(db=db, db_group=db_group)
        logger.info(f"Group ID {group_id} deleted successfully by admin {current_user.id}.")
        # Invalidate group settings and all group-symbol settings cache for this group
        await delete_group_settings_cache(redis_client, group_name)
        await delete_all_group_symbol_settings_cache(redis_client, group_name)
        return StatusResponse(message=f"Group with ID {group_id} deleted successfully.")
    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting group ID {group_id} by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the group."
        )