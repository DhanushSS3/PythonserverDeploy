# app/api/v1/endpoints/groups.py

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import IntegrityError # Import IntegrityError
from typing import List, Optional

from app.database.session import get_db
from app.database.models import Group, User # Import Group and User models
from app.schemas.group import GroupCreate, GroupUpdate, GroupResponse # Import Group schemas
from app.schemas.user import StatusResponse # Import StatusResponse from user schema (assuming it's defined there)
from app.crud import group as crud_group # Import crud_group
from app.core.security import get_current_admin_user # Import the admin dependency

import logging

logger = logging.getLogger(__name__)

router = APIRouter(
    tags=["groups"]
)

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
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Creates a new group.
    """
    # Admin check is handled by get_current_admin_user dependency

    try:
        new_group = await crud_group.create_group(db=db, group_create=group_create)
        logger.info(f"Group '{new_group.name}' (Symbol: {new_group.symbol}) created successfully by admin {current_user.id}.")
        return new_group
    except IntegrityError as e:
        await db.rollback()
        # The IntegrityError detail message is now set in the CRUD layer to be more specific
        logger.warning(f"Attempted to create group with existing symbol/name combination by admin {current_user.id}. Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e) # Return the detail message from the IntegrityError
        )
    except Exception as e:
        await db.rollback()
        logger.error(f"Error creating group by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while creating the group."
        )

# Endpoint to get all groups with search and pagination (Admin Only)
@router.get(
    "/",
    response_model=List[GroupResponse],
    summary="Get all groups (Admin Only)",
    description="Retrieves a list of all groups with optional search and pagination (requires admin authentication)."
)
async def read_groups(
    skip: int = Query(0, description="Number of groups to skip"),
    limit: int = Query(100, description="Maximum number of groups to return"),
    search: Optional[str] = Query(None, description="Search term for group name or symbol"),
    db: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Retrieves a paginated and searchable list of groups.
    """
    # Admin check is handled by get_current_admin_user dependency

    groups = await crud_group.get_groups(db, skip=skip, limit=limit, search=search)
    return groups

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
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Updates a group's information.
    """
    # Admin check is handled by get_current_admin_user dependency

    db_group = await crud_group.get_group_by_id(db, group_id=group_id)
    if db_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found"
        )

    try:
        updated_group = await crud_group.update_group(db=db, db_group=db_group, group_update=group_update)
        logger.info(f"Group ID {group_id} updated successfully by admin {current_user.id}.")
        return updated_group
    except IntegrityError as e:
        await db.rollback()
        # The IntegrityError detail message is now set in the CRUD layer to be more specific
        logger.warning(f"Attempted to update group ID {group_id} with existing symbol/name combination by admin {current_user.id}. Error: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e) # Return the detail message from the IntegrityError
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
    current_user: User = Depends(get_current_admin_user) # Restrict to admin
):
    """
    Deletes a group.
    """
    # Admin check is handled by get_current_admin_user dependency

    db_group = await crud_group.get_group_by_id(db, group_id=group_id)
    if db_group is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Group not found"
        )

    try:
        await crud_group.delete_group(db=db, db_group=db_group)
        logger.info(f"Group ID {group_id} deleted successfully by admin {current_user.id}.")
        return StatusResponse(message=f"Group with ID {group_id} deleted successfully.")
    except Exception as e:
        await db.rollback()
        logger.error(f"Error deleting group ID {group_id} by admin {current_user.id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="An error occurred while deleting the group."
        )
