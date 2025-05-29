# app/crud/group.py

from typing import List, Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy.exc import IntegrityError # Import IntegrityError
from sqlalchemy import or_ # Import or_ for search filtering

from app.database.models import Group # Import the Group model
from app.schemas.group import GroupCreate, GroupUpdate # Import Group schemas
from app.schemas.money_request import MoneyRequestCreate

# Function to get a group by ID
async def get_group_by_id(db: AsyncSession, group_id: int) -> Group | None:
    """
    Retrieves a group from the database by its ID.
    """
    result = await db.execute(select(Group).filter(Group.id == group_id))
    return result.scalars().first()

# Function to get a group by name (useful for search or specific lookups, but uniqueness check is now composite)
async def get_group_by_name(db: AsyncSession, group_name: str) -> List[Group]: # Returns a list as name is no longer unique alone
    """
    Retrieves groups from the database by their name.
    Note: Name is no longer unique on its own, so this returns a list.
    """
    result = await db.execute(select(Group).filter(Group.name == group_name))
    return result.scalars().all()

# Function to get a group by symbol and name (for uniqueness check)
async def get_group_by_symbol_and_name(db: AsyncSession, symbol: Optional[str], name: str) -> Group | None:
    """
    Retrieves a group from the database by its symbol and name combination.
    Used to check for uniqueness before creating a new group.
    Handles cases where symbol might be None.
    """
    query = select(Group).filter(Group.name == name)
    if symbol is None:
        # Filter where symbol is NULL
        query = query.filter(Group.symbol.is_(None))
    else:
        # Filter where symbol == symbol
        query = query.filter(Group.symbol == symbol)

    result = await db.execute(query)
    return result.scalars().first()


# Function to get all groups with optional search and pagination
async def get_groups(
    db: AsyncSession,
    skip: int = 0,
    limit: int = 100,
    search: Optional[str] = None # Add search parameter
) -> List[Group]:
    """
    Retrieves a list of groups with optional search filtering and pagination.

    Args:
        db: The asynchronous database session.
        skip: The number of records to skip (for pagination).
        limit: The maximum number of records to return (for pagination).
        search: Optional search string to filter by name or symbol.

    Returns:
        A list of Group SQLAlchemy model instances.
    """
    query = select(Group)

    # Apply search filter if provided
    if search:
        # Use or_ to search in either name or symbol
        # Ensure Group.symbol is not None before applying ilike to avoid potential errors
        # Added a check for Group.symbol is not None before using ilike
        query = query.filter(
            or_(
                Group.name.ilike(f"%{search}%"), # Case-insensitive search in name
                Group.symbol.ilike(f"%{search}%") if hasattr(Group, 'symbol') and Group.symbol is not None else False # Ensure symbol exists and is not None
            )
        )

    # Apply pagination
    query = query.offset(skip).limit(limit)

    result = await db.execute(query)
    return result.scalars().all()

# --- NEW FUNCTION: Get all unique symbols for a given group name ---
async def get_all_symbols_for_group(db: AsyncSession, group_name: str) -> List[str]:
    """
    Fetches all unique symbols associated with a given group name from the database.
    Returns a list of symbol strings.
    """
    # Select distinct symbol values from the Group table where the name matches
    result = await db.execute(
        select(Group.symbol)
        .where(Group.name == group_name)
        .distinct() # Get unique symbols
    )
    # Fetch all results and extract symbols, filtering out None values
    symbols = [row[0] for row in result.all() if row[0] is not None]
    return symbols

# Function to create a new group
async def create_group(db: AsyncSession, group_create: GroupCreate) -> Group:
    """
    Creates a new group in the database.
    Checks for unique symbol and name combination before creating.
    Includes sending_orders and book fields.
    """
    # Check if a group with the same symbol and name already exists
    existing_group = await get_group_by_symbol_and_name(db, symbol=group_create.symbol, name=group_create.name)
    if existing_group:
        # Raise IntegrityError to be caught by the endpoint for a 400 response
        # The detail message should reflect the composite uniqueness
        detail_message = f"Group with symbol '{group_create.symbol}' and name '{group_create.name}' already exists." if group_create.symbol else f"Group with no symbol and name '{group_create.name}' already exists."
        raise IntegrityError(detail_message, {}, {})

    # Create a new SQLAlchemy Group model instance using data from the schema
    db_group = Group(
        symbol=group_create.symbol,
        name=group_create.name,
        commision_type=group_create.commision_type,
        commision_value_type=group_create.commision_value_type,
        type=group_create.type,
        pip_currency=group_create.pip_currency,
        show_points=group_create.show_points,
        swap_buy=group_create.swap_buy,
        swap_sell=group_create.swap_sell,
        commision=group_create.commision,
        margin=group_create.margin,
        spread=group_create.spread,
        deviation=group_create.deviation,
        min_lot=group_create.min_lot,
        max_lot=group_create.max_lot,
        pips=group_create.pips,
        spread_pip=group_create.spread_pip,
        # --- Include New Fields ---
        sending_orders=group_create.sending_orders,
        book=group_create.book,
        # --- End New Fields ---
        # created_at and updated_at will be set by database defaults
    )
    db.add(db_group)
    await db.commit()
    await db.refresh(db_group)
    return db_group

# Function to update an existing group
async def update_group(db: AsyncSession, db_group: Group, group_update: GroupUpdate) -> Group:
    """
    Updates an existing group in the database.
    Handles potential unique constraint violation on symbol and name combination.
    Includes sending_orders and book fields in the update.
    """
    # Convert the GroupUpdate Pydantic model to a dictionary, excluding unset fields
    update_data = group_update.model_dump(exclude_unset=True)

    # Check for unique symbol and name combination if either is being updated
    if "symbol" in update_data or "name" in update_data:
        # Use the potentially updated symbol and name, falling back to existing if not updated
        updated_symbol = update_data.get("symbol", db_group.symbol)
        updated_name = update_data.get("name", db_group.name)

        existing_group = await get_group_by_symbol_and_name(db, symbol=updated_symbol, name=updated_name)

        # If a group with the same symbol and name exists AND it's not the group we are updating
        if existing_group and existing_group.id != db_group.id:
             # Raise IntegrityError to be caught by the endpoint for a 400 response
             detail_message = f"Group with symbol '{updated_symbol}' and name '{updated_name}' already exists." if updated_symbol else f"Group with no symbol and name '{updated_name}' already exists."
             raise IntegrityError(detail_message, {}, {})


    # Apply updates from the Pydantic model to the SQLAlchemy model
    for field, value in update_data.items():
        # Ensure the field exists on the SQLAlchemy model before setting
        if hasattr(db_group, field):
            setattr(db_group, field, value)
        else:
            # Optional: Log a warning if trying to set a field that doesn't exist on the model
            # logger.warning(f"Attempted to update non-existent field '{field}' on Group model.")
            pass # Silently ignore fields not in the model


    await db.commit()
    await db.refresh(db_group) # Refresh to get the updated values
    return db_group

# Function to delete a group
async def delete_group(db: AsyncSession, db_group: Group):
    """
    Deletes a group from the database.
    """
    await db.delete(db_group)
    await db.commit()
    # Note: Deleting a group might require handling associated users (e.g., setting user.group_name to NULL or
    # handling foreign key constraints) depending on your database schema and application logic.
    # If users have a foreign key to groups, the database might prevent deletion or cascade the delete.

    # app/crud/group.py

from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.database.models import Group # Import Group model
# Import the external_symbol_info CRUD functions to use get_external_symbol_info_by_symbol
from app.crud import external_symbol_info as crud_external_symbol_info


# app/crud/group.py

from typing import Optional, List, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.database.models import Group # Import Group model
# Import the external_symbol_info CRUD functions to use get_external_symbol_info_by_symbol
from app.crud import external_symbol_info as crud_external_symbol_info


async def get_groups_by_name(db: AsyncSession, group_name: str) -> List[Group]:
    """
    Retrieves all Group objects from the database that match the given name.
    """
    result = await db.execute(
        select(Group).filter(Group.name == group_name)
    )
    return result.scalars().all() # Fetch all records that match the group name


async def get_group_symbols_and_external_info(db: AsyncSession, group_name: str) -> Dict[str, Any]:
    """
    Fetches all unique symbols belonging to a specific group name
    and their associated external symbol information.
    """
    groups = await get_groups_by_name(db, group_name)
    
    if not groups:
        return {"group_found": False, "message": f"Group '{group_name}' not found."}

    unique_symbols_from_groups = set()
    for group_record in groups:
        if group_record.symbol:
            unique_symbols_from_groups.add(group_record.symbol)

    external_symbol_details = []
    for symbol_value in unique_symbols_from_groups:
        # Correctly call the function from crud_external_symbol_info
        external_info = await crud_external_symbol_info.get_external_symbol_info_by_symbol(db, symbol_value)
        if external_info:
            external_symbol_details.append({
                "id": external_info.id,
                "fix_symbol": external_info.fix_symbol,
                "description": external_info.description,
                "instrument_type": external_info.instrument_type,
                "contract_size": str(external_info.contract_size) # Convert Decimal to string for JSON
                # Removed 'created_at' and 'updated_at' as ExternalSymbolInfo model does not have these attributes
            })
    
    # Assuming all group records with the same name share common group properties
    # besides the 'symbol' field, we can take the properties from the first one.
    first_group_record = groups[0]

    return {
        "group_found": True,
        "group_name": first_group_record.name,
        "group_id": first_group_record.id,
        "group_symbols_from_records": list(unique_symbols_from_groups), # The actual 'symbol' values from Group records
        "external_symbols_info": external_symbol_details, # The detailed ExternalSymbolInfo objects
        "group_details": { # Other details of the group (from the first record)
            "symbol": first_group_record.symbol, # Include the symbol from the first group record if needed
            "commision_type": first_group_record.commision_type,
            "commision_value_type": first_group_record.commision_value_type,
            "type": first_group_record.type,
            "pip_currency": first_group_record.pip_currency,
            "show_points": first_group_record.show_points,
            "swap_buy": str(first_group_record.swap_buy),
            "swap_sell": str(first_group_record.swap_sell),
            "commision": str(first_group_record.commision),
            "margin": str(first_group_record.margin),
            "spread": str(first_group_record.spread),
            "deviation": str(first_group_record.deviation),
            "min_lot": str(first_group_record.min_lot),
            "max_lot": str(first_group_record.max_lot),
            "pips": str(first_group_record.pips),
            "spread_pip": str(first_group_record.spread_pip),
            "sending_orders": first_group_record.sending_orders,
            "book": first_group_record.book,
            "created_at": first_group_record.created_at.isoformat(),
            "updated_at": first_group_record.updated_at.isoformat()
        }
    }