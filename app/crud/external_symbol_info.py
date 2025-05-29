# app/crud/external_symbol_info.py

from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.database.models import ExternalSymbolInfo # Import the new model

# Function to get symbol info by fix_symbol from the database
async def get_external_symbol_info_by_symbol(db: AsyncSession, fix_symbol: str) -> Optional[ExternalSymbolInfo]:
    """
    Retrieves external symbol information from the database by its fix_symbol.
    """
    # Perform a case-insensitive query if fix_symbol might have different cases
    # from sqlalchemy import case, func # Import func and case for case-insensitive query
    # result = await db.execute(
    #     select(ExternalSymbolInfo).filter(func.lower(ExternalSymbolInfo.fix_symbol) == func.lower(fix_symbol))
    # )
    # Or if fix_symbol in DB is always consistent case, a simple filter is fine:
    result = await db.execute(select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol == fix_symbol))
    return result.scalars().first()

# We don't need batch insert functions here as you will insert data manually.

# app/crud/external_symbol_info.py

from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select

from app.database.models import ExternalSymbolInfo # Import the new model

# Function to get symbol info by fix_symbol from the database
async def get_external_symbol_info_by_symbol(db: AsyncSession, fix_symbol: str) -> Optional[ExternalSymbolInfo]:
    """
    Retrieves external symbol information from the database by its fix_symbol.
    """
    # Perform a case-insensitive query if fix_symbol might have different cases
    # from sqlalchemy import case, func # Import func and case for case-insensitive query
    # result = await db.execute(
    #     select(ExternalSymbolInfo).filter(func.lower(ExternalSymbolInfo.fix_symbol) == func.lower(fix_symbol))
    # )
    # Or if fix_symbol in DB is always consistent case, a simple filter is fine:
    result = await db.execute(select(ExternalSymbolInfo).filter(ExternalSymbolInfo.fix_symbol == fix_symbol))
    return result.scalars().first()

# We don't need batch insert functions here as you will insert data manually.