from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from app.database.models import Symbol

async def get_symbol_type(db: AsyncSession, symbol_name: str) -> int:
    """
    Get the type of a symbol from the symbols table.
    Returns the type as an integer, or None if symbol not found.
    """
    stmt = select(Symbol).filter(Symbol.name.ilike(symbol_name))
    result = await db.execute(stmt)
    symbol = result.scalars().first()
    return symbol.type if symbol else None 