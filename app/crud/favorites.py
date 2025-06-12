from typing import List, Optional, Union, Dict, Any
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from sqlalchemy import and_, or_
from redis.asyncio import Redis
import json
import logging

from app.database.models import UserFavoriteSymbol, Symbol, User, DemoUser
from app.schemas.favorites import AddFavoriteSymbol

logger = logging.getLogger(__name__)

# Redis cache key for favorites
FAVORITES_CACHE_KEY_PREFIX = "favorites:"
CACHE_EXPIRY = 60 * 60  # 1 hour


async def get_symbol_by_name(db: AsyncSession, symbol_name: str) -> Optional[Symbol]:
    """Get a symbol by its name"""
    query = select(Symbol).where(Symbol.name == symbol_name)
    result = await db.execute(query)
    return result.scalars().first()


async def add_favorite_symbol(
    db: AsyncSession, 
    user_id: int, 
    symbol_id: int, 
    user_type: str = "live",
    redis_client: Optional[Redis] = None
) -> UserFavoriteSymbol:
    """Add a symbol to user's favorites"""
    # Check if already a favorite
    query = select(UserFavoriteSymbol).where(
        and_(
            UserFavoriteSymbol.user_id == user_id,
            UserFavoriteSymbol.symbol_id == symbol_id,
            UserFavoriteSymbol.user_type == user_type
        )
    )
    result = await db.execute(query)
    existing = result.scalars().first()
    
    if existing:
        return existing
    
    # Create new favorite
    new_favorite = UserFavoriteSymbol(
        user_id=user_id,
        symbol_id=symbol_id,
        user_type=user_type
    )
    
    db.add(new_favorite)
    await db.commit()
    await db.refresh(new_favorite)
    
    # Invalidate cache
    if redis_client:
        cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}{user_id}:{user_type}"
        try:
            await redis_client.delete(cache_key)
            logger.debug(f"Invalidated favorites cache for user {user_id} ({user_type})")
        except Exception as e:
            logger.error(f"Error invalidating favorites cache: {e}")
    
    return new_favorite


async def remove_favorite_symbol(
    db: AsyncSession, 
    user_id: int, 
    symbol_id: int, 
    user_type: str = "live",
    redis_client: Optional[Redis] = None
) -> bool:
    """Remove a symbol from user's favorites"""
    query = select(UserFavoriteSymbol).where(
        and_(
            UserFavoriteSymbol.user_id == user_id,
            UserFavoriteSymbol.symbol_id == symbol_id,
            UserFavoriteSymbol.user_type == user_type
        )
    )
    result = await db.execute(query)
    favorite = result.scalars().first()
    
    if not favorite:
        return False
    
    await db.delete(favorite)
    await db.commit()
    
    # Invalidate cache
    if redis_client:
        cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}{user_id}:{user_type}"
        try:
            await redis_client.delete(cache_key)
            logger.debug(f"Invalidated favorites cache for user {user_id} ({user_type})")
        except Exception as e:
            logger.error(f"Error invalidating favorites cache: {e}")
    
    return True


async def get_favorite_symbols(
    db: AsyncSession, 
    user_id: int, 
    user_type: str = "live",
    redis_client: Optional[Redis] = None
) -> List[Symbol]:
    """Get all favorite symbols for a user with symbol details"""
    # Try to get from cache first
    if redis_client:
        cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}{user_id}:{user_type}"
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                logger.debug(f"Found favorites in cache for user {user_id} ({user_type})")
                symbols_data = json.loads(cached_data)
                # We need to query the symbols anyway to get full objects
                if symbols_data and len(symbols_data) > 0:
                    symbol_ids = [s.get("id") for s in symbols_data]
                    query = select(Symbol).where(Symbol.id.in_(symbol_ids))
                    result = await db.execute(query)
                    return result.scalars().all()
        except Exception as e:
            logger.error(f"Error retrieving favorites from cache: {e}")
    
    # If not in cache or cache failed, query from database
    try:
        query = select(Symbol).join(
            UserFavoriteSymbol, 
            and_(
                Symbol.id == UserFavoriteSymbol.symbol_id,
                UserFavoriteSymbol.user_id == user_id
                # Not filtering by user_type to get all favorites
            )
        )
        result = await db.execute(query)
        symbols = result.scalars().all()
        
        # Handle invalid datetime values in symbols
        for symbol in symbols:
            # SQLAlchemy automatically loads these as Python datetime objects
            # If they are None or invalid, set defaults
            if not symbol.created_at or str(symbol.created_at) == '0000-00-00 00:00:00':
                from datetime import datetime
                symbol.created_at = datetime.now()
            if not symbol.updated_at or str(symbol.updated_at) == '0000-00-00 00:00:00':
                from datetime import datetime
                symbol.updated_at = datetime.now()
        
        # Cache the results
        if redis_client and symbols:
            cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}{user_id}:{user_type}"
            try:
                # Create a serializable version of the symbols
                serializable_symbols = [{"id": s.id, "name": s.name} for s in symbols]
                await redis_client.setex(
                    cache_key, 
                    CACHE_EXPIRY, 
                    json.dumps(serializable_symbols)
                )
                logger.debug(f"Cached favorites for user {user_id} ({user_type})")
            except Exception as e:
                logger.error(f"Error caching favorites: {e}")
        
        return symbols
    except Exception as e:
        logger.error(f"Error fetching favorite symbols: {e}")
        # Return empty list on error
        return []


async def get_favorite_symbol_ids(
    db: AsyncSession, 
    user_id: int, 
    user_type: str = "live"
) -> List[int]:
    """Get IDs of all favorite symbols for a user"""
    query = select(UserFavoriteSymbol.symbol_id).where(
        and_(
            UserFavoriteSymbol.user_id == user_id
            # Not filtering by user_type to get all favorites
        )
    )
    result = await db.execute(query)
    return [row[0] for row in result.fetchall()]


async def get_favorite_symbol_names(
    db: AsyncSession, 
    user_id: int, 
    user_type: str = "live",
    redis_client: Optional[Redis] = None
) -> List[str]:
    """Get only the names of favorite symbols for a user"""
    try:
        # Try to get from cache first with a different key
        if redis_client:
            cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}names:{user_id}:{user_type}"
            try:
                cached_data = await redis_client.get(cache_key)
                if cached_data:
                    logger.debug(f"Found favorite symbol names in cache for user {user_id} ({user_type})")
                    return json.loads(cached_data)
            except Exception as e:
                logger.error(f"Error retrieving favorite symbol names from cache: {e}")
        
        # Direct query to get symbol names - ignore user_type to get all favorites
        query = select(Symbol.name).join(
            UserFavoriteSymbol, 
            and_(
                Symbol.id == UserFavoriteSymbol.symbol_id,
                UserFavoriteSymbol.user_id == user_id
                # Not filtering by user_type to get all favorites
            )
        )
        result = await db.execute(query)
        symbol_names = [row[0] for row in result.fetchall()]
        
        # Log the count of symbols found
        logger.debug(f"Found {len(symbol_names)} favorite symbol names for user {user_id}")
        
        # Cache the results
        if redis_client and symbol_names:
            cache_key = f"{FAVORITES_CACHE_KEY_PREFIX}names:{user_id}:{user_type}"
            try:
                await redis_client.setex(
                    cache_key, 
                    CACHE_EXPIRY, 
                    json.dumps(symbol_names)
                )
                logger.debug(f"Cached favorite symbol names for user {user_id} ({user_type})")
            except Exception as e:
                logger.error(f"Error caching favorite symbol names: {e}")
        
        return symbol_names
    except Exception as e:
        logger.error(f"Error fetching favorite symbol names: {e}")
        return [] 