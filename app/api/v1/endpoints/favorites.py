from fastapi import APIRouter, Depends, HTTPException, status, Query, Request
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import List, Union
import logging

from app.database.session import get_db
from app.database.models import User, DemoUser, Symbol
from app.schemas.favorites import (
    AddFavoriteSymbol, 
    RemoveFavoriteSymbol, 
    FavoriteSymbolResponse,
    FavoriteSymbolsWithDetails,
    SimpleFavoriteSymbolsList,
    SymbolDetails
)
from app.crud import favorites as crud_favorites
from app.core.security import get_current_user
from app.dependencies.redis_client import get_redis_client
from redis.asyncio import Redis

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/favorites",
    tags=["favorites"]
)


@router.post(
    "",
    response_model=FavoriteSymbolResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Add a symbol to favorites"
)
async def add_to_favorites(
    favorite: AddFavoriteSymbol,
    current_user: Union[User, DemoUser] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Add a trading symbol to the current user's favorites list.
    """
    try:
        # Determine user type from the current user
        user_type = getattr(current_user, 'user_type', 'live')
        
        # Log the request payload for debugging
        logger.info(f"Add to favorites request: {favorite.dict()}")
        
        # Find the symbol by name
        symbol = await crud_favorites.get_symbol_by_name(db, favorite.symbol)
        if not symbol:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Symbol '{favorite.symbol}' not found"
            )
        
        # Add to favorites
        user_favorite = await crud_favorites.add_favorite_symbol(
            db=db,
            user_id=current_user.id,
            symbol_id=symbol.id,
            user_type=user_type,
            redis_client=redis_client
        )
        
        return {
            "id": user_favorite.id,
            "symbol": symbol.name,
            "symbol_id": symbol.id,
            "created_at": user_favorite.created_at
        }
    except Exception as e:
        logger.error(f"Error in add_to_favorites: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add symbol to favorites: {str(e)}"
        )


@router.delete(
    "",
    status_code=status.HTTP_200_OK,
    summary="Remove a symbol from favorites"
)
async def remove_from_favorites(
    favorite: RemoveFavoriteSymbol,
    current_user: Union[User, DemoUser] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Remove a trading symbol from the current user's favorites list.
    """
    try:
        # Determine user type from the current user
        user_type = getattr(current_user, 'user_type', 'live')
        
        # Log the request payload for debugging
        logger.info(f"Remove from favorites request: {favorite.dict()}")
        
        # Find the symbol by name
        symbol = await crud_favorites.get_symbol_by_name(db, favorite.symbol)
        if not symbol:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Symbol '{favorite.symbol}' not found"
            )
        
        # Remove from favorites
        success = await crud_favorites.remove_favorite_symbol(
            db=db,
            user_id=current_user.id,
            symbol_id=symbol.id,
            user_type=user_type,
            redis_client=redis_client
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Symbol '{favorite.symbol}' is not in your favorites"
            )
        
        return {"message": f"Symbol '{favorite.symbol}' removed from favorites"}
    except Exception as e:
        logger.error(f"Error in remove_from_favorites: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to remove symbol from favorites: {str(e)}"
        )


@router.get(
    "/detailed",
    response_model=FavoriteSymbolsWithDetails,
    status_code=status.HTTP_200_OK,
    summary="Get all favorite symbols with details"
)
async def get_favorites(
    current_user: Union[User, DemoUser] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Get all favorite symbols for the current user with their details.
    """
    try:
        # Determine user type from the current user
        user_type = getattr(current_user, 'user_type', 'live')
        
        # Get favorite symbols
        favorite_symbols = await crud_favorites.get_favorite_symbols(
            db=db,
            user_id=current_user.id,
            user_type=user_type,
            redis_client=redis_client
        )
        
        # Log the number of symbols found
        logger.debug(f"Found {len(favorite_symbols)} favorite symbols for user {current_user.id} ({user_type})")
        
        # Filter out any symbols with problematic data
        valid_symbols = []
        for symbol in favorite_symbols:
            try:
                # Validate the datetime fields
                if (str(symbol.created_at) == '0000-00-00 00:00:00' or 
                    str(symbol.updated_at) == '0000-00-00 00:00:00'):
                    # Use now() for invalid dates
                    from datetime import datetime
                    symbol.created_at = datetime.now()
                    symbol.updated_at = datetime.now()
                valid_symbols.append(symbol)
            except Exception as e:
                logger.error(f"Error validating symbol {symbol.id}: {e}")
                # Skip invalid symbols
                continue
        
        return {
            "favorites": valid_symbols,
            "total": len(valid_symbols)
        }
    except Exception as e:
        logger.error(f"Error in get_favorites endpoint: {e}")
        # Return empty result on error
        return {
            "favorites": [],
            "total": 0
        }


@router.get(
    "",
    response_model=SimpleFavoriteSymbolsList,
    status_code=status.HTTP_200_OK,
    summary="Get favorite symbols as a simple list"
)
async def get_simple_favorites(
    current_user: Union[User, DemoUser] = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
    redis_client: Redis = Depends(get_redis_client)
):
    """
    Get a simplified list of favorite symbols for the current user (names only).
    """
    try:
        # Determine user type from the current user
        user_type = getattr(current_user, 'user_type', 'live')
        
        # Direct SQL query to get all favorite symbols without user_type filter
        from sqlalchemy import text
        sql = text("""
            SELECT s.name 
            FROM symbols s
            JOIN user_favorite_symbols ufs ON s.id = ufs.symbol_id
            WHERE ufs.user_id = :user_id
        """)
        
        result = await db.execute(sql, {"user_id": current_user.id})
        symbol_names = [row[0] for row in result.fetchall()]
        
        # Log what we found
        logger.info(f"Direct SQL query found {len(symbol_names)} symbols for user {current_user.id}")
        logger.info(f"Symbols: {symbol_names}")
        
        return {
            "symbols": symbol_names
        }
    except Exception as e:
        logger.error(f"Error in get_simple_favorites endpoint: {e}")
        # Return empty result on error
        return {
            "symbols": []
        }




# Add OPTIONS method handlers for CORS preflight requests
@router.options(
    "",
    status_code=status.HTTP_200_OK,
    summary="CORS preflight for favorites endpoints"
)
async def options_favorites(request: Request):
    """
    Handle OPTIONS preflight request for favorites endpoints.
    """
    return JSONResponse(
        content={"message": "OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, DELETE, GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        }
    )

@router.options(
    "/detailed",
    status_code=status.HTTP_200_OK,
    summary="CORS preflight for detailed favorites endpoint"
)
async def options_detailed_favorites(request: Request):
    """
    Handle OPTIONS preflight request for detailed favorites endpoint.
    """
    return JSONResponse(
        content={"message": "OK"},
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
        }
    ) 