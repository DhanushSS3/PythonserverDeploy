# app/api/v1/api.py

from fastapi import APIRouter

# Import individual routers from endpoints
from app.api.v1.endpoints import users, groups # Import the users router

# Create the main API router for version 1
api_router = APIRouter()

# Include individual routers
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(groups.router, prefix="/groups", tags=["groups"]) 

# You will include other routers here as you create them:
# api_router.include_router(orders.router, prefix="/orders", tags=["orders"])
# api_router.include_router(wallets.router, prefix="/wallets", tags=["wallets"])
# api_router.include_router(groups.router, prefix="/groups", tags=["groups"])
# api_router.include_router(symbols.router, prefix="/symbols", tags=["symbols"])
