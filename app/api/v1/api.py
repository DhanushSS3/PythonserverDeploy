# app/api/v1/api.py

from fastapi import APIRouter

# Import individual routers from endpoints
from app.api.v1.endpoints import users, groups # Import the users and groups routers
# Import the market data WebSocket router module
from app.api.v1.endpoints import market_data_ws # Import the module

from app.api.v1.endpoints import orders

from app.api.v1.endpoints import money_requests
# Create the main API router for version 1
api_router = APIRouter()


api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(groups.router, prefix="/groups", tags=["groups"])
api_router.include_router(orders.router, tags=["orders"])


api_router.include_router(market_data_ws.router, tags=["market_data"])

api_router.include_router(money_requests.router, prefix="", tags=["Money Requests"])
