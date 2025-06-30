# app/api/v1/api.py

from fastapi import APIRouter

# Import individual routers from endpoints
from app.api.v1.endpoints import users, groups, orders, wallets, news, money_requests, crypto_payments
# Uncomment favorites import 
from app.api.v1.endpoints import favorites
# Import the market data WebSocket router module
from app.api.v1.endpoints import market_data_ws # Import the module
from app.api.v1.endpoints import admin_wallet
# Create the main API router for version 1
api_router = APIRouter()

# Include all routers with appropriate prefixes and tags
api_router.include_router(users.router, prefix="/users", tags=["users"])
api_router.include_router(groups.router, prefix="/groups", tags=["groups"])
api_router.include_router(orders.router, tags=["orders"])
api_router.include_router(money_requests.router, prefix="", tags=["Money Requests"])
api_router.include_router(wallets.router, tags=["Wallets"])
api_router.include_router(news.router, tags=["News"])
api_router.include_router(crypto_payments.router, prefix="/payments", tags=["Crypto Payments"])
# Include the favorites router
api_router.include_router(favorites.router, tags=["favorites"])
# Include the WebSocket router
api_router.include_router(market_data_ws.router, tags=["market_data"])
api_router.include_router(admin_wallet.router, tags=["admin_wallet"])