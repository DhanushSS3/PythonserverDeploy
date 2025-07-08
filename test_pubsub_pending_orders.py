#!/usr/bin/env python3
"""
Test script for the new pub/sub approach for pending orders and SL-TP triggering.
This script will:
1. Place a pending order
2. Publish market data updates
3. Monitor if the pending order gets triggered
"""

import asyncio
import json
import time
from decimal import Decimal
import redis.asyncio as redis
from app.core.cache import DecimalEncoder, REDIS_MARKET_DATA_CHANNEL
from app.services.pending_orders import get_pending_orders_for_symbol, get_open_orders_for_symbol
from app.database.session import AsyncSessionLocal
from app.crud.user import get_demo_user_by_id
from app.api.v1.endpoints.orders import place_pending_order

async def test_pubsub_pending_orders():
    """Test the new pub/sub approach for pending orders"""
    
    # Initialize Redis connection
    redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=False)
    
    try:
        # Test parameters
        test_user_id = 1  # Demo user ID
        test_symbol = "EURUSD"
        test_trigger_price = Decimal("1.0850")
        
        print(f"=== Testing Pub/Sub Pending Orders ===")
        print(f"User ID: {test_user_id}")
        print(f"Symbol: {test_symbol}")
        print(f"Trigger Price: {test_trigger_price}")
        
        # Step 1: Check if user exists
        async with AsyncSessionLocal() as db:
            user = await get_demo_user_by_id(db, test_user_id)
            if not user:
                print(f"❌ Demo user {test_user_id} not found")
                return
            print(f"✅ Found user: {user.email}")
        
        # Step 2: Place a pending order
        print(f"\n--- Step 1: Placing Pending Order ---")
        
        pending_order_data = {
            "order_company_name": test_symbol,
            "order_type": "BUY_STOP",
            "order_volume": "0.01",
            "trigger_price": str(test_trigger_price),
            "stop_loss": str(test_trigger_price - Decimal("0.0010")),
            "take_profit": str(test_trigger_price + Decimal("0.0020")),
            "order_user_id": test_user_id,
            "user_type": "demo"
        }
        
        try:
            async with AsyncSessionLocal() as db:
                result = await place_pending_order(db, redis_client, pending_order_data)
                print(f"✅ Pending order placed: {result}")
        except Exception as e:
            print(f"❌ Failed to place pending order: {e}")
            return
        
        # Step 3: Check if pending order is in Redis
        print(f"\n--- Step 2: Checking Pending Order in Redis ---")
        await asyncio.sleep(1)  # Give time for Redis update
        
        pending_orders = await get_pending_orders_for_symbol(redis_client, test_symbol)
        print(f"Pending orders for {test_symbol}: {len(pending_orders)}")
        for order in pending_orders:
            print(f"  - Order ID: {order.get('order_id')}")
            print(f"    Type: {order.get('order_type')}")
            print(f"    Trigger: {order.get('trigger_price')}")
            print(f"    User: {order.get('order_user_id')}")
        
        # Step 4: Publish market data below trigger price (should not trigger)
        print(f"\n--- Step 3: Publishing Market Data Below Trigger Price ---")
        
        market_data_below = {
            "type": "market_data_update",
            test_symbol: {
                "bid": 1.0840,
                "ask": 1.0842,
                "timestamp": time.time()
            },
            "_timestamp": time.time()
        }
        
        message = json.dumps(market_data_below, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
        print(f"✅ Published market data below trigger price, subscribers: {result}")
        
        # Wait a bit and check if order was triggered
        await asyncio.sleep(2)
        pending_orders_after_below = await get_pending_orders_for_symbol(redis_client, test_symbol)
        print(f"Pending orders after below trigger: {len(pending_orders_after_below)}")
        
        # Step 5: Publish market data above trigger price (should trigger)
        print(f"\n--- Step 4: Publishing Market Data Above Trigger Price ---")
        
        market_data_above = {
            "type": "market_data_update",
            test_symbol: {
                "bid": 1.0855,
                "ask": 1.0857,
                "timestamp": time.time()
            },
            "_timestamp": time.time()
        }
        
        message = json.dumps(market_data_above, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
        print(f"✅ Published market data above trigger price, subscribers: {result}")
        
        # Wait a bit and check if order was triggered
        await asyncio.sleep(3)
        pending_orders_after_above = await get_pending_orders_for_symbol(redis_client, test_symbol)
        open_orders_after_trigger = await get_open_orders_for_symbol(redis_client, test_symbol)
        
        print(f"Pending orders after above trigger: {len(pending_orders_after_above)}")
        print(f"Open orders after trigger: {len(open_orders_after_trigger)}")
        
        if len(pending_orders_after_above) < len(pending_orders_after_below):
            print("✅ Pending order was triggered successfully!")
        else:
            print("❌ Pending order was not triggered")
        
        # Step 6: Check open orders for SL-TP
        print(f"\n--- Step 5: Checking Open Orders for SL-TP ---")
        for order in open_orders_after_trigger:
            print(f"  - Order ID: {order.get('order_id')}")
            print(f"    Type: {order.get('order_type')}")
            print(f"    Stop Loss: {order.get('stop_loss')}")
            print(f"    Take Profit: {order.get('take_profit')}")
        
        print(f"\n=== Test Completed ===")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(test_pubsub_pending_orders()) 