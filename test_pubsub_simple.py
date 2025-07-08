#!/usr/bin/env python3
"""
Simple test script to verify the pub/sub system is working.
"""

import asyncio
import json
import time
from decimal import Decimal
import redis.asyncio as redis
from app.core.cache import DecimalEncoder, REDIS_MARKET_DATA_CHANNEL
from app.services.pending_orders import get_pending_orders_for_symbol, add_pending_order

async def test_pubsub_simple():
    """Simple test to verify pub/sub system"""
    
    # Initialize Redis connection
    redis_client = redis.Redis(host="127.0.0.1", port=6379, decode_responses=False)
    
    try:
        print("=== Simple Pub/Sub Test ===")
        
        # Step 1: Check if market data subscriber is running
        print("\n1. Checking Redis pub/sub subscribers...")
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(REDIS_MARKET_DATA_CHANNEL)
        
        # Get subscriber count
        result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, "test")
        print(f"   Subscribers to {REDIS_MARKET_DATA_CHANNEL}: {result}")
        
        if result == 0:
            print("   ❌ No subscribers found! Market data subscriber may not be running.")
        else:
            print("   ✅ Market data subscriber is running!")
        
        await pubsub.unsubscribe(REDIS_MARKET_DATA_CHANNEL)
        await pubsub.close()
        
        # Step 2: Test adding a pending order to Redis
        print("\n2. Testing pending order storage...")
        
        test_order = {
            "order_id": "TEST123456",
            "order_company_name": "EURUSD",
            "order_type": "BUY_STOP",
            "order_volume": "0.01",
            "trigger_price": "1.0850",
            "order_user_id": 1,
            "user_type": "demo",
            "group_name": "Group A"
        }
        
        try:
            await add_pending_order(redis_client, test_order)
            print("   ✅ Test pending order added to Redis")
        except Exception as e:
            print(f"   ❌ Failed to add test order: {e}")
        
        # Step 3: Check if the order is retrievable
        print("\n3. Checking if order is retrievable...")
        
        pending_orders = await get_pending_orders_for_symbol(redis_client, "EURUSD")
        print(f"   Found {len(pending_orders)} pending orders for EURUSD")
        
        for order in pending_orders:
            print(f"   - Order ID: {order.get('order_id')}")
            print(f"     Type: {order.get('order_type')}")
            print(f"     Trigger: {order.get('trigger_price')}")
            print(f"     User: {order.get('order_user_id')}")
        
        # Step 4: Test market data publishing
        print("\n4. Testing market data publishing...")
        
        market_data = {
            "type": "market_data_update",
            "EURUSD": {
                "bid": 1.0855,
                "ask": 1.0857,
                "timestamp": time.time()
            },
            "_timestamp": time.time()
        }
        
        message = json.dumps(market_data, cls=DecimalEncoder)
        result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
        print(f"   Published market data, received by {result} subscribers")
        
        # Step 5: Wait and check if order was triggered
        print("\n5. Waiting for order processing...")
        await asyncio.sleep(3)
        
        pending_orders_after = await get_pending_orders_for_symbol(redis_client, "EURUSD")
        print(f"   Pending orders after market data: {len(pending_orders_after)}")
        
        if len(pending_orders_after) < len(pending_orders):
            print("   ✅ Order was triggered!")
        else:
            print("   ❌ Order was not triggered")
        
        # Step 6: Clean up test order
        print("\n6. Cleaning up test order...")
        try:
            from app.services.pending_orders import remove_pending_order
            await remove_pending_order(redis_client, "TEST123456", "EURUSD", "BUY_STOP", "1")
            print("   ✅ Test order removed")
        except Exception as e:
            print(f"   ❌ Failed to remove test order: {e}")
        
        print("\n=== Test Completed ===")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await redis_client.close()

if __name__ == "__main__":
    asyncio.run(test_pubsub_simple()) 