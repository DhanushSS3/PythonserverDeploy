#!/usr/bin/env python3
"""
Script to simulate market data updates for WebSocket testing
This will send fake market data to Redis channels to test WebSocket message flow
"""

import asyncio
import json
import random
import time
from datetime import datetime
from decimal import Decimal
import sys
import os

# Add the app directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app.dependencies.redis_client import get_redis_client
from app.core.cache import DecimalEncoder, REDIS_MARKET_DATA_CHANNEL

async def simulate_market_data_updates(duration_seconds: int = 60, interval_seconds: float = 2.0):
    """
    Simulate market data updates for testing WebSocket performance
    
    Args:
        duration_seconds: How long to run the simulation
        interval_seconds: How often to send updates
    """
    
    # Sample market symbols and base prices
    symbols = {
        'EURUSD': {'base_buy': 1.0850, 'base_sell': 1.0845},
        'GBPUSD': {'base_buy': 1.2650, 'base_sell': 1.2645},
        'USDJPY': {'base_buy': 148.50, 'base_sell': 148.45},
        'AUDUSD': {'base_buy': 0.6650, 'base_sell': 0.6645},
        'USDCAD': {'base_buy': 1.3550, 'base_sell': 1.3545},
        'BTCUSD': {'base_buy': 45000.0, 'base_sell': 44995.0},
        'XAUUSD': {'base_buy': 2050.0, 'base_sell': 2049.5},
        'EURGBP': {'base_buy': 0.8580, 'base_sell': 0.8575},
        'GBPJPY': {'base_buy': 187.80, 'base_sell': 187.75},
        'AUDCAD': {'base_buy': 0.8950, 'base_sell': 0.8945}
    }
    
    print(f"🚀 Starting market data simulation for {duration_seconds} seconds")
    print(f"📊 Sending updates every {interval_seconds} seconds")
    print(f"💱 Simulating {len(symbols)} symbols")
    print("=" * 60)
    
    try:
        redis_client = await get_redis_client()
        if not redis_client:
            print("❌ Failed to connect to Redis")
            return
        
        start_time = time.time()
        update_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Create market data update
            market_data = {}
            
            for symbol, base_prices in symbols.items():
                # Add some random variation to simulate real market movement
                variation = random.uniform(-0.001, 0.001)
                
                buy_price = base_prices['base_buy'] + variation
                sell_price = base_prices['base_sell'] + variation
                
                # Ensure sell is always lower than buy
                if sell_price >= buy_price:
                    sell_price = buy_price - 0.0005
                
                market_data[symbol] = {
                    'b': round(buy_price, 5),   # Bid (sell)
                    'o': round(sell_price, 5)   # Offer (buy)
                }
            
            # Add timestamp
            market_data['_timestamp'] = time.time()
            market_data['type'] = 'market_data_update'
            
            # Publish to Redis
            try:
                message = json.dumps(market_data, cls=DecimalEncoder)
                result = await redis_client.publish(REDIS_MARKET_DATA_CHANNEL, message)
                update_count += 1
                
                print(f"📈 Update {update_count}: Published market data to {result} subscribers")
                
                # Show a sample of the data
                if update_count == 1:
                    sample_data = {k: v for k, v in list(market_data.items())[:3] if k != '_timestamp' and k != 'type'}
                    print(f"   Sample data: {json.dumps(sample_data, indent=2)}")
                
            except Exception as e:
                print(f"❌ Error publishing market data: {e}")
            
            # Wait for next update
            await asyncio.sleep(interval_seconds)
        
        print("=" * 60)
        print(f"✅ Simulation completed!")
        print(f"📊 Total updates sent: {update_count}")
        print(f"⏱️  Duration: {duration_seconds} seconds")
        print(f"📈 Average rate: {update_count / duration_seconds:.2f} updates/second")
        
    except Exception as e:
        print(f"❌ Simulation error: {e}")
    finally:
        if 'redis_client' in locals():
            await redis_client.close()

async def simulate_order_updates(user_id: int, duration_seconds: int = 30, interval_seconds: float = 5.0):
    """
    Simulate order updates for testing WebSocket order notifications
    """
    
    print(f"📋 Starting order update simulation for user {user_id}")
    print(f"⏰ Sending updates every {interval_seconds} seconds for {duration_seconds} seconds")
    print("=" * 60)
    
    try:
        redis_client = await get_redis_client()
        if not redis_client:
            print("❌ Failed to connect to Redis")
            return
        
        from app.core.cache import REDIS_ORDER_UPDATES_CHANNEL
        
        start_time = time.time()
        update_count = 0
        
        while time.time() - start_time < duration_seconds:
            # Create order update message
            order_update = {
                'type': 'ORDER_UPDATE',
                'user_id': user_id,
                'timestamp': datetime.now().isoformat(),
                'order_id': f"TEST_{update_count + 1}",
                'action': 'simulated_update'
            }
            
            try:
                message = json.dumps(order_update, cls=DecimalEncoder)
                result = await redis_client.publish(REDIS_ORDER_UPDATES_CHANNEL, message)
                update_count += 1
                
                print(f"📋 Order Update {update_count}: Published to {result} subscribers")
                
            except Exception as e:
                print(f"❌ Error publishing order update: {e}")
            
            await asyncio.sleep(interval_seconds)
        
        print("=" * 60)
        print(f"✅ Order simulation completed! Sent {update_count} updates")
        
    except Exception as e:
        print(f"❌ Order simulation error: {e}")
    finally:
        if 'redis_client' in locals():
            await redis_client.close()

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description='Simulate market data for WebSocket testing')
    parser.add_argument('--duration', type=int, default=60, help='Duration in seconds (default: 60)')
    parser.add_argument('--interval', type=float, default=2.0, help='Update interval in seconds (default: 2.0)')
    parser.add_argument('--user-id', type=int, help='User ID for order updates (optional)')
    parser.add_argument('--orders-only', action='store_true', help='Only simulate order updates')
    parser.add_argument('--market-only', action='store_true', help='Only simulate market data')
    
    args = parser.parse_args()
    
    print("🎯 Market Data Simulation Tool")
    print("=" * 60)
    
    async def run_simulation():
        if args.orders_only:
            if not args.user_id:
                print("❌ --user-id is required for order simulation")
                return
            await simulate_order_updates(args.user_id, args.duration, args.interval)
        elif args.market_only:
            await simulate_market_data_updates(args.duration, args.interval)
        else:
            # Run both simulations concurrently
            tasks = []
            tasks.append(simulate_market_data_updates(args.duration, args.interval))
            
            if args.user_id:
                tasks.append(simulate_order_updates(args.user_id, args.duration, args.interval))
            
            await asyncio.gather(*tasks)
    
    try:
        asyncio.run(run_simulation())
    except KeyboardInterrupt:
        print("\n⏹️  Simulation stopped by user")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main() 