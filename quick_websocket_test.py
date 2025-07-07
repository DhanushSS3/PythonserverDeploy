#!/usr/bin/env python3
"""
Quick WebSocket Connection Test
Simple script to test WebSocket connection performance
"""

import asyncio
import aiohttp
import time
import json
import sys

async def test_websocket_connection(url, token, connection_id=1):
    """Test a single WebSocket connection"""
    start_time = time.time()
    
    try:
        ws_url = f"{url}/api/v1/ws/market-data?token={token}"
        print(f"Connection {connection_id}: Attempting to connect...")
        
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(ws_url, timeout=30) as ws:
                connect_time = time.time() - start_time
                print(f"Connection {connection_id}: ✅ Connected in {connect_time:.3f}s")
                
                # Wait for initial messages
                messages_received = 0
                for i in range(3):
                    try:
                        msg = await asyncio.wait_for(ws.receive_json(), timeout=10.0)
                        messages_received += 1
                        print(f"Connection {connection_id}: 📨 Received message {i+1}: {msg.get('type', 'unknown')}")
                        
                        if msg.get('type') == 'market_update':
                            print(f"Connection {connection_id}: ✅ Received market data")
                            break
                    except asyncio.TimeoutError:
                        print(f"Connection {connection_id}: ⏰ Timeout waiting for message {i+1}")
                        break
                
                # Keep connection alive briefly
                await asyncio.sleep(2)
                total_time = time.time() - start_time
                
                print(f"Connection {connection_id}: ✅ Test completed in {total_time:.3f}s ({messages_received} messages)")
                return True, connect_time, messages_received
                
    except Exception as e:
        total_time = time.time() - start_time
        print(f"Connection {connection_id}: ❌ Failed after {total_time:.3f}s - {e}")
        return False, 0, 0

async def test_multiple_connections(url, token, num_connections=5, delay=0.5):
    """Test multiple WebSocket connections"""
    print(f"\n🚀 Testing {num_connections} WebSocket connections to {url}")
    print("=" * 60)
    
    start_time = time.time()
    results = []
    
    for i in range(num_connections):
        success, connect_time, messages = await test_websocket_connection(url, token, i + 1)
        results.append({
            'connection_id': i + 1,
            'success': success,
            'connect_time': connect_time,
            'messages': messages
        })
        
        if delay > 0 and i < num_connections - 1:
            await asyncio.sleep(delay)
    
    total_time = time.time() - start_time
    
    # Analyze results
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print("\n" + "=" * 60)
    print("📊 TEST RESULTS")
    print("=" * 60)
    print(f"Total Connections: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Success Rate: {len(successful)/len(results)*100:.1f}%")
    print(f"Total Test Time: {total_time:.3f}s")
    
    if successful:
        connect_times = [r['connect_time'] for r in successful]
        avg_time = sum(connect_times) / len(connect_times)
        min_time = min(connect_times)
        max_time = max(connect_times)
        
        print(f"\n⏱️  Connection Times (successful):")
        print(f"  Average: {avg_time:.3f}s")
        print(f"  Min: {min_time:.3f}s")
        print(f"  Max: {max_time:.3f}s")
        
        total_messages = sum(r['messages'] for r in successful)
        print(f"\n📨 Messages Received: {total_messages}")
    
    if failed:
        print(f"\n❌ Failed Connections:")
        for result in failed:
            print(f"  Connection {result['connection_id']}")
    
    return results

async def check_server_status(url):
    """Check if the server is running and accessible"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}/docs", timeout=10) as response:
                if response.status == 200:
                    print(f"✅ Server is running at {url}")
                    return True
                else:
                    print(f"❌ Server responded with status {response.status}")
                    return False
    except Exception as e:
        print(f"❌ Cannot connect to server at {url}: {e}")
        return False

async def check_monitoring_endpoints(url):
    """Check if monitoring endpoints are available"""
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{url}/api/v1/market-data/monitoring/websocket-stats", timeout=10) as response:
                if response.status == 200:
                    stats = await response.json()
                    print(f"✅ Monitoring endpoints available")
                    print(f"   Current connections: {stats.get('current_connections', 0)}")
                    print(f"   Total connections: {stats.get('total_connections', 0)}")
                    return True
                else:
                    print(f"⚠️  Monitoring endpoints not available (status: {response.status})")
                    return False
    except Exception as e:
        print(f"⚠️  Cannot access monitoring endpoints: {e}")
        return False

def main():
    if len(sys.argv) < 3:
        print("Usage: python quick_websocket_test.py <url> <token> [connections]")
        print("Example: python quick_websocket_test.py http://localhost:8000 'your_jwt_token' 5")
        sys.exit(1)
    
    url = sys.argv[1]
    token = sys.argv[2]
    num_connections = int(sys.argv[3]) if len(sys.argv) > 3 else 5
    
    print("🔍 WebSocket Quick Test")
    print("=" * 60)
    
    async def run_tests():
        # Check server status
        if not await check_server_status(url):
            print("❌ Server is not accessible. Please check if your FastAPI server is running.")
            return
        
        # Check monitoring endpoints
        await check_monitoring_endpoints(url)
        
        # Run WebSocket tests
        await test_multiple_connections(url, token, num_connections)
        
        print("\n🎉 Test completed!")
        print("\n💡 Tips:")
        print("  - Use the monitoring endpoints to track performance")
        print("  - Run with more connections to test capacity")
        print("  - Check server logs for detailed information")
    
    asyncio.run(run_tests())

if __name__ == "__main__":
    main() 