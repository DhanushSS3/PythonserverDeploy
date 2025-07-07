#!/usr/bin/env python3
"""
Simple WebSocket Performance Testing Script
"""

import asyncio
import aiohttp
import time
import json
import statistics
from typing import List
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def test_single_connection(session, url, token, connection_id):
    """Test a single WebSocket connection"""
    start_time = time.time()
    try:
        ws_url = f"{url}/api/v1/ws/market-data?token={token}"
        async with session.ws_connect(ws_url, timeout=30) as ws:
            connect_time = time.time() - start_time
            logger.info(f"Connection {connection_id}: Connected in {connect_time:.3f}s")
            
            # Wait for initial messages
            messages_received = 0
            for i in range(3):  # Wait for loading + market_update
                try:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=10.0)
                    messages_received += 1
                    if msg.get('type') == 'market_update':
                        break
                except asyncio.TimeoutError:
                    break
            
            # Keep connection alive briefly
            await asyncio.sleep(2)
            total_time = time.time() - start_time
            
            return {
                'connection_id': connection_id,
                'success': True,
                'connection_time': connect_time,
                'total_time': total_time,
                'messages_received': messages_received
            }
            
    except Exception as e:
        total_time = time.time() - start_time
        logger.error(f"Connection {connection_id}: Failed - {e}")
        return {
            'connection_id': connection_id,
            'success': False,
            'connection_time': 0,
            'total_time': total_time,
            'error': str(e)
        }

async def test_concurrent_connections(url, token, num_connections, delay=0.1):
    """Test multiple concurrent connections"""
    logger.info(f"Testing {num_connections} concurrent connections...")
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for i in range(num_connections):
            task = asyncio.create_task(test_single_connection(session, url, token, i + 1))
            tasks.append(task)
            
            if delay > 0 and i < num_connections - 1:
                await asyncio.sleep(delay)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'connection_id': i + 1,
                    'success': False,
                    'connection_time': 0,
                    'total_time': 0,
                    'error': str(result)
                })
            else:
                processed_results.append(result)
        
        return processed_results

def analyze_results(results):
    """Analyze test results"""
    successful = [r for r in results if r['success']]
    failed = [r for r in results if not r['success']]
    
    print(f"\n=== WebSocket Test Results ===")
    print(f"Total Connections: {len(results)}")
    print(f"Successful: {len(successful)}")
    print(f"Failed: {len(failed)}")
    print(f"Success Rate: {len(successful)/len(results)*100:.1f}%")
    
    if successful:
        connection_times = [r['connection_time'] for r in successful]
        print(f"\nConnection Times (successful):")
        print(f"  Average: {statistics.mean(connection_times):.3f}s")
        print(f"  Median: {statistics.median(connection_times):.3f}s")
        print(f"  Min: {min(connection_times):.3f}s")
        print(f"  Max: {max(connection_times):.3f}s")
        
        messages = [r.get('messages_received', 0) for r in successful]
        print(f"\nMessages Received:")
        print(f"  Average: {statistics.mean(messages):.1f}")
        print(f"  Total: {sum(messages)}")
    
    if failed:
        print(f"\nFailure Analysis:")
        error_counts = {}
        for r in failed:
            error = r.get('error', 'Unknown')
            error_counts[error] = error_counts.get(error, 0) + 1
        
        for error, count in error_counts.items():
            print(f"  {error}: {count} occurrences")

async def main():
    parser = argparse.ArgumentParser(description='Simple WebSocket Performance Test')
    parser.add_argument('--url', required=True, help='Base URL (e.g., http://localhost:8000)')
    parser.add_argument('--token', required=True, help='JWT token')
    parser.add_argument('--connections', type=int, default=20, help='Number of connections')
    parser.add_argument('--delay', type=float, default=0.1, help='Delay between connections')
    
    args = parser.parse_args()
    
    print(f"Testing WebSocket at {args.url}")
    print(f"Connections: {args.connections}")
    print(f"Delay: {args.delay}s")
    
    results = await test_concurrent_connections(args.url, args.token, args.connections, args.delay)
    analyze_results(results)

if __name__ == "__main__":
    asyncio.run(main()) 