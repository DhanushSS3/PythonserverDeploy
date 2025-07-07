#!/usr/bin/env python3
"""
WebSocket Performance Testing Script
Tests connection capacity, connection time, and concurrent connections for your market data WebSocket endpoint.
"""

import asyncio
import aiohttp
import time
import json
import statistics
from typing import List, Dict, Any
import argparse
import logging
from dataclasses import dataclass
from datetime import datetime
import csv
import os

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('websocket_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TestResult:
    """Data class to store test results"""
    connection_id: int
    connection_time: float
    success: bool
    error_message: str = ""
    messages_received: int = 0
    first_message_time: float = 0
    last_message_time: float = 0
    total_duration: float = 0

class WebSocketTester:
    def __init__(self, base_url: str, token: str, max_connections: int = 100):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.max_connections = max_connections
        self.results: List[TestResult] = []
        self.active_connections = 0
        self.connection_lock = asyncio.Lock()
        self.target_messages = 5
        self.message_timeout = 10.0
        
    async def test_single_connection(self, connection_id: int, timeout: int = 30) -> TestResult:
        """Test a single WebSocket connection"""
        start_time = time.time()
        connection_time = 0
        success = False
        error_message = ""
        messages_received = 0
        first_message_time = 0
        last_message_time = 0
        
        try:
            # Create WebSocket URL with token
            # ws_url = f"{self.base_url}/ws/market-data?token={self.token}"
            ws_url = f"{self.base_url}/api/v1/ws/market-data?token={self.token}"
            # Measure connection time
            connect_start = time.time()
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, timeout=aiohttp.ClientTimeout(total=timeout)) as ws:
                    connect_end = time.time()
                    connection_time = connect_end - connect_start
                    success = True
                    
                    # Track active connections
                    async with self.connection_lock:
                        self.active_connections += 1
                        current_active = self.active_connections
                    
                    logger.info(f"Connection {connection_id}: Established in {connection_time:.3f}s (Active: {current_active})")
                    
                    # Listen for messages - wait for at least 5 messages or up to 10 seconds
                    message_start = time.time()
                    message_timeout = self.message_timeout
                    target_messages = self.target_messages
                    
                    try:
                        while messages_received < target_messages:
                            # Check if we've exceeded the message timeout
                            if time.time() - message_start > message_timeout:
                                logger.info(f"Connection {connection_id}: Reached {message_timeout}s timeout with {messages_received} messages")
                                break
                            
                            # Wait for next message with remaining timeout
                            remaining_timeout = message_timeout - (time.time() - message_start)
                            if remaining_timeout <= 0:
                                break
                                
                            msg = await asyncio.wait_for(ws.receive_json(), timeout=remaining_timeout)
                            messages_received += 1
                            
                            if first_message_time == 0:
                                first_message_time = time.time() - start_time
                            
                            last_message_time = time.time() - start_time
                            
                            # Log message details for debugging
                            msg_type = msg.get('type', 'unknown')
                            if messages_received <= 3:  # Log first 3 messages
                                logger.debug(f"Connection {connection_id}: Message {messages_received} - Type: {msg_type}")
                                
                                # Log additional details for market_update messages
                                if msg_type == 'market_update' and 'data' in msg:
                                    data = msg['data']
                                    if 'account_summary' in data:
                                        balance = data['account_summary'].get('balance', 'N/A')
                                        margin = data['account_summary'].get('margin', 'N/A')
                                        open_orders_count = len(data['account_summary'].get('open_orders', []))
                                        pending_orders_count = len(data['account_summary'].get('pending_orders', []))
                                        logger.debug(f"Connection {connection_id}: Account - Balance: {balance}, Margin: {margin}, Open Orders: {open_orders_count}, Pending: {pending_orders_count}")
                                    
                                    if 'market_prices' in data:
                                        market_count = len(data['market_prices'])
                                        logger.debug(f"Connection {connection_id}: Market prices count: {market_count}")
                            
                            # Log progress every 5 messages
                            if messages_received % 5 == 0:
                                logger.info(f"Connection {connection_id}: Received {messages_received} messages so far...")
                                
                    except asyncio.TimeoutError:
                        logger.info(f"Connection {connection_id}: Message collection timeout after {message_timeout}s with {messages_received} messages")
                    except Exception as msg_error:
                        logger.warning(f"Connection {connection_id}: Error receiving messages: {msg_error}")
                    
                    # Keep connection alive for a bit to test stability
                    await asyncio.sleep(1)
                    
        except asyncio.TimeoutError:
            error_message = "Connection timeout"
            logger.error(f"Connection {connection_id}: Timeout after {timeout}s")
        except Exception as e:
            error_message = str(e)
            logger.error(f"Connection {connection_id}: Error - {e}")
        finally:
            # Track active connections
            async with self.connection_lock:
                self.active_connections -= 1
            
            total_duration = time.time() - start_time
            
        return TestResult(
            connection_id=connection_id,
            connection_time=connection_time,
            success=success,
            error_message=error_message,
            messages_received=messages_received,
            first_message_time=first_message_time,
            last_message_time=last_message_time,
            total_duration=total_duration
        )
    
    async def test_concurrent_connections(self, num_connections: int, delay_between: float = 0.1) -> List[TestResult]:
        """Test multiple concurrent WebSocket connections"""
        logger.info(f"Starting concurrent connection test with {num_connections} connections")
        
        # Create tasks for all connections
        tasks = []
        for i in range(num_connections):
            task = asyncio.create_task(self.test_single_connection(i + 1))
            tasks.append(task)
            
            # Add small delay between connection attempts to avoid overwhelming the server
            if delay_between > 0 and i < num_connections - 1:
                await asyncio.sleep(delay_between)
        
        # Wait for all connections to complete
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(TestResult(
                    connection_id=i + 1,
                    connection_time=0,
                    success=False,
                    error_message=str(result)
                ))
            else:
                processed_results.append(result)
        
        return processed_results
    
    async def test_connection_capacity(self, start_connections: int = 10, max_connections: int = 500, step: int = 10) -> Dict[str, Any]:
        """Test how many connections the server can handle"""
        logger.info(f"Starting capacity test: {start_connections} to {max_connections} connections, step {step}")
        
        capacity_results = {
            'max_successful_connections': 0,
            'failure_threshold': 0,
            'connection_times': [],
            'success_rates': [],
            'test_points': []
        }
        
        for num_connections in range(start_connections, max_connections + 1, step):
            logger.info(f"Testing capacity with {num_connections} connections...")
            
            # Test this number of connections
            results = await self.test_concurrent_connections(num_connections, delay_between=0.05)
            
            # Calculate success rate
            successful = sum(1 for r in results if r.success)
            success_rate = successful / len(results) if results else 0
            
            # Calculate average connection time for successful connections
            successful_times = [r.connection_time for r in results if r.success]
            avg_connection_time = statistics.mean(successful_times) if successful_times else 0
            
            capacity_results['test_points'].append({
                'connections': num_connections,
                'successful': successful,
                'success_rate': success_rate,
                'avg_connection_time': avg_connection_time,
                'max_connection_time': max(successful_times) if successful_times else 0,
                'min_connection_time': min(successful_times) if successful_times else 0
            })
            
            logger.info(f"Capacity test {num_connections}: {successful}/{num_connections} successful ({success_rate:.1%}), avg time: {avg_connection_time:.3f}s")
            
            # If success rate drops below 90%, we've found our limit
            if success_rate < 0.9 and capacity_results['failure_threshold'] == 0:
                capacity_results['failure_threshold'] = num_connections
                logger.warning(f"Success rate dropped below 90% at {num_connections} connections")
            
            # If success rate drops below 50%, stop testing
            if success_rate < 0.5:
                logger.error(f"Success rate dropped below 50% at {num_connections} connections. Stopping capacity test.")
                break
            
            # Update max successful connections
            if successful > capacity_results['max_successful_connections']:
                capacity_results['max_successful_connections'] = successful
            
            # Small delay between capacity tests
            await asyncio.sleep(1)
        
        return capacity_results
    
    def generate_report(self, results: List[TestResult], capacity_results: Dict[str, Any] = None) -> str:
        """Generate a comprehensive test report"""
        if not results:
            return "No test results to report"
        
        successful_results = [r for r in results if r.success]
        failed_results = [r for r in results if not r.success]
        
        # Basic statistics
        total_connections = len(results)
        successful_connections = len(successful_results)
        success_rate = successful_connections / total_connections if total_connections > 0 else 0
        
        # Connection time statistics
        connection_times = [r.connection_time for r in successful_results]
        avg_connection_time = statistics.mean(connection_times) if connection_times else 0
        median_connection_time = statistics.median(connection_times) if connection_times else 0
        min_connection_time = min(connection_times) if connection_times else 0
        max_connection_time = max(connection_times) if connection_times else 0
        
        # Message statistics
        messages_received = [r.messages_received for r in successful_results]
        avg_messages = statistics.mean(messages_received) if messages_received else 0
        median_messages = statistics.median(messages_received) if messages_received else 0
        min_messages = min(messages_received) if messages_received else 0
        max_messages = max(messages_received) if messages_received else 0
        
        # Message distribution analysis
        message_distribution = {}
        for msg_count in messages_received:
            range_key = f"{msg_count} messages"
            message_distribution[range_key] = message_distribution.get(range_key, 0) + 1
        
        # Success rate by message count
        target_message_count = getattr(self, 'target_messages', 5)  # Default to 5 if not set
        connections_with_target_messages = sum(1 for r in successful_results if r.messages_received >= target_message_count)
        target_message_success_rate = connections_with_target_messages / successful_connections if successful_connections > 0 else 0
        
        # Generate report
        report = f"""
=== WebSocket Performance Test Report ===
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

SUMMARY:
- Total Connections Tested: {total_connections}
- Successful Connections: {successful_connections}
- Failed Connections: {len(failed_results)}
- Success Rate: {success_rate:.2%}

CONNECTION TIMES (successful connections only):
- Average: {avg_connection_time:.3f}s
- Median: {median_connection_time:.3f}s
- Minimum: {min_connection_time:.3f}s
- Maximum: {max_connection_time:.3f}s

MESSAGE STATISTICS:
- Total Messages Received: {sum(messages_received)}
- Average Messages per Connection: {avg_messages:.1f}
- Median Messages per Connection: {median_messages:.1f}
- Minimum Messages: {min_messages}
- Maximum Messages: {max_messages}
- Connections with {target_message_count}+ Messages: {connections_with_target_messages}/{successful_connections} ({target_message_success_rate:.1%})

MESSAGE DISTRIBUTION:
"""
        
        # Sort message distribution by count
        sorted_distribution = sorted(message_distribution.items(), key=lambda x: int(x[0].split()[0]))
        for msg_range, count in sorted_distribution:
            percentage = (count / successful_connections * 100) if successful_connections > 0 else 0
            report += f"- {msg_range}: {count} connections ({percentage:.1f}%)\n"
        
        report += f"""
FAILURE ANALYSIS:
"""
        
        if failed_results:
            error_counts = {}
            for result in failed_results:
                error_type = result.error_message.split(':')[0] if ':' in result.error_message else result.error_message
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
            
            for error_type, count in error_counts.items():
                report += f"- {error_type}: {count} occurrences\n"
        else:
            report += "- No failures recorded\n"
        
        # Add capacity test results if available
        if capacity_results:
            report += f"""
CAPACITY TEST RESULTS:
- Maximum Successful Connections: {capacity_results['max_successful_connections']}
- Failure Threshold (90% success rate): {capacity_results['failure_threshold']}

CAPACITY TEST DETAILS:
"""
            for point in capacity_results['test_points']:
                report += f"- {point['connections']} connections: {point['successful']} successful ({point['success_rate']:.1%}), avg time: {point['avg_connection_time']:.3f}s\n"
        
        return report
    
    def save_results_to_csv(self, results: List[TestResult], filename: str = "websocket_test_results.csv"):
        """Save test results to CSV file"""
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['connection_id', 'success', 'connection_time', 'messages_received', 
                         'first_message_time', 'last_message_time', 'total_duration', 'error_message',
                         'messages_per_second', 'target_achieved']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            total_messages = 0
            successful_connections = 0
            total_connection_time = 0
            
            # Get target messages from instance or default to 5
            target_messages = getattr(self, 'target_messages', 5)
            
            for result in results:
                # Calculate messages per second
                messages_per_second = 0
                if result.total_duration > 0:
                    messages_per_second = result.messages_received / result.total_duration
                
                # Check if target messages were achieved
                target_achieved = "Yes" if result.messages_received >= target_messages else "No"
                
                writer.writerow({
                    'connection_id': result.connection_id,
                    'success': result.success,
                    'connection_time': f"{result.connection_time:.3f}",
                    'messages_received': result.messages_received,
                    'first_message_time': f"{result.first_message_time:.3f}",
                    'last_message_time': f"{result.last_message_time:.3f}",
                    'total_duration': f"{result.total_duration:.3f}",
                    'error_message': result.error_message,
                    'messages_per_second': f"{messages_per_second:.2f}",
                    'target_achieved': target_achieved
                })
                
                if result.success:
                    total_messages += result.messages_received
                    successful_connections += 1
                    total_connection_time += result.connection_time
            
            # Add summary row
            if successful_connections > 0:
                avg_messages = total_messages / successful_connections
                avg_connection_time = total_connection_time / successful_connections
                writer.writerow({
                    'connection_id': 'SUMMARY',
                    'success': f"{successful_connections}/{len(results)}",
                    'connection_time': f"{avg_connection_time:.3f}",
                    'messages_received': f"{avg_messages:.1f}",
                    'first_message_time': '',
                    'last_message_time': '',
                    'total_duration': '',
                    'error_message': f"Total: {total_messages}",
                    'messages_per_second': '',
                    'target_achieved': ''
                })
        
        logger.info(f"Results saved to {filename}")

async def main():
    parser = argparse.ArgumentParser(description='WebSocket Performance Testing Tool')
    parser.add_argument('--url', required=True, help='Base URL of your FastAPI server (e.g., http://localhost:8000)')
    parser.add_argument('--token', required=True, help='JWT token for authentication')
    parser.add_argument('--connections', type=int, default=50, help='Number of concurrent connections to test')
    parser.add_argument('--capacity-test', action='store_true', help='Run capacity test to find maximum connections')
    parser.add_argument('--start-capacity', type=int, default=10, help='Starting number of connections for capacity test')
    parser.add_argument('--max-capacity', type=int, default=200, help='Maximum number of connections for capacity test')
    parser.add_argument('--capacity-step', type=int, default=10, help='Step size for capacity test')
    parser.add_argument('--target-messages', type=int, default=5, help='Target number of messages to receive per connection')
    parser.add_argument('--message-timeout', type=float, default=10.0, help='Timeout in seconds for message collection')
    parser.add_argument('--output', default='websocket_test_results.csv', help='Output CSV filename')
    parser.add_argument('--report', default='websocket_test_report.txt', help='Output report filename')
    
    args = parser.parse_args()
    
    # Create tester
    tester = WebSocketTester(args.url, args.token)
    
    # Update the target messages and timeout in the tester
    tester.target_messages = args.target_messages
    tester.message_timeout = args.message_timeout
    
    try:
        if args.capacity_test:
            logger.info("Running capacity test...")
            capacity_results = await tester.test_connection_capacity(
                start_connections=args.start_capacity,
                max_connections=args.max_capacity,
                step=args.capacity_step
            )
            
            # Run a final test with the recommended number of connections
            recommended_connections = capacity_results.get('failure_threshold', args.connections)
            if recommended_connections > 0:
                logger.info(f"Running final test with {recommended_connections} connections...")
                final_results = await tester.test_concurrent_connections(recommended_connections)
            else:
                final_results = await tester.test_concurrent_connections(args.connections)
        else:
            logger.info(f"Running concurrent connection test with {args.connections} connections...")
            final_results = await tester.test_concurrent_connections(args.connections)
            capacity_results = None
        
        # Generate and save report
        report = tester.generate_report(final_results, capacity_results)
        
        with open(args.report, 'w') as f:
            f.write(report)
        
        print(report)
        logger.info(f"Report saved to {args.report}")
        
        # Save results to CSV
        tester.save_results_to_csv(final_results, args.output)
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 