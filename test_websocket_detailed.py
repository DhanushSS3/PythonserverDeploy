#!/usr/bin/env python3
"""
Detailed WebSocket Testing Script
Captures and analyzes every message with timestamps and content details
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

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('websocket_detailed_test.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class DetailedMessage:
    """Data class to store detailed message information"""
    connection_id: int
    message_number: int
    timestamp: float
    message_type: str
    content_preview: str
    message_size: int
    time_since_connection: float

@dataclass
class DetailedTestResult:
    """Data class to store detailed test results"""
    connection_id: int
    connection_time: float
    success: bool
    error_message: str = ""
    total_messages: int = 0
    messages: List[DetailedMessage] = None
    first_message_time: float = 0
    last_message_time: float = 0
    total_duration: float = 0
    message_types: Dict[str, int] = None
    
    def __post_init__(self):
        if self.messages is None:
            self.messages = []
        if self.message_types is None:
            self.message_types = {}

class DetailedWebSocketTester:
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip('/')
        self.token = token
        self.results: List[DetailedTestResult] = []
        
    async def test_single_connection_detailed(self, connection_id: int, timeout: int = 30, message_timeout: float = 15.0) -> DetailedTestResult:
        """Test a single WebSocket connection with detailed message tracking"""
        start_time = time.time()
        connection_time = 0
        success = False
        error_message = ""
        messages = []
        message_types = {}
        first_message_time = 0
        last_message_time = 0
        
        try:
            # Create WebSocket URL with token
            ws_url = f"{self.base_url}/api/v1/ws/market-data?token={self.token}"
            
            # Measure connection time
            connect_start = time.time()
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(ws_url, timeout=aiohttp.ClientTimeout(total=timeout)) as ws:
                    connect_end = time.time()
                    connection_time = connect_end - connect_start
                    success = True
                    
                    logger.info(f"Connection {connection_id}: Established in {connection_time:.3f}s")
                    
                    # Listen for messages with detailed tracking
                    message_start = time.time()
                    message_count = 0
                    
                    try:
                        while time.time() - message_start < message_timeout:
                            # Calculate remaining timeout
                            remaining_timeout = message_timeout - (time.time() - message_start)
                            if remaining_timeout <= 0:
                                break
                                
                            msg = await asyncio.wait_for(ws.receive_json(), timeout=remaining_timeout)
                            message_count += 1
                            current_time = time.time()
                            time_since_connection = current_time - start_time
                            
                            # Parse message details
                            msg_type = msg.get('type', 'unknown')
                            msg_content = json.dumps(msg, indent=2)
                            msg_size = len(msg_content)
                            
                            # Track message types
                            message_types[msg_type] = message_types.get(msg_type, 0) + 1
                            
                            # Create detailed message record
                            detailed_msg = DetailedMessage(
                                connection_id=connection_id,
                                message_number=message_count,
                                timestamp=current_time,
                                message_type=msg_type,
                                content_preview=msg_content[:200] + "..." if len(msg_content) > 200 else msg_content,
                                message_size=msg_size,
                                time_since_connection=time_since_connection
                            )
                            messages.append(detailed_msg)
                            
                            # Track timing
                            if first_message_time == 0:
                                first_message_time = time_since_connection
                            last_message_time = time_since_connection
                            
                            # Log detailed message info
                            logger.info(f"Connection {connection_id}: Message {message_count} - Type: {msg_type} - Size: {msg_size} bytes - Time: {time_since_connection:.3f}s")
                            
                            # Log detailed content for first few messages
                            if message_count <= 3:
                                logger.info(f"Connection {connection_id}: Message {message_count} Content:")
                                logger.info(f"  Type: {msg_type}")
                                logger.info(f"  Size: {msg_size} bytes")
                                logger.info(f"  Time since connection: {time_since_connection:.3f}s")
                                
                                # Log specific details for market_update messages
                                if msg_type == 'market_update' and 'data' in msg:
                                    data = msg['data']
                                    if 'account_summary' in data:
                                        account = data['account_summary']
                                        logger.info(f"  Account Summary:")
                                        logger.info(f"    Balance: {account.get('balance', 'N/A')}")
                                        logger.info(f"    Margin: {account.get('margin', 'N/A')}")
                                        logger.info(f"    Open Orders: {len(account.get('open_orders', []))}")
                                        logger.info(f"    Pending Orders: {len(account.get('pending_orders', []))}")
                                    
                                    if 'market_prices' in data:
                                        market_count = len(data['market_prices'])
                                        logger.info(f"  Market Prices: {market_count} symbols")
                                        # Log first few symbols
                                        symbols = list(data['market_prices'].keys())[:5]
                                        logger.info(f"  Sample Symbols: {symbols}")
                                
                                logger.info(f"  Full Content Preview:")
                                logger.info(f"    {detailed_msg.content_preview}")
                                logger.info("")
                            
                            # Log progress every 5 messages
                            if message_count % 5 == 0:
                                logger.info(f"Connection {connection_id}: Received {message_count} messages so far...")
                                
                    except asyncio.TimeoutError:
                        logger.info(f"Connection {connection_id}: Message collection timeout after {message_timeout}s with {message_count} messages")
                    except Exception as msg_error:
                        logger.warning(f"Connection {connection_id}: Error receiving messages: {msg_error}")
                    
                    # Keep connection alive briefly
                    await asyncio.sleep(1)
                    
        except asyncio.TimeoutError:
            error_message = "Connection timeout"
            logger.error(f"Connection {connection_id}: Timeout after {timeout}s")
        except Exception as e:
            error_message = str(e)
            logger.error(f"Connection {connection_id}: Error - {e}")
        
        total_duration = time.time() - start_time
        
        return DetailedTestResult(
            connection_id=connection_id,
            connection_time=connection_time,
            success=success,
            error_message=error_message,
            total_messages=len(messages),
            messages=messages,
            first_message_time=first_message_time,
            last_message_time=last_message_time,
            total_duration=total_duration,
            message_types=message_types
        )
    
    async def test_multiple_connections_detailed(self, num_connections: int, delay_between: float = 0.1) -> List[DetailedTestResult]:
        """Test multiple WebSocket connections with detailed tracking"""
        logger.info(f"Starting detailed test with {num_connections} connections")
        
        tasks = []
        for i in range(num_connections):
            task = asyncio.create_task(self.test_single_connection_detailed(i + 1))
            tasks.append(task)
            
            if delay_between > 0 and i < num_connections - 1:
                await asyncio.sleep(delay_between)
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append(DetailedTestResult(
                    connection_id=i + 1,
                    connection_time=0,
                    success=False,
                    error_message=str(result)
                ))
            else:
                processed_results.append(result)
        
        return processed_results
    
    def generate_detailed_report(self, results: List[DetailedTestResult]) -> str:
        """Generate a comprehensive detailed test report"""
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
        total_messages = sum(r.total_messages for r in successful_results)
        messages_per_connection = [r.total_messages for r in successful_results]
        avg_messages = statistics.mean(messages_per_connection) if messages_per_connection else 0
        median_messages = statistics.median(messages_per_connection) if messages_per_connection else 0
        min_messages = min(messages_per_connection) if messages_per_connection else 0
        max_messages = max(messages_per_connection) if messages_per_connection else 0
        
        # Message type analysis
        all_message_types = {}
        for result in successful_results:
            for msg_type, count in result.message_types.items():
                all_message_types[msg_type] = all_message_types.get(msg_type, 0) + count
        
        # Timing analysis
        first_message_times = [r.first_message_time for r in successful_results if r.first_message_time > 0]
        avg_first_message_time = statistics.mean(first_message_times) if first_message_times else 0
        
        # Generate report
        report = f"""
=== Detailed WebSocket Performance Test Report ===
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
- Total Messages Received: {total_messages}
- Average Messages per Connection: {avg_messages:.1f}
- Median Messages per Connection: {median_messages:.1f}
- Minimum Messages: {min_messages}
- Maximum Messages: {max_messages}
- Average Time to First Message: {avg_first_message_time:.3f}s

MESSAGE TYPE DISTRIBUTION:
"""
        
        for msg_type, count in all_message_types.items():
            percentage = (count / total_messages * 100) if total_messages > 0 else 0
            report += f"- {msg_type}: {count} messages ({percentage:.1f}%)\n"
        
        # Connection-by-connection breakdown
        report += f"""
CONNECTION BREAKDOWN:
"""
        
        for result in successful_results:
            report += f"- Connection {result.connection_id}: {result.total_messages} messages"
            if result.message_types:
                types_str = ", ".join([f"{t}: {c}" for t, c in result.message_types.items()])
                report += f" ({types_str})"
            report += f" - First msg: {result.first_message_time:.3f}s\n"
        
        if failed_results:
            report += f"""
FAILURE ANALYSIS:
"""
            error_counts = {}
            for result in failed_results:
                error_type = result.error_message.split(':')[0] if ':' in result.error_message else result.error_message
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
            
            for error_type, count in error_counts.items():
                report += f"- {error_type}: {count} occurrences\n"
        
        return report
    
    def save_detailed_results_to_csv(self, results: List[DetailedTestResult], filename: str = "websocket_detailed_results.csv"):
        """Save detailed test results to CSV file"""
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            # Main results
            fieldnames = ['connection_id', 'success', 'connection_time', 'total_messages', 
                         'first_message_time', 'last_message_time', 'total_duration', 'error_message']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for result in results:
                writer.writerow({
                    'connection_id': result.connection_id,
                    'success': result.success,
                    'connection_time': f"{result.connection_time:.3f}",
                    'total_messages': result.total_messages,
                    'first_message_time': f"{result.first_message_time:.3f}",
                    'last_message_time': f"{result.last_message_time:.3f}",
                    'total_duration': f"{result.total_duration:.3f}",
                    'error_message': result.error_message
                })
        
        # Save detailed message log
        message_filename = filename.replace('.csv', '_messages.csv')
        with open(message_filename, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['connection_id', 'message_number', 'timestamp', 'message_type', 
                         'message_size', 'time_since_connection', 'content_preview']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            
            for result in results:
                for msg in result.messages:
                    writer.writerow({
                        'connection_id': msg.connection_id,
                        'message_number': msg.message_number,
                        'timestamp': f"{msg.timestamp:.3f}",
                        'message_type': msg.message_type,
                        'message_size': msg.message_size,
                        'time_since_connection': f"{msg.time_since_connection:.3f}",
                        'content_preview': msg.content_preview.replace('\n', ' ').replace('\r', ' ')
                    })
        
        logger.info(f"Detailed results saved to {filename}")
        logger.info(f"Message details saved to {message_filename}")

async def main():
    parser = argparse.ArgumentParser(description='Detailed WebSocket Performance Testing Tool')
    parser.add_argument('--url', required=True, help='Base URL of your FastAPI server')
    parser.add_argument('--token', required=True, help='JWT token for authentication')
    parser.add_argument('--connections', type=int, default=5, help='Number of concurrent connections to test')
    parser.add_argument('--message-timeout', type=float, default=15.0, help='Timeout for message collection')
    parser.add_argument('--output', default='websocket_detailed_results.csv', help='Output CSV filename')
    parser.add_argument('--report', default='websocket_detailed_report.txt', help='Output report filename')
    
    args = parser.parse_args()
    
    # Create tester
    tester = DetailedWebSocketTester(args.url, args.token)
    
    try:
        logger.info(f"Running detailed WebSocket test with {args.connections} connections...")
        results = await tester.test_multiple_connections_detailed(args.connections)
        
        # Generate and save report
        report = tester.generate_detailed_report(results)
        
        with open(args.report, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(report)
        logger.info(f"Detailed report saved to {args.report}")
        
        # Save results to CSV
        tester.save_detailed_results_to_csv(results, args.output)
        
    except KeyboardInterrupt:
        logger.info("Test interrupted by user")
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(main()) 