# WebSocket Performance Testing Guide

This guide explains how to use the WebSocket performance testing tools to evaluate your market data WebSocket endpoint.

## Overview

The testing suite includes:
- **`test_websocket_performance.py`** - Main performance testing script
- **`get_test_token.py`** - Helper script to get JWT tokens
- **`run_websocket_test.py`** - Test runner with predefined configurations

## Prerequisites

1. **Python Dependencies**: Make sure you have the required packages installed:
   ```bash
   pip install aiohttp requests
   ```

2. **Server Running**: Ensure your FastAPI server is running and accessible

3. **Valid User Account**: You need a valid user account (live or demo) to get a JWT token

## Quick Start

### Step 1: Get a JWT Token

Use the helper script to get a token:

```bash
# For live user
python get_test_token.py http://localhost:8000 your_email@example.com your_password live

# For demo user
python get_test_token.py http://localhost:8000 demo@example.com demo_password demo
```

### Step 2: Run Basic Test

```bash
python test_websocket_performance.py \
  --url http://localhost:8000 \
  --token YOUR_JWT_TOKEN \
  --connections 10 \
  --target-messages 5 \
  --message-timeout 10.0
```

### Step 3: Run Capacity Test

```bash
python test_websocket_performance.py \
  --url http://localhost:8000 \
  --token YOUR_JWT_TOKEN \
  --capacity-test \
  --max-capacity 100
```

## Test Configurations

### Basic Test (10 connections)
- Tests 10 concurrent WebSocket connections
- Waits for 5 messages per connection or 10 seconds
- Good for initial validation

### Capacity Test
- Gradually increases connection count to find server limits
- Tests from 10 to 100 connections in steps of 10
- Identifies when success rate drops below 90%

### Stress Test (50 connections)
- Tests 50 concurrent connections
- Good for stress testing under load

## Using the Test Runner

The `run_websocket_test.py` script provides easy access to predefined test configurations:

```bash
# Run basic test
python run_websocket_test.py http://localhost:8000 YOUR_TOKEN basic

# Run capacity test
python run_websocket_test.py http://localhost:8000 YOUR_TOKEN capacity

# Run stress test
python run_websocket_test.py http://localhost:8000 YOUR_TOKEN stress

# Run all tests
python run_websocket_test.py http://localhost:8000 YOUR_TOKEN all
```

## Command Line Options

### Main Script Options

| Option | Description | Default |
|--------|-------------|---------|
| `--url` | Base URL of your FastAPI server | Required |
| `--token` | JWT token for authentication | Required |
| `--connections` | Number of concurrent connections | 50 |
| `--capacity-test` | Run capacity test to find max connections | False |
| `--start-capacity` | Starting connections for capacity test | 10 |
| `--max-capacity` | Maximum connections for capacity test | 200 |
| `--capacity-step` | Step size for capacity test | 10 |
| `--target-messages` | Target messages per connection | 5 |
| `--message-timeout` | Timeout for message collection (seconds) | 10.0 |
| `--output` | CSV output filename | websocket_test_results.csv |
| `--report` | Report output filename | websocket_test_report.txt |

## Understanding the Results

### Connection Statistics
- **Success Rate**: Percentage of successful connections
- **Connection Times**: Average, median, min, max connection establishment times
- **Message Statistics**: Total messages received and distribution

### Message Analysis
The script tracks:
- Number of messages received per connection
- Time to first message
- Time to last message
- Messages per second rate
- Whether target message count was achieved

### Message Format Tracking
For `market_update` messages, the script logs:
- Account summary (balance, margin, open/pending orders)
- Market prices count
- Message types received

### CSV Output
The CSV file includes:
- Connection ID and success status
- Connection and message timing details
- Messages per second calculation
- Target achievement status
- Summary row with totals

## Example Output

```
=== WebSocket Performance Test Report ===
Generated: 2025-01-06 15:30:45

SUMMARY:
- Total Connections Tested: 10
- Successful Connections: 10
- Failed Connections: 0
- Success Rate: 100.00%

CONNECTION TIMES (successful connections only):
- Average: 0.245s
- Median: 0.238s
- Minimum: 0.201s
- Maximum: 0.312s

MESSAGE STATISTICS:
- Total Messages Received: 67
- Average Messages per Connection: 6.7
- Median Messages per Connection: 7.0
- Minimum Messages: 5
- Maximum Messages: 8
- Connections with 5+ Messages: 10/10 (100.0%)

MESSAGE DISTRIBUTION:
- 5 messages: 2 connections (20.0%)
- 6 messages: 3 connections (30.0%)
- 7 messages: 3 connections (30.0%)
- 8 messages: 2 connections (20.0%)
```

## Troubleshooting

### Common Issues

1. **Connection Timeouts**
   - Increase the `--message-timeout` value
   - Check server load and resources

2. **Authentication Errors**
   - Verify your JWT token is valid and not expired
   - Check user account status (active/verified)

3. **Low Success Rates**
   - Reduce the number of concurrent connections
   - Check server capacity and configuration
   - Monitor server logs for errors

4. **Few Messages Received**
   - Increase `--message-timeout` to allow more time
   - Check if market data is being sent regularly
   - Verify WebSocket endpoint is working correctly

### Debug Mode

Enable debug logging by modifying the script:
```python
logging.basicConfig(level=logging.DEBUG)
```

## Performance Benchmarks

### Good Performance Indicators
- **Success Rate**: >95% for normal load
- **Connection Time**: <1 second average
- **Message Rate**: >1 message per second per connection
- **Target Achievement**: >90% of connections reach target message count

### Capacity Guidelines
- **Development**: 10-50 connections
- **Testing**: 50-200 connections  
- **Production**: 200+ connections (depending on server specs)

## Advanced Usage

### Custom Test Scenarios

You can create custom test scenarios by modifying the parameters:

```bash
# Test with higher message targets
python test_websocket_performance.py \
  --url http://localhost:8000 \
  --token YOUR_TOKEN \
  --connections 20 \
  --target-messages 10 \
  --message-timeout 15.0

# Test with longer timeouts
python test_websocket_performance.py \
  --url http://localhost:8000 \
  --token YOUR_TOKEN \
  --connections 30 \
  --target-messages 3 \
  --message-timeout 20.0
```

### Continuous Monitoring

For continuous monitoring, you can schedule regular tests:

```bash
# Run basic test every 5 minutes
while true; do
  python test_websocket_performance.py --url http://localhost:8000 --token YOUR_TOKEN --connections 10
  sleep 300
done
```

## Integration with CI/CD

You can integrate these tests into your CI/CD pipeline:

```yaml
# Example GitHub Actions workflow
- name: WebSocket Performance Test
  run: |
    python get_test_token.py ${{ secrets.API_URL }} ${{ secrets.USER_EMAIL }} ${{ secrets.USER_PASSWORD }} > token.txt
    TOKEN=$(tail -1 token.txt | cut -d' ' -f3)
    python test_websocket_performance.py --url ${{ secrets.API_URL }} --token $TOKEN --connections 20
```

## Support

If you encounter issues:
1. Check the server logs for errors
2. Verify your JWT token is valid
3. Test with fewer connections first
4. Review the detailed CSV output for specific connection failures 


   python get_test_token.py http://localhost:8000 dhanush777ss@gmail.com 8618025298@Dh live

## Understanding WebSocket Message Flow

### How Your WebSocket System Works

Your WebSocket endpoint only sends messages when there are **actual market data updates** from Firebase:

1. **Firebase** → sends real-time market data
2. **Firebase Stream** → processes and queues data  
3. **Redis Publisher** → publishes to Redis channels
4. **WebSocket Listeners** → forward updates to connected clients

### Why You See Few Messages in Tests

During testing, you typically see only **1-2 messages per connection** because:
- **Initial connection**: 1 message (loading + initial market data)
- **Market updates**: 1+ messages (only if Firebase sends new data during test)

This is **normal behavior** for a real trading system where market data updates depend on external sources.

### Testing with Simulated Market Data

To test WebSocket performance with more messages, use the market data simulation tool:

```bash
# Simulate market data updates (every 2 seconds for 60 seconds)
python simulate_market_data.py --duration 60 --interval 2.0

# Simulate order updates for a specific user
python simulate_market_data.py --user-id 123 --orders-only

# Run both market and order simulations
python simulate_market_data.py --user-id 123 --duration 30 --interval 1.0
```

### Testing Workflow

1. **Start your FastAPI server**
2. **Start market data simulation** (optional, for more messages)
3. **Run WebSocket performance test**
4. **Analyze results**

Example:
```bash
# Terminal 1: Start simulation
python simulate_market_data.py --duration 120 --interval 1.0

# Terminal 2: Run WebSocket test
python test_websocket_performance.py \
  --url http://localhost:8000 \
  --token YOUR_TOKEN \
  --connections 20 \
  --target-messages 10 \
  --message-timeout 15.0
```

## Prerequisites