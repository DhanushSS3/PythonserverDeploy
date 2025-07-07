# WebSocket Performance Testing Guide

This guide provides comprehensive testing methods for your WebSocket endpoint to measure connection capacity, performance, and reliability.

## Prerequisites

1. **Install required packages:**
```bash
pip install aiohttp asyncio
```

2. **Get a valid JWT token** for testing (you can create one using your existing authentication endpoints)

3. **Ensure your FastAPI server is running** with the WebSocket endpoint

## Testing Tools

### 1. Simple Performance Test (`test_websocket_simple.py`)

**Basic usage:**
```bash
python test_websocket_simple.py --url http://localhost:8000 --token "your_jwt_token" --connections 20
```

**Parameters:**
- `--url`: Your FastAPI server URL
- `--token`: Valid JWT token for authentication
- `--connections`: Number of concurrent connections to test (default: 20)
- `--delay`: Delay between connection attempts in seconds (default: 0.1)

**Example outputs:**
```
=== WebSocket Test Results ===
Total Connections: 20
Successful: 18
Failed: 2
Success Rate: 90.0%

Connection Times (successful):
  Average: 0.245s
  Median: 0.231s
  Min: 0.189s
  Max: 0.412s

Messages Received:
  Average: 2.1
  Total: 38
```

### 2. Comprehensive Performance Test (`test_websocket_performance.py`)

**Basic usage:**
```bash
python test_websocket_performance.py --url http://localhost:8000 --token "your_jwt_token" --connections 50
```

**Capacity testing:**
```bash
python test_websocket_performance.py --url http://localhost:8000 --token "your_jwt_token" --capacity-test --start-capacity 10 --max-capacity 200 --capacity-step 10
```

**Parameters:**
- `--capacity-test`: Run capacity test to find maximum connections
- `--start-capacity`: Starting number of connections (default: 10)
- `--max-capacity`: Maximum number of connections to test (default: 200)
- `--capacity-step`: Step size for capacity test (default: 10)
- `--output`: CSV output filename (default: websocket_test_results.csv)
- `--report`: Report output filename (default: websocket_test_report.txt)

## Monitoring Endpoints

Your FastAPI app now includes monitoring endpoints to track WebSocket performance:

### 1. Get WebSocket Statistics
```bash
curl http://localhost:8000/api/v1/market-data/monitoring/websocket-stats
```

**Response:**
```json
{
  "total_connections": 150,
  "current_connections": 25,
  "max_concurrent_connections": 45,
  "connection_times": [0.123, 0.234, 0.189, ...],
  "failed_connections": 3,
  "last_connection_time": 1640995200.123,
  "avg_connection_time": 0.245,
  "min_connection_time": 0.189,
  "max_connection_time": 0.412,
  "success_rate": 98.0,
  "timestamp": 1640995200.456
}
```

### 2. Get Active Connections
```bash
curl http://localhost:8000/api/v1/market-data/monitoring/active-connections
```

**Response:**
```json
{
  "current_connections": 25,
  "max_concurrent_connections": 45,
  "timestamp": 1640995200.456
}
```

### 3. Reset Statistics (for testing)
```bash
curl -X POST http://localhost:8000/api/v1/market-data/monitoring/reset-stats
```

## Testing Scenarios

### 1. Basic Performance Test
```bash
# Test 20 concurrent connections
python test_websocket_simple.py --url http://localhost:8000 --token "your_token" --connections 20
```

### 2. Load Testing
```bash
# Test 100 concurrent connections
python test_websocket_simple.py --url http://localhost:8000 --token "your_token" --connections 100 --delay 0.05
```

### 3. Capacity Testing
```bash
# Find maximum capacity
python test_websocket_performance.py --url http://localhost:8000 --token "your_token" --capacity-test --start-capacity 10 --max-capacity 500 --capacity-step 20
```

### 4. Stress Testing
```bash
# Test with minimal delay to stress the server
python test_websocket_simple.py --url http://localhost:8000 --token "your_token" --connections 200 --delay 0.01
```

### 5. Continuous Monitoring
```bash
# Monitor while running tests
while true; do
  curl -s http://localhost:8000/api/v1/market-data/monitoring/active-connections | jq .
  sleep 5
done
```

## Performance Benchmarks

### Good Performance Indicators:
- **Connection Time**: < 0.5 seconds average
- **Success Rate**: > 95%
- **Concurrent Connections**: > 100 stable connections
- **Message Reception**: All connections receive initial messages

### Warning Signs:
- **Connection Time**: > 1 second average
- **Success Rate**: < 90%
- **High Failure Rate**: Many timeout or connection refused errors
- **Memory Usage**: Rapidly increasing during tests

## Troubleshooting

### Common Issues:

1. **Connection Timeouts**
   - Check server resources (CPU, memory)
   - Verify database connection pool settings
   - Check Redis connection limits

2. **High Connection Times**
   - Database queries during connection setup
   - Redis operations blocking
   - Network latency

3. **Connection Failures**
   - Invalid tokens
   - Server overload
   - Network issues

### Debugging Steps:

1. **Check server logs** during testing
2. **Monitor system resources** (CPU, memory, network)
3. **Use monitoring endpoints** to track real-time metrics
4. **Test with different connection counts** to find breaking point

## Performance Optimization Tips

1. **Database Optimization**
   - Use connection pooling
   - Optimize queries during WebSocket setup
   - Consider caching frequently accessed data

2. **Redis Optimization**
   - Monitor Redis memory usage
   - Optimize Redis operations
   - Consider Redis clustering for high load

3. **Network Optimization**
   - Use load balancers for multiple server instances
   - Optimize WebSocket message sizes
   - Consider CDN for static assets

## Example Test Results Analysis

### Good Performance Example:
```
=== WebSocket Test Results ===
Total Connections: 50
Successful: 49
Failed: 1
Success Rate: 98.0%

Connection Times (successful):
  Average: 0.234s
  Median: 0.221s
  Min: 0.189s
  Max: 0.345s

Messages Received:
  Average: 2.1
  Total: 103
```

### Poor Performance Example:
```
=== WebSocket Test Results ===
Total Connections: 50
Successful: 35
Failed: 15
Success Rate: 70.0%

Connection Times (successful):
  Average: 1.234s
  Median: 1.156s
  Min: 0.789s
  Max: 2.456s

Messages Received:
  Average: 1.2
  Total: 42
```

## Next Steps

1. **Run baseline tests** to establish current performance
2. **Identify bottlenecks** using monitoring endpoints
3. **Optimize based on findings**
4. **Re-test after optimizations**
5. **Set up continuous monitoring** for production

## Production Monitoring

For production environments, consider:

1. **Setting up alerts** for high connection times or failure rates
2. **Monitoring system resources** during peak usage
3. **Implementing circuit breakers** for overload protection
4. **Using load testing in staging** before production deployments 