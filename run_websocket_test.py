#!/usr/bin/env python3
"""
Simple script to run WebSocket performance tests with different configurations
"""

import subprocess
import sys
import os
from datetime import datetime

def run_test(test_name, command):
    """Run a test and return the result"""
    print(f"\n{'='*60}")
    print(f"🧪 Running Test: {test_name}")
    print(f"{'='*60}")
    print(f"Command: {' '.join(command)}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("-" * 60)
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print("✅ Test completed successfully")
            print("\n📊 Test Output:")
            print(result.stdout)
        else:
            print("❌ Test failed")
            print(f"Error: {result.stderr}")
            
        return result.returncode == 0
        
    except subprocess.TimeoutExpired:
        print("⏰ Test timed out after 5 minutes")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False

def main():
    if len(sys.argv) < 4:
        print("Usage: python run_websocket_test.py <base_url> <token> [test_type]")
        print("Test types:")
        print("  basic     - Basic test with 10 connections")
        print("  capacity  - Capacity test to find max connections")
        print("  stress    - Stress test with 50 connections")
        print("  all       - Run all tests")
        print("\nExample: python run_websocket_test.py http://localhost:8000 <your_token> basic")
        sys.exit(1)
    
    base_url = sys.argv[1]
    token = sys.argv[2]
    test_type = sys.argv[3] if len(sys.argv) > 3 else "basic"
    
    print("🚀 WebSocket Performance Test Runner")
    print("=" * 60)
    print(f"Base URL: {base_url}")
    print(f"Token: {token[:20]}...")
    print(f"Test Type: {test_type}")
    
    # Define test configurations
    tests = {
        "basic": {
            "name": "Basic Test (10 connections)",
            "command": [
                "python", "test_websocket_performance.py",
                "--url", base_url,
                "--token", token,
                "--connections", "10",
                "--target-messages", "5",
                "--message-timeout", "10.0",
                "--output", "basic_test_results.csv",
                "--report", "basic_test_report.txt"
            ]
        },
        "capacity": {
            "name": "Capacity Test (find max connections)",
            "command": [
                "python", "test_websocket_performance.py",
                "--url", base_url,
                "--token", token,
                "--capacity-test",
                "--start-capacity", "10",
                "--max-capacity", "100",
                "--capacity-step", "10",
                "--target-messages", "5",
                "--message-timeout", "10.0",
                "--output", "capacity_test_results.csv",
                "--report", "capacity_test_report.txt"
            ]
        },
        "stress": {
            "name": "Stress Test (50 connections)",
            "command": [
                "python", "test_websocket_performance.py",
                "--url", base_url,
                "--token", token,
                "--connections", "50",
                "--target-messages", "5",
                "--message-timeout", "10.0",
                "--output", "stress_test_results.csv",
                "--report", "stress_test_report.txt"
            ]
        }
    }
    
    # Run selected tests
    if test_type == "all":
        test_to_run = list(tests.keys())
    elif test_type in tests:
        test_to_run = [test_type]
    else:
        print(f"❌ Unknown test type: {test_type}")
        print(f"Available types: {', '.join(tests.keys())}, all")
        sys.exit(1)
    
    results = {}
    for test_key in test_to_run:
        test_config = tests[test_key]
        success = run_test(test_config["name"], test_config["command"])
        results[test_key] = success
    
    # Summary
    print(f"\n{'='*60}")
    print("📋 Test Summary")
    print(f"{'='*60}")
    for test_key, success in results.items():
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"{test_key.upper()}: {status}")
    
    passed = sum(results.values())
    total = len(results)
    print(f"\nOverall: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 All tests completed successfully!")
    else:
        print("⚠️  Some tests failed. Check the output above for details.")

if __name__ == "__main__":
    main() 