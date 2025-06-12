"""
Test script for the favorites API endpoints.
Run this with: python test_favorites_api.py
"""
import asyncio
import httpx
import json
import sys
import os

# API base URL
BASE_URL = "http://127.0.0.1:8000/api/v1"

# Login credentials - use environment variables or specify directly
DEFAULT_EMAIL = "user@example.com"
DEFAULT_PASSWORD = "password123" 

# Test with these credentials - modify as needed
TEST_EMAIL = os.environ.get("TEST_USER_EMAIL", DEFAULT_EMAIL)
TEST_PASSWORD = os.environ.get("TEST_USER_PASSWORD", DEFAULT_PASSWORD)

async def test_login():
    """Test login and get JWT token."""
    print(f"Testing login with email: {TEST_EMAIL}")
    
    credentials = {
        "email": TEST_EMAIL,
        "password": TEST_PASSWORD
    }
    
    async with httpx.AsyncClient() as client:
        # Step 1: Login to get access token
        print("Attempting to login...")
        
        login_url = f"{BASE_URL}/users/login"
        print(f"Login URL: {login_url}")
        print(f"Request JSON: {json.dumps(credentials)}")
        
        try:
            login_response = await client.post(
                login_url, 
                json=credentials,
                timeout=10.0
            )
            
            print(f"Login status code: {login_response.status_code}")
            print(f"Login response headers: {dict(login_response.headers)}")
            print(f"Login response text: {login_response.text}")
            
            if login_response.status_code == 200:
                login_data = login_response.json()
                access_token = login_data.get("access_token")
                print(f"✅ Login successful! Access token: {access_token[:10]}...")
                return True
            else:
                print(f"❌ Login failed: {login_response.status_code}")
                return False
                
        except Exception as e:
            print(f"❌ Exception during login: {e}")
            return False

if __name__ == "__main__":
    result = asyncio.run(test_login())
    if not result:
        sys.exit(1) 