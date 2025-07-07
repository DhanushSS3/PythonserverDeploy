#!/usr/bin/env python3
"""
Simple script to get a JWT token for WebSocket testing
"""

import requests
import json
import sys

def get_token(base_url, email, password, user_type="live"):
    """Get JWT token by logging in"""
    
    # Determine the login endpoint based on user type
    if user_type == "demo":
        login_url = f"{base_url}/api/v1/demo-users/login"
    else:
        login_url = f"{base_url}/api/v1/users/login"
    
    login_data = {
        "email": email,
        "password": password,
        "user_type": user_type
    }
    
    try:
        print(f"Attempting to login to: {login_url}")
        print(f"Email: {email}")
        print(f"User Type: {user_type}")
        
        response = requests.post(login_url, json=login_data, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            access_token = data.get('access_token')
            if access_token:
                print("✅ Login successful!")
                print(f"Access Token: {access_token}")
                return access_token
            else:
                print("❌ No access token in response")
                print(f"Response: {data}")
                return None
        else:
            print(f"❌ Login failed with status code: {response.status_code}")
            print(f"Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def main():
    if len(sys.argv) < 4:
        print("Usage: python get_test_token.py <base_url> <email> <password> [user_type]")
        print("Example: python get_test_token.py http://localhost:8000 user@example.com password123 live")
        print("Example: python get_test_token.py http://localhost:8000 demo@example.com password123 demo")
        sys.exit(1)
    
    base_url = sys.argv[1].rstrip('/')
    email = sys.argv[2]
    password = sys.argv[3]
    user_type = sys.argv[4] if len(sys.argv) > 4 else "live"
    
    print("🔑 JWT Token Generator for WebSocket Testing")
    print("=" * 50)
    
    token = get_token(base_url, email, password, user_type)
    
    if token:
        print("\n" + "=" * 50)
        print("🎯 Use this token with the WebSocket performance test:")
        print(f"python test_websocket_performance.py --url {base_url} --token {token} --connections 10")
        print("\nOr for a capacity test:")
        print(f"python test_websocket_performance.py --url {base_url} --token {token} --capacity-test --max-capacity 100")
    else:
        print("\n❌ Failed to get token. Please check your credentials and server status.")
        sys.exit(1)

if __name__ == "__main__":
    main() 