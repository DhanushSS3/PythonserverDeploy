#!/usr/bin/env python3
"""
Simple JWT Token Test
"""

import jwt
import json
from datetime import datetime

def decode_token(token):
    """Decode JWT token without verification (for testing)"""
    try:
        # Decode without verification to see the payload
        decoded = jwt.decode(token, options={"verify_signature": False})
        return decoded
    except Exception as e:
        print(f"Error decoding token: {e}")
        return None

def main():
    # Your token
    token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiI0IiwidXNlcl90eXBlIjoibGl2ZSIsImFjY291bnRfbnVtYmVyIjoiRFlZNUEiLCJleHAiOjE3NTE2OTA2NjAsImlhdCI6MTc1MTY4ODg2MH0.sVWswVJJDa0ABh7-RmwEf3Q2tyXDuP7V_k7k8v_TNX8"
    
    print("🔍 JWT Token Analysis")
    print("=" * 50)
    
    # Decode the token
    payload = decode_token(token)
    if payload:
        print("✅ Token decoded successfully")
        print(f"Payload: {json.dumps(payload, indent=2)}")
        
        # Check expiration
        exp = payload.get('exp')
        if exp:
            exp_time = datetime.fromtimestamp(exp)
            now = datetime.now()
            print(f"Expiration: {exp_time}")
            print(f"Current time: {now}")
            print(f"Expired: {now > exp_time}")
        
        # Check required fields
        required_fields = ['sub', 'user_type', 'account_number']
        missing_fields = [field for field in required_fields if field not in payload]
        if missing_fields:
            print(f"❌ Missing required fields: {missing_fields}")
        else:
            print("✅ All required fields present")
            
        print(f"User Type: {payload.get('user_type')}")
        print(f"Account Number: {payload.get('account_number')}")
        print(f"Subject: {payload.get('sub')}")
    else:
        print("❌ Failed to decode token")

if __name__ == "__main__":
    main() 