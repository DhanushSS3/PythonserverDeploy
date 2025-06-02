import jwt
import time

# Replace these with your actual values!
SECRET_KEY = "your_actual_secret_key"
ALGORITHM = "HS256"

payload = {
    "sub": "1",              # User ID or subject
    "user_type": "demo",     # User type
    "exp": int(time.time()) + 3600,  # Expires in 1 hour
    "iat": int(time.time())           # Issued at
}

token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
print("JWT Token:", token)