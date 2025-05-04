# app/core/security.py

# We'll use passlib for password hashing
# Make sure you have installed it: pip install passlib[bcrypt]
from passlib.context import CryptContext

# Configure the password hashing context
# bcrypt is a strong, recommended hashing algorithm
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """
    Verifies a plain password against a hashed password.

    Args:
        plain_password: The password string provided by the user.
        hashed_password: The hashed password stored in the database.

    Returns:
        True if the plain password matches the hashed password, False otherwise.
    """
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password: str) -> str:
    """
    Generates a hash for a given plain password.

    Args:
        password: The plain password string.

    Returns:
        The hashed password string.
    """
    return pwd_context.hash(password)

# Example Usage:
# plain_pw = "mysecretpassword"
# hashed_pw = get_password_hash(plain_pw)
# print(f"Hashed password: {hashed_pw}")
# print(f"Verification result: {verify_password(plain_pw, hashed_pw)}")
# print(f"Verification result with wrong password: {verify_password('wrongpassword', hashed_pw)}")
