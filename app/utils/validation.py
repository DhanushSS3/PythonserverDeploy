from fastapi import HTTPException
from typing import Optional
from app.core.security import decode_token

def enforce_service_user_id_restriction(request_user_id: Optional[int], token: str):
    payload = decode_token(token)
    if request_user_id is not None and payload.get("sub") != "service":
        raise HTTPException(status_code=403, detail="Only service accounts may specify user_id.")
