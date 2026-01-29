from typing import Optional
from fastapi import Header, HTTPException

from .config import settings

def require_token(authorization: Optional[str] = Header(default=None)) -> None:
    """
    Require: Authorization: Bearer <API_TOKEN>
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization scheme, use: Bearer <token>")

    token = parts[1].strip()
    if token != settings.api_token:
        raise HTTPException(status_code=403, detail="Invalid token")
