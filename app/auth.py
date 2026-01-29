from fastapi import Header, HTTPException
from app.config import settings


def require_token(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    prefix = "Bearer "
    if not authorization.startswith(prefix):
        raise HTTPException(status_code=401, detail="Invalid Authorization header format")

    token = authorization[len(prefix):].strip()
    if token != settings.api_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    return True
