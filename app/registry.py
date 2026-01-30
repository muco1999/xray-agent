from __future__ import annotations
from typing import List, Optional
from app.redis_client import r

EMAILS_KEY = "xray:inbound:{tag}:emails"          # SET
UUID_BY_EMAIL_KEY = "xray:inbound:{tag}:uuid_by_email"  # HASH

def add_user(tag: str, email: str, uuid: str) -> None:
    r.sadd(EMAILS_KEY.format(tag=tag), email)
    r.hset(UUID_BY_EMAIL_KEY.format(tag=tag), email, uuid)

def remove_user(tag: str, email: str) -> None:
    r.srem(EMAILS_KEY.format(tag=tag), email)
    r.hdel(UUID_BY_EMAIL_KEY.format(tag=tag), email)

def list_emails(tag: str) -> List[str]:
    vals = r.smembers(EMAILS_KEY.format(tag=tag))
    out: List[str] = []
    for v in vals:
        out.append(v.decode() if isinstance(v, (bytes, bytearray)) else str(v))
    out.sort()
    return out

def count(tag: str) -> int:
    return int(r.scard(EMAILS_KEY.format(tag=tag)))

def get_uuid(tag: str, email: str) -> Optional[str]:
    v = r.hget(UUID_BY_EMAIL_KEY.format(tag=tag), email)
    if v is None:
        return None
    return v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
