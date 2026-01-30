from __future__ import annotations

from typing import List, Optional
from app.redis_client import r

EMAILS_KEY = "xray:inbound:{tag}:emails"                 # SET
UUID_BY_EMAIL_KEY = "xray:inbound:{tag}:uuid_by_email"   # HASH


async def add_user(tag: str, email: str, uuid: str) -> None:
    key_emails = EMAILS_KEY.format(tag=tag)
    key_map = UUID_BY_EMAIL_KEY.format(tag=tag)

    # атомарно
    async with r.pipeline(transaction=True) as pipe:
        pipe.sadd(key_emails, email)
        pipe.hset(key_map, email, uuid)
        await pipe.execute()


async def remove_user(tag: str, email: str) -> None:
    key_emails = EMAILS_KEY.format(tag=tag)
    key_map = UUID_BY_EMAIL_KEY.format(tag=tag)

    async with r.pipeline(transaction=True) as pipe:
        pipe.srem(key_emails, email)
        pipe.hdel(key_map, email)
        await pipe.execute()


async def list_emails(tag: str) -> List[str]:
    vals = await r.smembers(EMAILS_KEY.format(tag=tag))
    out = sorted(vals)  # vals уже list[str] / set[str] при decode_responses=True
    return out


async def count(tag: str) -> int:
    return int(await r.scard(EMAILS_KEY.format(tag=tag)))


async def get_uuid(tag: str, email: str) -> Optional[str]:
    v = await r.hget(UUID_BY_EMAIL_KEY.format(tag=tag), email)
    return v  # либо str, либо None
