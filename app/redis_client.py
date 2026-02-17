from __future__ import annotations

import redis.asyncio as redis

from app.settings import settings


# decode_responses=True => Redis возвращает str, а не bytes
r: redis.Redis = redis.Redis.from_url(
    settings.redis_url,
    decode_responses=True,
    health_check_interval=30,
    socket_connect_timeout=5,
    socket_timeout=10,
    retry_on_timeout=True,
)
