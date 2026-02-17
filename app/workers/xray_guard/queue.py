# queue.py
from __future__ import annotations

from dataclasses import dataclass

from app.redis_client import r as redis


@dataclass(frozen=True)
class DedupKeys:
    warn: str
    ban: str
    thanks: str
    warned_at: str


class GuardRedis:
    """
    Общий redis client НЕ закрываем (не ломаем другие части агента).
    Держим:
      - warned_at (timestamp)
      - allow_once locks: warn/ban/thanks
    """

    def __init__(self):
        self.r = redis

    async def close(self) -> None:
        # общий клиент не закрываем
        return None

    async def allow_once(self, key: str, ttl_sec: int) -> bool:
        return bool(await self.r.set(name=key, value="1", nx=True, ex=int(ttl_sec)))

    @staticmethod
    def keys(inbound_tag: str, email: str) -> DedupKeys:
        base = f"xray_guard:{inbound_tag}:{email}"
        return DedupKeys(
            warn=f"{base}:once:warn",
            ban=f"{base}:once:ban",
            thanks=f"{base}:once:thanks",
            warned_at=f"{base}:warned_at",
        )

    async def get(self, key: str) -> str | None:
        v = await self.r.get(key)
        if v is None:
            return None
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", errors="replace")
        return str(v)

    async def setex(self, key: str, ttl_sec: int, value: str) -> None:
        await self.r.set(name=key, value=value, ex=int(ttl_sec))

    async def delete(self, key: str) -> None:
        await self.r.delete(key)