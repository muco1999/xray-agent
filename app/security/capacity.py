from __future__ import annotations

"""
Capacity limiter (лимит ёмкости) для защиты от "бомбы" выдачи новых клиентов.

Зачем:
- Простая проверка `if count >= 50` даёт гонку под параллельной нагрузкой.
- Решение: атомарный reserve slot в Redis через Lua.

Как использовать:
- Перед add_client/issue_client: reserve(inbound_tag)
  - если False -> CAPACITY_EXCEEDED
- Если add_client упал -> release(inbound_tag)
- При remove_client (успешном) -> release(inbound_tag)

TTL:
- safety TTL (по умолчанию 120с) защищает от утечек слотов, если воркер упал в середине.
"""

from dataclasses import dataclass

from app.redis_client import r
from app.logger import log


CAPACITY_RESERVE_LUA = r"""
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])

local cur = redis.call("GET", key)
if cur == false then
  cur = 0
else
  cur = tonumber(cur)
end

if cur >= limit then
  return {0, cur}  -- denied
end

cur = redis.call("INCR", key)
redis.call("EXPIRE", key, ttl)

return {1, cur}  -- ok + new value
"""

CAPACITY_RELEASE_LUA = r"""
local key = KEYS[1]
local cur = redis.call("GET", key)
if cur == false then
  return 0
end
cur = tonumber(cur)
if cur <= 0 then
  redis.call("DEL", key)
  return 0
end
cur = redis.call("DECR", key)
if cur <= 0 then
  redis.call("DEL", key)
end
return cur
"""


@dataclass(frozen=True)
class CapacityPolicy:
    limit: int = 50
    ttl_sec: int = 120


class CapacityLimiter:
    def __init__(self, prefix: str = "cap"):
        self.prefix = prefix

    def key(self, inbound_tag: str) -> str:
        return f"{self.prefix}:{inbound_tag}"

    async def reserve(self, inbound_tag: str, policy: CapacityPolicy) -> bool:
        key = self.key(inbound_tag)
        try:
            ok, _cur = await r.eval(
                CAPACITY_RESERVE_LUA,
                1,
                key,
                str(policy.limit),
                str(policy.ttl_sec),
            )

            return int(ok) == 1
        except Exception as e:
            # fail-closed: безопаснее под DDoS
            log.error("capacity reserve redis error: %r", e)
            return False

    async def release(self, inbound_tag: str) -> None:
        key = self.key(inbound_tag)
        try:
            await r.eval(CAPACITY_RELEASE_LUA, 1, key)
        except Exception as e:
            log.error("capacity release redis error: %r", e)