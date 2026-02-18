from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Callable, Optional

from fastapi import Request
from starlette.responses import JSONResponse, Response

# redis asyncio client (у тебя уже есть app.redis_client.r)
from app.redis_client import r
from app.logger import log


@dataclass(frozen=True)
class RateLimit:
    # токены в секунду
    rate: float
    # максимальный burst (ведро)
    burst: int


@dataclass(frozen=True)
class RateRule:
    name: str
    limit: RateLimit


DEFAULT_RULES: dict[str, RateRule] = {
    "health": RateRule("health", RateLimit(rate=2.0, burst=5)),
    "status": RateRule("status", RateLimit(rate=10.0, burst=30)),
    "count":  RateRule("count",  RateLimit(rate=5.0, burst=15)),
    "emails": RateRule("emails", RateLimit(rate=1.0, burst=3)),
    "mutate": RateRule("mutate", RateLimit(rate=1.0, burst=3)),
}


# Lua token bucket (atomic)
# key: bucket state
# ARGV: now_ms, rate_per_ms, burst
# returns: allowed(0/1), retry_after_ms, remaining_tokens
LUA_TOKEN_BUCKET = r"""
local key = KEYS[1]
local now = tonumber(ARGV[1])
local rate = tonumber(ARGV[2])
local burst = tonumber(ARGV[3])

local data = redis.call("HMGET", key, "ts", "tokens")
local last_ts = tonumber(data[1])
local tokens = tonumber(data[2])

if last_ts == nil then
  last_ts = now
  tokens = burst
end

-- refill
local delta = now - last_ts
if delta < 0 then delta = 0 end
tokens = math.min(burst, tokens + (delta * rate))
last_ts = now

local allowed = 0
local retry_after = 0
if tokens >= 1.0 then
  allowed = 1
  tokens = tokens - 1.0
else
  allowed = 0
  retry_after = math.ceil((1.0 - tokens) / rate)
end

-- store
redis.call("HMSET", key, "ts", last_ts, "tokens", tokens)

-- expire: if idle, remove bucket
local ttl_ms = math.ceil((burst / rate) * 2)
redis.call("PEXPIRE", key, ttl_ms)

return {allowed, retry_after, tokens}
"""


def _client_ip(request: Request) -> str:
    # если ты за Nginx — добавь trust proxy и бери X-Forwarded-For аккуратно
    xff = request.headers.get("x-forwarded-for")
    if xff:
        return xff.split(",")[0].strip()
    if request.client:
        return request.client.host
    return "unknown"


def _token_fingerprint(request: Request) -> str:
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        # не храним токен целиком
        return "t:" + str(hash(auth[7:]))  # ok для keying, не для крипто
    return "anon"


def default_group_resolver(request: Request) -> str:
    """
    Маппинг пути -> группа лимитов.
    НЕ меняет логику эндпоинтов — только group.
    """
    p = request.url.path

    # health
    if p.startswith("/health"):
        return "health"

    # status
    if p.startswith("/xray/status"):
        return "status"

    # heavy inbound
    if "/inbounds/" in p and p.endswith("/users/count"):
        return "count"
    if "/inbounds/" in p and p.endswith("/emails"):
        return "emails"

    # mutate operations
    if p.startswith("/clients/issue") or p.startswith("/clients/") or p.startswith("/xray/restore") or p.startswith("/xray/add_user"):
        return "mutate"

    return "status"


async def _allow(request: Request, rule: RateRule, *, key_prefix: str = "rl") -> tuple[bool, int, float]:
    """
    Returns: (allowed, retry_after_ms, remaining_tokens)
    """
    ip = _client_ip(request)
    tf = _token_fingerprint(request)
    group = rule.name

    # key contains: prefix + group + token_fingerprint + ip
    key = f"{key_prefix}:{group}:{tf}:{ip}"

    now_ms = int(time.time() * 1000)
    rate_per_ms = rule.limit.rate / 1000.0

    try:
        res = await r.eval(
            LUA_TOKEN_BUCKET,
            1,  # numkeys
            key,  # KEYS[1]
            now_ms,  # ARGV[1]
            rate_per_ms,  # ARGV[2]
            rule.limit.burst  # ARGV[3]
        )

        allowed = int(res[0]) == 1
        retry_after_ms = int(res[1])
        remaining = float(res[2])

        return allowed, retry_after_ms, remaining
    except Exception as e:
        # fail-open по rate limit (лучше пропустить, чем убить API из-за Redis)
        log.error("rate_limit redis error: %r", e)
        return True, 0, 0.0


def rate_limit_middleware(
    rules: dict[str, RateRule] = DEFAULT_RULES,
    group_resolver: Callable[[Request], str] = default_group_resolver,
    whitelist_ips: Optional[set[str]] = None,
) -> Callable[[Request, Callable], Response]:
    whitelist_ips = whitelist_ips or set()

    async def middleware(request: Request, call_next: Callable):
        ip = _client_ip(request)
        if ip in whitelist_ips:
            return await call_next(request)

        group = group_resolver(request)
        rule = rules.get(group) or rules["status"]

        allowed, retry_ms, remaining = await _allow(request, rule)

        if not allowed:
            # 429
            headers = {
                "Retry-After": str(max(1, int(retry_ms / 1000))),
                "X-RateLimit-Group": group,
            }
            return JSONResponse(
                status_code=429,
                content={
                    "code": "RATE_LIMITED",
                    "message": "Too many requests",
                    "group": group,
                    "retry_after_ms": retry_ms,
                },
                headers=headers,
            )

        resp = await call_next(request)
        resp.headers["X-RateLimit-Group"] = group
        return resp

    return middleware
