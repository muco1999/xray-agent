# disable_queue.py  (опционально)
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Optional, Union

from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from app.redis_client import r as redis_client


JsonStr = str
BytesOrStr = Union[bytes, str]


@dataclass(frozen=True, slots=True)
class DisableJob:
    inbound_tag: str
    xray_email: str
    reason: str
    devices: int
    limit: int
    created_at: int

    @staticmethod
    def make(*, inbound_tag: str, xray_email: str, reason: str, devices: int, limit: int) -> "DisableJob":
        return DisableJob(
            inbound_tag=inbound_tag,
            xray_email=xray_email,
            reason=reason,
            devices=devices,
            limit=limit,
            created_at=int(time.time()),
        )

    def to_json(self) -> JsonStr:
        return json.dumps(
            {
                "inbound_tag": self.inbound_tag,
                "xray_email": self.xray_email,
                "reason": self.reason,
                "devices": self.devices,
                "limit": self.limit,
                "created_at": self.created_at,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

    @staticmethod
    def from_json(raw: BytesOrStr) -> "DisableJob":
        s = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
        d = json.loads(s)
        return DisableJob(
            inbound_tag=str(d.get("inbound_tag") or ""),
            xray_email=str(d.get("xray_email") or ""),
            reason=str(d.get("reason") or ""),
            devices=int(d.get("devices") or 0),
            limit=int(d.get("limit") or 0),
            created_at=int(d.get("created_at") or 0),
        )


class DisableQueue:
    """
    Redis LIST queue: LPUSH + BRPOP.
    Если хочешь банить строго отдельным воркером — используй это.
    """

    def __init__(self, queue_key: str = "xray_guard:disable_queue"):
        self.queue_key = queue_key
        self._client = redis_client
        self._closed = False

    async def close(self) -> None:
        self._closed = True

    async def push(self, job: DisableJob) -> None:
        if self._closed:
            raise RuntimeError("DisableQueue is closed")
        try:
            await self._client.lpush(self.queue_key, job.to_json())
        except (RedisConnectionError, RedisTimeoutError):
            raise

    async def brpop(self, timeout: int = 5) -> Optional[DisableJob]:
        if self._closed:
            raise RuntimeError("DisableQueue is closed")
        try:
            item = await self._client.brpop([self.queue_key], timeout=int(timeout))
        except (RedisConnectionError, RedisTimeoutError):
            raise

        if not item:
            return None

        _key, payload = item
        return DisableJob.from_json(payload)