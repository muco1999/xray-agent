# #/xray-agent/worker.py
from __future__ import annotations

import json
import signal
import traceback
import uuid
from typing import Any, Dict, Tuple, Optional

import httpx

from app.logger import log
from app.security.capacity import CapacityLimiter, CapacityPolicy
from app.settings import settings
import asyncio
import random
from contextlib import suppress

from app.redis_client import r
from app.queue import QUEUE_KEY, set_job_state, clear_issue_dedupe_cache
from app.xray import add_client, remove_client

# ✅ grpc.aio adapter (полностью async)
# ЛОГИКА НЕ МЕНЯЕТСЯ: меняется только способ вызова (раньше blocking->thread, теперь await)

# 🛡️ Capacity limiter (лимит ёмкости inbound)

cap_limiter = CapacityLimiter()
cap_policy = CapacityPolicy(
    limit=int(getattr(settings, 'capacity_limit_per_inbound', 50) or 50),
    ttl_sec=int(getattr(settings, 'capacity_limit_ttl_sec', 120) or 120),
)


# -----------------------------
# Capacity helpers
# -----------------------------
async def _reserve_capacity(inbound_tag: str) -> bool:
    ok = await cap_limiter.reserve(inbound_tag, cap_policy)
    if not ok:
        log.warning(
            "capacity exceeded inbound_tag=%s limit=%s",
            inbound_tag,
            cap_policy.limit,
        )
    return ok

# -----------------------------
# Link builder (как у тебя, но без обязательных утечек)
# -----------------------------
def build_vless_link(user_uuid: str, email: str, flow: str) -> str:
    missing = []
    if not settings.public_host:
        missing.append("PUBLIC_HOST")
    if not settings.reality_sni:
        missing.append("REALITY_SNI")
    if not settings.reality_pbk:
        missing.append("REALITY_PBK")
    if not settings.reality_sid:
        missing.append("REALITY_SID")
    if missing:
        raise RuntimeError(f"Missing env params: {', '.join(missing)}")

    port = int(getattr(settings, "public_port", 443) or 443)
    fp = getattr(settings, "reality_fp", "chrome") or "chrome"
    flow_q = f"&flow={flow}" if flow else "&flow=xtls-rprx-vision"

    return (
        f"vless://{user_uuid}@{settings.public_host}:{port}"
        f"?encryption=none"
        f"{flow_q}"
        f"&security=reality"
        f"&sni={settings.reality_sni}"
        f"&fp={fp}"
        f"&pbk={settings.reality_pbk}"
        f"&sid={settings.reality_sid}"
        f"&type=tcp"
        f"#VPN-{email}"
    )


# -----------------------------
# Notify (async + retries)
# -----------------------------
def _notify_config() -> Tuple[Optional[str], Optional[str], int, int]:
    notify_url = getattr(settings, "notify_url", None)
    notify_key = getattr(settings, "notify_api_key", None)
    timeout = int(getattr(settings, "notify_timeout_sec", 10))
    retries = int(getattr(settings, "notify_retries", 3))
    return notify_url, notify_key, timeout, retries


async def notify_external(payload: Dict[str, Any]) -> Dict[str, Any]:
    notify_url, notify_key, timeout, retries = _notify_config()
    if not notify_url:
        return {"skipped": True, "reason": "NOTIFY_URL not set"}

    headers = {"Content-Type": "application/json"}
    if notify_key:
        headers["X-API-Key"] = notify_key

    last_err: str | None = None
    async with httpx.AsyncClient(timeout=timeout) as client:
        for attempt in range(1, retries + 1):
            try:
                resp = await client.post(notify_url, json=payload, headers=headers)
                if 200 <= resp.status_code < 300:
                    return {"skipped": False, "status_code": resp.status_code}

                last_err = f"HTTP {resp.status_code}: {resp.text[:300]}"
            except Exception as e:
                last_err = f"{type(e).__name__}: {e}"

            await asyncio.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(f"notify failed after {retries} attempts: {last_err}")


# -----------------------------
# Job utils
# -----------------------------
def _parse_job(raw: Any) -> dict:
    # decode_responses=True => raw уже str
    if not isinstance(raw, str):
        raw = str(raw)
    return json.loads(raw)


def _require_field(obj: dict, key: str) -> Any:
    if key not in obj:
        raise ValueError(f"missing required field '{key}'")
    return obj[key]


def _safe_error(e: Exception) -> Dict[str, Any]:
    """
    Безопасная ошибка для сохранения в Redis (чтобы API не утекало всё подряд).
    Полный traceback сохраняем только если settings.debug=True
    """
    base = {
        "type": type(e).__name__,
        "message": str(e)[:500],
    }
    if bool(getattr(settings, "debug", False)):
        base["trace"] = traceback.format_exc()[:8000]
    return base


async def handle(job: dict) -> dict:
    kind = _require_field(job, "kind")
    payload = _require_field(job, "payload")

    # NOTE: ЛОГИКА 1-в-1 как раньше:
    # - add_client/remove_client/issue_client выполняются и раньше, и сейчас.
    # - отличие: теперь вызовы gRPC не блокируют event-loop (grpc.aio),
    #   поэтому _to_thread больше не нужен.

    if kind == "add_client":
        user_uuid = _require_field(payload, "uuid")
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")
        level = int(payload.get("level", 0))
        flow = payload.get("flow", "") or ""

        # 🛡️ capacity reserve (anti-bomb)
        if not await _reserve_capacity(inbound_tag):
            return {"error": "CAPACITY_EXCEEDED", "limit": cap_policy.limit, "inbound_tag": inbound_tag}

        try:
            # ✅ grpc.aio: await
            return await asyncio.wait_for(
                add_client(user_uuid, email, inbound_tag, level, flow),
                timeout=float(getattr(settings, "grpc_timeout_sec", 10)),
            )
        except Exception:
            # если не удалось добавить — освобождаем слот
            await cap_limiter.release(inbound_tag)
            raise

    if kind == "remove_client":
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")

        # ✅ grpc.aio: await
        res = await asyncio.wait_for(
            remove_client(email, inbound_tag),
            timeout=float(getattr(settings, "grpc_timeout_sec", 10)),
        )

        # ✅ очистка кеша
        try:
            n = await clear_issue_dedupe_cache(telegram_id=email, inbound_tag=inbound_tag)
            log.info(f"[CACHE] cleared issue dedupe keys={n} email={email} tag={inbound_tag}")
        except Exception as e:
            log.error(f"[CACHE] clear dedupe failed email={email} tag={inbound_tag} err={str(e)[:200]}")

        return {"removed": res, "cache_cleared": True}

    if kind == "issue_client":
        telegram_id = str(_require_field(payload, "telegram_id")).strip()
        inbound_tag = str(payload.get("inbound_tag") or settings.default_inbound_tag)
        level = int(payload.get("level", 0))
        flow = payload.get("flow")
        flow = (flow if flow is not None else settings.default_flow) or ""

        user_uuid = str(uuid.uuid4())

        # 🛡️ capacity reserve (anti-bomb)
        if not await _reserve_capacity(inbound_tag):
            return {"error": "CAPACITY_EXCEEDED", "limit": cap_policy.limit, "inbound_tag": inbound_tag}

        # 1) gRPC add user (async grpc.aio)
        try:
            await asyncio.wait_for(
                add_client(user_uuid, telegram_id, inbound_tag, level, flow),
            timeout=float(getattr(settings, "grpc_timeout_sec", 10)),
            )
        except Exception:
            await cap_limiter.release(inbound_tag)
            raise

        # 2) build link (fast)
        link = build_vless_link(user_uuid, telegram_id, flow)

        issued = {"uuid": user_uuid, "email": telegram_id, "inbound_tag": inbound_tag, "link": link}

        # 3) notify (async)
        try:
            notify_info = await asyncio.wait_for(
                notify_external(issued),
                timeout=float(getattr(settings, "notify_total_timeout_sec", 20)),
            )
        except Exception as e:
            notify_info = {"skipped": True, "reason": f"notify_failed: {type(e).__name__}: {str(e)[:200]}"}

        return {"issued": issued, "notify": notify_info}

    raise RuntimeError(f"unknown job kind={kind}")


# -----------------------------
# Main loop
# -----------------------------
class GracefulExit:
    def __init__(self):
        self._event = asyncio.Event()

    def request_stop(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        await self._event.wait()

    def is_stopping(self) -> bool:
        return self._event.is_set()



# Подбери исключения под твою версию redis-py:
try:
    from redis.exceptions import ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError
except Exception:  # pragma: no cover
    RedisConnectionError = TimeoutError  # type: ignore
    RedisTimeoutError = TimeoutError  # type: ignore


async def _safe_set_job_state(job_id: str, state: str, **kwargs) -> None:
    """
    Никогда не даём падать воркеру из-за проблем записи статуса в Redis.
    """
    try:
        # ограничим время на запись статуса, чтобы не залипнуть на сетевом флапе
        await asyncio.wait_for(set_job_state(job_id, state, **kwargs), timeout=3)
    except asyncio.CancelledError:
        # при остановке воркера — даём корректно завершиться
        raise
    except Exception as e:
        log.error("set_job_state failed id=%s state=%s err=%r", job_id, state, e)


async def worker_loop():
    log.info("worker started queue=%s", QUEUE_KEY)
    stopper = GracefulExit()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopper.request_stop)
        except NotImplementedError:
            pass

    # backoff для проблем с Redis: растёт до 5с, потом сбрасывается при успехе
    backoff = 0.1
    backoff_max = 5.0

    while True:
        if stopper.is_stopping():
            log.info("worker stopping gracefully...")
            break

        # --- 1) максимально безопасное чтение из очереди ---
        try:
            # brpop может зависнуть при сетевом флапе — ограничим внешним таймаутом
            item = await asyncio.wait_for(
                r.brpop([QUEUE_KEY], timeout=1),
                timeout=3,
            )

            backoff = 0.1  # успех -> сбрасываем backoff
        except asyncio.TimeoutError:
            # либо наш wait_for, либо внутренний таймаут: это нормальный "idle"
            continue
        except asyncio.CancelledError:
            log.warning("worker cancelled -> stopping")
            break
        except (RedisConnectionError, RedisTimeoutError, OSError, ConnectionError) as e:
            log.error("redis brpop/connect failed err=%r; backoff=%.2fs", e, backoff)
            await asyncio.sleep(backoff + random.uniform(0, backoff * 0.2))
            backoff = min(backoff * 2, backoff_max)
            continue
        except Exception as e:
            # любой неожиданный кейс: логируем и не падаем
            log.exception("unexpected error in brpop err=%r", e)
            await asyncio.sleep(0.5)
            continue

        if item is None:
            continue

        _, raw = item

        try:
            job = _parse_job(raw)
        except Exception as e:
            log.error("invalid job payload err=%s raw=%r", e, raw)
            continue

        job_id = job.get("id")
        if not job_id:
            log.error("job without id: %r", job)
            continue

        # --- 2) запись состояния: никогда не должна валить воркер ---
        await _safe_set_job_state(job_id, "running")
        log.info("job running id=%s kind=%s", job_id, job.get("kind"))

        try:
            # --- 3) handle: оставляем как есть, но ловим CancelledError отдельно ---
            res = await handle(job)
            if not isinstance(res, dict):
                res = {"raw": res}

            await _safe_set_job_state(job_id, "done", result=res)
            log.info("job done id=%s", job_id)

        except asyncio.CancelledError:
            # при остановке — попробуем пометить, но не обязаны успеть
            with suppress(Exception):
                await _safe_set_job_state(job_id, "error", error={"type": "CancelledError", "msg": "worker stopping"})
            log.warning("job cancelled id=%s -> stopping worker", job_id)
            break

        except Exception as e:
            err_doc = _safe_error(e)
            await _safe_set_job_state(job_id, "error", error=err_doc)
            log.error("job error id=%s err=%s", job_id, err_doc)


def main():
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()