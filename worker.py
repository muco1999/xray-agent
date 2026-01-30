from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
import traceback
import uuid
from typing import Any, Dict, Tuple, Optional

import httpx

from app.config import settings
from app.redis_client import r
from app.queue import QUEUE_KEY, set_job_state
from app.xray import add_client, remove_client

log = logging.getLogger("xray-agent-worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


def _now() -> int:
    return int(time.time())


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
    flow_q = f"&flow={flow}" if flow else ""

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


# -----------------------------
# Blocking gRPC calls -> thread
# -----------------------------
async def _to_thread(fn, *args, **kwargs):
    return await asyncio.to_thread(fn, *args, **kwargs)


async def handle(job: dict) -> dict:
    kind = _require_field(job, "kind")
    payload = _require_field(job, "payload")

    if kind == "add_client":
        user_uuid = _require_field(payload, "uuid")
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")
        level = int(payload.get("level", 0))
        flow = payload.get("flow", "") or ""
        return await _to_thread(add_client, user_uuid, email, inbound_tag, level, flow)

    if kind == "remove_client":
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")
        return await _to_thread(remove_client, email, inbound_tag)

    if kind == "issue_client":
        telegram_id = str(_require_field(payload, "telegram_id")).strip()
        inbound_tag = str(payload.get("inbound_tag") or settings.default_inbound_tag)
        level = int(payload.get("level", 0))
        flow = payload.get("flow")
        flow = (flow if flow is not None else settings.default_flow) or ""

        user_uuid = str(uuid.uuid4())

        # 1) gRPC add user (blocking -> thread)
        await _to_thread(add_client, user_uuid, telegram_id, inbound_tag, level, flow)

        # 2) build link (fast)
        link = build_vless_link(user_uuid, telegram_id, flow)

        issued = {"uuid": user_uuid, "email": telegram_id, "inbound_tag": inbound_tag, "link": link}

        # 3) notify (async)
        notify_info = await notify_external(issued)

        return {"issued": issued, "notify": notify_info}

    raise RuntimeError(f"unknown job kind={kind}")


# -----------------------------
# Main loop
# -----------------------------
class GracefulExit:
    def __init__(self):
        self._stop = asyncio.Event()

    def request_stop(self):
        self._stop.set()

    async def wait(self):
        await self._stop.wait()


async def worker_loop():
    log.info("worker started queue=%s", QUEUE_KEY)
    stopper = GracefulExit()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopper.request_stop)
        except NotImplementedError:
            # windows fallback
            pass

    while True:
        if stopper._stop.is_set():
            log.info("worker stopping gracefully...")
            break

        item = await r.brpop(QUEUE_KEY, timeout=1)
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

        await set_job_state(job_id, "running")
        log.info("job running id=%s kind=%s", job_id, job.get("kind"))

        try:
            res = await handle(job)
            if not isinstance(res, dict):
                res = {"raw": res}
            await set_job_state(job_id, "done", result=res)
            log.info("job done id=%s", job_id)
        except Exception as e:
            err_doc = _safe_error(e)
            await set_job_state(job_id, "error", error=err_doc)
            log.error("job error id=%s err=%s", job_id, err_doc)


def main():
    asyncio.run(worker_loop())


if __name__ == "__main__":
    main()
