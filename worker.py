"""
Worker process for Xray Agent.

Запускается отдельным процессом/контейнером.
Читает Redis очередь (BRPOP) и выполняет задачи:

- add_client (legacy)
- remove_client (legacy)
- issue_client:
    * генерирует UUID
    * добавляет пользователя в Xray (AlterInbound AddUserOperation)
    * генерирует vless:// ссылку из env-параметров
    * опционально отправляет payload на notify-сервер (/v1/notify)

Результат/ошибка пишутся в Redis job state (для GET /jobs/{job_id}).
"""

from __future__ import annotations

import json
import traceback
import uuid
from typing import Any, Dict

import requests

from app.config import settings
from app.redis_client import r
from app.queue import QUEUE_KEY, set_job_state
from app.xray import add_client, remove_client


def build_vless_link(user_uuid: str, email: str, flow: str) -> str:
    """
    Build vless:// link from server-side .env settings.

    Required env:
      PUBLIC_HOST, PUBLIC_PORT
      REALITY_SNI, REALITY_FP, REALITY_PBK, REALITY_SID
      DEFAULT_FLOW (optional)
    """
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
        raise RuntimeError(f"Missing required env settings: {', '.join(missing)}")

    return (
        f"vless://{user_uuid}@{settings.public_host}:{settings.public_port}"
        f"?encryption=none"
        f"&flow={flow}"
        f"&security=reality"
        f"&sni={settings.reality_sni}"
        f"&fp={settings.reality_fp}"
        f"&pbk={settings.reality_pbk}"
        f"&sid={settings.reality_sid}"
        f"&type=tcp"
        f"#VPN-{email}"
    )


def notify_external(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Send payload to external notify service (optional).

    Controlled by env:
      NOTIFY_URL
      NOTIFY_API_KEY   (sent as X-API-Key)
      NOTIFY_TIMEOUT_SEC
      NOTIFY_RETRIES
    """
    notify_url = getattr(settings, "notify_url", None)
    notify_key = getattr(settings, "notify_api_key", None)
    if not notify_url:
        # notify is optional
        return {"skipped": True, "reason": "NOTIFY_URL not set"}

    timeout = int(getattr(settings, "notify_timeout_sec", 10))
    retries = int(getattr(settings, "notify_retries", 3))

    headers = {"Content-Type": "application/json"}
    if notify_key:
        headers["X-API-Key"] = notify_key

    last_err: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(notify_url, json=payload, headers=headers, timeout=timeout)
            if 200 <= resp.status_code < 300:
                return {"skipped": False, "status_code": resp.status_code}
            last_err = RuntimeError(f"notify failed: {resp.status_code} {resp.text[:500]}")
        except Exception as e:
            last_err = e

    raise RuntimeError(f"notify failed after retries: {last_err}")


def handle(job: dict) -> dict:
    """
    Execute single job.

    Job envelope (единый формат):
      {
        "id": "<job_id>",
        "kind": "issue_client" | "add_client" | "remove_client",
        "payload": {...}
      }
    """
    kind = job["kind"]
    p = job["payload"]

    if kind == "add_client":
        # legacy job
        return add_client(p["uuid"], p["email"], p["inbound_tag"], p.get("level", 0), p.get("flow", ""))

    if kind == "remove_client":
        # legacy job
        return remove_client(p["email"], p["inbound_tag"])

    if kind == "issue_client":
        # new job: server generates uuid
        telegram_id = str(p["telegram_id"]).strip()
        inbound_tag = p.get("inbound_tag") or settings.default_inbound_tag
        level = int(p.get("level", 0))
        flow = p.get("flow") or settings.default_flow

        user_uuid = str(uuid.uuid4())

        # Add to Xray
        add_client(user_uuid, telegram_id, inbound_tag, level, flow)

        # Build link
        link = build_vless_link(user_uuid, telegram_id, flow)

        result_payload = {
            "uuid": user_uuid,
            "email": telegram_id,
            "inbound_tag": inbound_tag,
            "link": link,
        }

        # Notify (optional)
        notify_info = notify_external(result_payload)

        return {"issued": result_payload, "notify": notify_info}

    raise RuntimeError(f"Unknown job kind: {kind}")


def main():
    while True:
        item = r.brpop(QUEUE_KEY, timeout=1)
        if item is None:
            continue

        _, raw = item

        try:
            if isinstance(raw, (bytes, bytearray)):
                raw = raw.decode("utf-8", errors="replace")
            job = json.loads(raw)
        except Exception:
            # bad payload - can't recover
            # IMPORTANT: we don't crash worker
            print("bad job payload:", repr(raw))
            continue

        job_id = job.get("id")
        if not job_id:
            print("job without id:", job)
            continue

        set_job_state(job_id, "running")

        try:
            res = handle(job)
            set_job_state(job_id, "done", result=res if isinstance(res, dict) else {"raw": res})
        except Exception as e:
            set_job_state(job_id, "error", error=str(e) + "\n" + traceback.format_exc())


if __name__ == "__main__":
    main()
