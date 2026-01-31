from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Dict, Optional, Tuple

from app.redis_client import r

QUEUE_KEY = "xray_jobs_queue"
JOB_KEY_PREFIX = "xray_job:"
IDEMPOTENCY_PREFIX = "xray_idem:"

JOB_TTL_SEC = 3600
IDEMPOTENCY_TTL_SEC = 3600


def _job_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


def _idem_key(idem_hash: str) -> str:
    return f"{IDEMPOTENCY_PREFIX}{idem_hash}"


def _now() -> int:
    return int(time.time())

def _normalize_error(err: Any) -> Optional[str]:
    if err is None:
        return None
    if isinstance(err, str):
        return err
    try:
        return json.dumps(err, ensure_ascii=False, default=str)
    except Exception:
        return str(err)


async def set_job_state(
    job_id: str,
    state: str,
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[Any] = None,   # ← ВАЖНО
) -> None:
    doc = {
        "id": job_id,
        "state": state,
        "ts": _now(),
        "result": result,
        "error": _normalize_error(error),  # ← ВАЖНО
    }
    await r.set(
        _job_key(job_id),
        json.dumps(doc, ensure_ascii=False),
        ex=JOB_TTL_SEC,
    )



async def get_job_state(job_id: str) -> Dict[str, Any]:
    raw = await r.get(_job_key(job_id))
    if not raw:
        return {"id": job_id, "state": "not_found"}
    return json.loads(raw)


async def enqueue_job(kind: str, payload: Dict[str, Any]) -> str:
    """
    Non-idempotent enqueue.

    Гарантии:
      - job_state(queued) и enqueue в LIST делаются атомарно (pipeline)
    """
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "kind": kind, "payload": payload, "ts": _now()}

    state_doc = {"id": job_id, "state": "queued", "ts": _now(), "result": None, "error": None}

    async with r.pipeline(transaction=True) as pipe:
        pipe.set(_job_key(job_id), json.dumps(state_doc, ensure_ascii=False), ex=JOB_TTL_SEC)
        pipe.lpush(QUEUE_KEY, json.dumps(job, ensure_ascii=False))
        await pipe.execute()

    return job_id


def _make_issue_idempotency_hash(telegram_id: str, inbound_tag: str) -> str:
    base = f"{telegram_id.strip()}|{inbound_tag.strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


async def enqueue_issue_job(req_model_dump: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Idempotent enqueue for issue_client.

    Гарантии:
      - SET idem NX EX атомарно предотвращает гонки
      - если уже есть ключ => возвращаем существующий job_id
      - статус + enqueue делаем pipeline
    """
    telegram_id = str(req_model_dump["telegram_id"]).strip()
    inbound_tag = str(req_model_dump.get("inbound_tag") or "").strip() or "vless-in"

    idem_hash = _make_issue_idempotency_hash(telegram_id, inbound_tag)
    idem_key = _idem_key(idem_hash)

    job_id = str(uuid.uuid4())

    # 1) пробуем выставить idem указатель атомарно (NX)
    ok = await r.set(idem_key, job_id, ex=IDEMPOTENCY_TTL_SEC, nx=True)
    if not ok:
        # уже существует => вернуть существующий job_id
        existing = await r.get(idem_key)
        if existing:
            return str(existing), True
        # редкий случай: ключ исчез между операциями — продолжаем как новый

    job = {"id": job_id, "kind": "issue_client", "payload": req_model_dump, "ts": _now()}
    state_doc = {"id": job_id, "state": "queued", "ts": _now(), "result": None, "error": None}

    async with r.pipeline(transaction=True) as pipe:
        pipe.set(_job_key(job_id), json.dumps(state_doc, ensure_ascii=False), ex=JOB_TTL_SEC)
        pipe.lpush(QUEUE_KEY, json.dumps(job, ensure_ascii=False))
        await pipe.execute()

    return job_id, False
