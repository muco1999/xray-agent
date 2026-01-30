"""
Redis queue implementation for Xray Agent.

Design:
- API enqueues jobs into Redis LIST via LPUSH
- Worker consumes jobs via BRPOP (blocking pop)
- Job status is stored as JSON in Redis key: xray_job:<job_id>

Job envelope (single canonical format):
  {
    "id": "<uuid>",
    "kind": "issue_client" | "add_client" | "remove_client",
    "payload": {...},
    "ts": <unix_ts>
  }

Job status (stored under xray_job:<job_id>):
  {
    "id": "<uuid>",
    "state": "queued" | "running" | "done" | "error",
    "ts": <unix_ts>,
    "result": {...} | null,
    "error": "<string>" | null
  }

Idempotency (for issue_client):
- key is derived from (telegram_id, inbound_tag) -> sha256
- stored under xray_idem:<hash> -> job_id (TTL)
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from typing import Any, Dict, Optional, Tuple

from app.redis_client import r  # sync redis client

# Redis keys
QUEUE_KEY = "xray_jobs_queue"
JOB_KEY_PREFIX = "xray_job:"
IDEMPOTENCY_PREFIX = "xray_idem:"

# TTLs (seconds)
JOB_TTL_SEC = 3600        # keep job status for 1 hour
IDEMPOTENCY_TTL_SEC = 3600  # dedupe window


def _job_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


def _idem_key(idem_hash: str) -> str:
    return f"{IDEMPOTENCY_PREFIX}{idem_hash}"


def set_job_state(
    job_id: str,
    state: str,
    *,
    result: Optional[Dict[str, Any]] = None,
    error: Optional[str] = None,
) -> None:
    """
    Update job status document. Overwrites previous state.
    """
    doc = {
        "id": job_id,
        "state": state,
        "ts": int(time.time()),
        "result": result,
        "error": error,
    }
    r.set(_job_key(job_id), json.dumps(doc), ex=JOB_TTL_SEC)


def get_job_state(job_id: str) -> Dict[str, Any]:
    """
    Fetch job status document.

    Returns:
      - stored doc if present
      - {"id": job_id, "state": "not_found"} if missing
    """
    raw = r.get(_job_key(job_id))
    if not raw:
        return {"id": job_id, "state": "not_found"}
    return json.loads(raw)


def enqueue_job(kind: str, payload: Dict[str, Any]) -> str:
    """
    Enqueue generic job (non-idempotent).

    Used for:
      - remove_client
      - (optionally) add_client legacy

    Returns:
      job_id
    """
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "kind": kind, "payload": payload, "ts": int(time.time())}

    set_job_state(job_id, "queued")
    r.lpush(QUEUE_KEY, json.dumps(job).encode("utf-8"))
    return job_id


def _make_issue_idempotency_hash(telegram_id: str, inbound_tag: str) -> str:
    base = f"{telegram_id.strip()}|{inbound_tag.strip()}"
    return hashlib.sha256(base.encode("utf-8")).hexdigest()


def enqueue_issue_job(req_model_dump: Dict[str, Any]) -> Tuple[str, bool]:
    """
    Enqueue issue_client job with idempotency.

    Idempotency scope:
      - (telegram_id, inbound_tag) within IDEMPOTENCY_TTL_SEC

    Args:
      req_model_dump: dict from IssueClientRequest.model_dump()

    Returns:
      (job_id, deduped)
    """
    telegram_id = str(req_model_dump["telegram_id"]).strip()
    inbound_tag = str(req_model_dump.get("inbound_tag") or "").strip() or "vless-in"

    idem_hash = _make_issue_idempotency_hash(telegram_id, inbound_tag)
    existing = r.get(_idem_key(idem_hash))
    if existing:
        job_id = existing.decode() if isinstance(existing, (bytes, bytearray)) else existing
        return job_id, True

    job_id = str(uuid.uuid4())
    job = {"id": job_id, "kind": "issue_client", "payload": req_model_dump, "ts": int(time.time())}

    # store idempotency pointer
    r.set(_idem_key(idem_hash), job_id, ex=IDEMPOTENCY_TTL_SEC)

    # store initial status + enqueue
    set_job_state(job_id, "queued")
    r.lpush(QUEUE_KEY, json.dumps(job).encode("utf-8"))
    return job_id, False
