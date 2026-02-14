from fastapi import Header, HTTPException
from app.config import settings
from app.models import IssueClientRequest
import json
import time
import uuid
import hashlib
from typing import Tuple



QUEUE_KEY = "xray:jobs:queue"
JOB_KEY_PREFIX = "xray:jobs:"
IDEMPOTENCY_PREFIX = "xray:idem:"


def require_token(authorization: str | None = Header(default=None)):
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    prefix = "Bearer "
    if not authorization.startswith(prefix):
        raise HTTPException(status_code=401, detail="Invalid Authorization header format")

    token = authorization[len(prefix):].strip()
    if token != settings.api_token:
        raise HTTPException(status_code=401, detail="Invalid token")

    return True

def _job_key(job_id: str) -> str:
    return f"{JOB_KEY_PREFIX}{job_id}"


def _idem_key(key: str) -> str:
    return f"{IDEMPOTENCY_PREFIX}{key}"


def set_job_state(r, job_id: str, state: str, *, result: dict | None = None, error: str | None = None):
    payload = {
        "id": job_id,
        "state": state,
        "ts": int(time.time()),
        "result": result,
        "error": error,
    }
    r.set(_job_key(job_id), json.dumps(payload), ex=3600)



def make_idempotency_key(req: IssueClientRequest) -> str:
    # один пользователь + inbound → одна задача
    base = f"{req.telegram_id.strip()}|{req.inbound_tag}"
    return hashlib.sha256(base.encode()).hexdigest()


def enqueue_issue_job(r, req: IssueClientRequest) -> Tuple[str, bool]:
    """
    Кладёт issue-client job в Redis.

    Returns:
      (job_id, deduped)
    """
    idem = make_idempotency_key(req)
    existing = r.get(_idem_key(idem))
    if existing:
        job_id = existing.decode() if isinstance(existing, (bytes, bytearray)) else existing
        return job_id, True

    job_id = str(uuid.uuid4())
    job = {
        "id": job_id,
        "type": "issue_client",
        "payload": req.model_dump(),
    }

    # сохраняем idempotency
    r.set(_idem_key(idem), job_id, ex=3600)

    # начальный статус
    set_job_state(r, job_id, "queued")

    # кладём в очередь
    r.lpush(QUEUE_KEY, json.dumps(job).encode("utf-8"))

    return job_id, False



