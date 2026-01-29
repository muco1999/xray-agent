"""
Redis queue implementation.

Идея:
- API кладёт job в Redis LIST (LPUSH)
- Worker ждёт job через BRPOP (blocking)
- Статус job хранится в ключе xray_job:<id> как JSON:
    queued -> running -> done|error
"""

import json
import time
import uuid
from typing import Any, Dict, Optional

from app.redis_client import r

QUEUE_KEY = "xray_jobs_queue"
JOB_KEY_PREFIX = "xray_job:"


def enqueue_job(kind: str, payload: Dict[str, Any]) -> str:
    """
    Положить задачу в очередь.
    Возвращает job_id.
    """
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "kind": kind, "payload": payload, "ts": int(time.time())}

    # начальный статус
    r.set(JOB_KEY_PREFIX + job_id, json.dumps({"state": "queued", "job": job}))
    # очередь
    r.lpush(QUEUE_KEY, json.dumps(job))
    return job_id


def set_job_state(job_id: str, state: str, result: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
    """Обновить состояние задачи."""
    obj = {"state": state, "result": result, "error": error, "ts": int(time.time())}
    r.set(JOB_KEY_PREFIX + job_id, json.dumps(obj))


def get_job(job_id: str) -> Dict[str, Any]:
    """Получить статус задачи."""
    raw = r.get(JOB_KEY_PREFIX + job_id)
    if not raw:
        return {"state": "not_found"}
    return json.loads(raw)
