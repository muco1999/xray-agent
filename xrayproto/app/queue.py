import json
import time
import uuid
from typing import Any, Dict

import redis
from fastapi import HTTPException

from .config import settings

QUEUE_KEY = "xray_jobs_queue"
JOB_KEY_PREFIX = "xray_job:"

r = redis.Redis.from_url(settings.redis_url, decode_responses=True)

def enqueue_job(kind: str, payload: Dict[str, Any]) -> str:
    job_id = str(uuid.uuid4())
    job = {"id": job_id, "kind": kind, "payload": payload, "ts": int(time.time())}
    r.set(JOB_KEY_PREFIX + job_id, json.dumps({"state": "queued", "job": job}))
    r.lpush(QUEUE_KEY, json.dumps(job))
    return job_id

def get_job(job_id: str) -> Dict[str, Any]:
    raw = r.get(JOB_KEY_PREFIX + job_id)
    if not raw:
        raise HTTPException(status_code=404, detail="job not found")
    return json.loads(raw)
