import json
import time

import redis

from app.config import settings
from app.queue import JOB_KEY_PREFIX, QUEUE_KEY
from app.xray import add_client, remove_client

r = redis.Redis.from_url(settings.redis_url, decode_responses=True)

def set_job(job_id: str, data: dict):
    r.set(JOB_KEY_PREFIX + job_id, json.dumps(data))

def main():
    while True:
        item = r.brpop(QUEUE_KEY, timeout=5)
        if not item:
            continue

        _, raw = item
        job = json.loads(raw)
        job_id = job["id"]
        kind = job["kind"]
        payload = job["payload"]

        set_job(job_id, {"state": "running", "job": job})

        try:
            if kind == "add_client":
                result = add_client(
                    uuid=payload["uuid"],
                    email=payload["email"],
                    inbound_tag=payload["inbound_tag"],
                    level=payload.get("level", 0),
                    flow=payload.get("flow", ""),
                )

            elif kind == "remove_client":
                result = remove_client(email=payload["email"], inbound_tag=payload["inbound_tag"])

            else:
                raise RuntimeError(f"Unknown job kind: {kind}")

            set_job(job_id, {"state": "done", "result": result, "job": job})

        except Exception as e:
            set_job(job_id, {"state": "error", "error": str(e), "job": job})
            time.sleep(0.2)

if __name__ == "__main__":
    main()
