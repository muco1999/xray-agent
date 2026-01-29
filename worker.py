"""
Worker process.

Запускается отдельным контейнером.
Слушает Redis очередь и выполняет задачи:
- add_client
- remove_client
"""

import json
import traceback

from app.redis_client import r
from app.queue import QUEUE_KEY, set_job_state
from app.xray import add_client, remove_client


def handle(job: dict):
    """
    Выполнить одну задачу.
    job.kind определяет действие.
    """
    kind = job["kind"]
    p = job["payload"]

    if kind == "add_client":
        return add_client(p["uuid"], p["email"], p["inbound_tag"], p.get("level", 0), p.get("flow", ""))
    if kind == "remove_client":
        return remove_client(p["email"], p["inbound_tag"])

    raise RuntimeError(f"Unknown job kind: {kind}")


def main():
    while True:
        # BRPOP блокируется пока не появится job
        _, raw = r.brpop(QUEUE_KEY, timeout=0)
        job = json.loads(raw)

        job_id = job["id"]
        set_job_state(job_id, "running")

        try:
            res = handle(job)
            set_job_state(job_id, "done", result=res if isinstance(res, dict) else {"raw": res})
        except Exception as e:
            set_job_state(job_id, "error", error=str(e) + "\n" + traceback.format_exc())


if __name__ == "__main__":
    main()
