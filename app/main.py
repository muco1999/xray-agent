"""
FastAPI entrypoint for Xray Agent API.

Назначение:
- Управление Xray через gRPC API (через grpcurl внутри add/remove функций)
- Health/status
- Просмотр inbound (count/emails)
- Async jobs через Redis:
    - remove_client (опционально)
    - issue_client: генерировать UUID + add_client + build link + notify (через worker)

Auth:
- Header: Authorization: Bearer <API_TOKEN>
"""

from __future__ import annotations

from fastapi import FastAPI, Query, Depends, HTTPException
from starlette import status
from starlette.responses import JSONResponse
from app.auth import require_token
from app.config import settings
from app.models import (
    JobEnqueueResponse,
    IssueClientRequest,
    JobStatusResponse,
)
from app.redis_client import r  # sync redis client
from app.queue import (
    enqueue_job,           # legacy: add/remove jobs
    enqueue_issue_job,     # issue_client job
    get_job_state,         # job polling
)
from app.xray import (
    xray_runtime_status,
    remove_client,
    inbound_users_count,
    inbound_emails,
)

app = FastAPI(title="Xray Agent API", version="1.0.0")


@app.get("/health/full", dependencies=[Depends(require_token)])
def health_full():
    """
    Full health:
    - Xray API gRPC reachable
    - grpcurl present
    - sys stats query (if implemented in xray_runtime_status)
    """
    st = xray_runtime_status()
    return {"time": st.get("time"), "xray": st, "ok": bool(st.get("ok"))}


@app.get("/xray/status", dependencies=[Depends(require_token)])
def xray_status():
    """
    Quick status for Xray API.
    """
    return xray_runtime_status()


@app.get("/inbounds/{tag}/users/count", dependencies=[Depends(require_token)])
def api_inbound_users_count(tag: str):
    """Inbound user count (runtime state)."""
    return {"result": inbound_users_count(tag)}


@app.get("/inbounds/{tag}/emails", dependencies=[Depends(require_token)])
def api_inbound_emails(tag: str):
    """Inbound emails list (runtime state)."""
    return {"result": inbound_emails(tag)}


@app.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    dependencies=[Depends(require_token)],
    summary="Get job status/result",
)
def api_job_get(job_id: str):
    """
    Poll async job status.

    states:
      - queued
      - running
      - done (result contains payload)
      - error (error contains traceback)
    """
    st = get_job_state(job_id)
    if st["state"] == "not_found":
        raise HTTPException(status_code=404, detail="job not found")
    return st


@app.delete("/clients/{email}", dependencies=[Depends(require_token)])
def api_remove_client(
    email: str,
    inbound_tag: str = settings.default_inbound_tag,
    async_: bool = Query(False, alias="async"),
):
    """
    Remove client by email (telegram_id) from inbound.

    Sync:
      DELETE /clients/{email}?inbound_tag=vless-in

    Async:
      DELETE /clients/{email}?inbound_tag=vless-in&async=true
      -> enqueue job remove_client
    """
    if async_:
        job_id = enqueue_job("remove_client", {"email": email, "inbound_tag": inbound_tag})
        return {"job_id": job_id}

    try:
        return {"result": remove_client(email=email, inbound_tag=inbound_tag)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post(
    "/clients/issue",
    response_model=JobEnqueueResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_token)],
    summary="Issue new client (async): worker generates UUID, adds to Xray, builds link, and notifies external service.",
)
def api_issue_client(req: IssueClientRequest, async_: bool = Query(True, alias="async")):
    """
    Async by default.

    Flow:
      - API enqueues issue_client job
      - worker:
          * generates UUID
          * add_client(uuid, telegram_id, inbound_tag, level, flow)
          * builds vless:// link from .env (PUBLIC_HOST/REALITY_*)
          * (optional) POSTs payload to notify-server /v1/notify
      - client polls GET /jobs/{job_id}
    """
    if not async_:
        return JSONResponse(
            status_code=400,
            content={"detail": "sync mode disabled; use /clients/issue?async=true"},
        )


    job_id, deduped = enqueue_issue_job(req.model_dump())

    return JSONResponse(
        status_code=status.HTTP_202_ACCEPTED,
        content=JobEnqueueResponse(job_id=job_id, deduped=deduped).model_dump(),
    )
