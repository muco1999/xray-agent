"""
FastAPI entrypoint for Xray Agent API (production async).

Features:
- async endpoints
- sync/blocking upstream calls (grpcio) executed via threadpool
- async Redis queue/status (enqueue + polling)
- normalized error responses (no sensitive leaks)
- request-id middleware (X-Request-ID)
- /health/full returns 503 when Xray is down

Auth:
- Header: Authorization: Bearer <API_TOKEN>
"""

from __future__ import annotations

from typing import Optional

from fastapi import Query, HTTPException, Request
from fastapi.concurrency import run_in_threadpool
from starlette import status

from app.andpoints.logger import log
from app.andpoints.tools import api_error, _safe_upstream_detail
from app.auth import require_token
from app.config import settings
from app.models import JobEnqueueResponse, IssueClientRequest, JobStatusResponse

# async queue
from app.queue import enqueue_job, enqueue_issue_job, get_job_state, clear_issue_dedupe_cache
from fastapi import APIRouter, Depends
# sync grpcio adapter
from app.xray import xray_runtime_status, remove_client, inbound_users_count, inbound_emails





router = APIRouter(tags=["xray-journald"])

# ----------------------------
# App + Middleware
# ----------------------------

# ----------------------------
# Routes
# ----------------------------

@router.get("/health/full", dependencies=[Depends(require_token)])
async def health_full(request: Request):
    """
    Full check:
    - TCP port open
    - gRPC GetSysStats

    Returns:
      - 200 if ok
      - 503 if xray not healthy
    """
    st = await run_in_threadpool(xray_runtime_status)
    ok = bool(st.get("ok"))

    if not ok:
        # 503 is important for infra health checks
        # Do not leak too much here; provide structured info.
        return api_error(
            request=request,
            http_status=503,
            code="XRAY_UNAVAILABLE",
            message="xray is not healthy",
            details={
                "xray_api_addr": st.get("xray_api_addr"),
                "xray_api_port_open": st.get("xray_api_port_open"),
            },
        )

    return {"ok": True, "time": st.get("time"), "xray": st, "request_id": request.state.request_id}


@router.get("/xray/status", dependencies=[Depends(require_token)])
async def xray_status(request: Request):
    """Fast status (still upstream). Always 200; /health/full handles 503 semantics."""
    st = await run_in_threadpool(xray_runtime_status)
    st["request_id"] = request.state.request_id
    return st


@router.get("/inbounds/{tag}/users/count", dependencies=[Depends(require_token)])
async def api_inbound_users_count(request: Request, tag: str):
    """Inbound users count (runtime state)."""
    try:
        # IMPORTANT: make inbound_users_count return int in app/xray.py
        result = await run_in_threadpool(inbound_users_count, tag)
        return {"result": result, "request_id": request.state.request_id}
    except Exception as e:
        log.exception("inbound_users_count failed", extra={"tag": tag, "request_id": request.state.request_id})
        return api_error(request, 502, "UPSTREAM_ERROR", "upstream service error", _safe_upstream_detail(e))


@router.get("/inbounds/{tag}/emails", dependencies=[Depends(require_token)])
async def api_inbound_emails(request: Request, tag: str):
    """Inbound user emails (runtime state)."""
    try:
        result = await run_in_threadpool(inbound_emails, tag)
        return {"result": result, "request_id": request.state.request_id}
    except Exception as e:
        log.exception("inbound_emails failed", extra={"tag": tag, "request_id": request.state.request_id})
        return api_error(request, 502, "UPSTREAM_ERROR", "upstream service error", _safe_upstream_detail(e))


@router.get(
    "/jobs/{job_id}",
    response_model=JobStatusResponse,
    dependencies=[Depends(require_token)],
    summary="Get job status/result",
)
async def api_job_get(request: Request, job_id: str):
    """
    Poll async job status.

    states:
      - queued
      - running
      - done
      - error
    """
    try:
        st = await get_job_state(job_id)
    except Exception as e:
        log.exception("get_job_state failed", extra={"job_id": job_id, "request_id": request.state.request_id})
        return api_error(request, 502, "REDIS_ERROR", "queue backend error", _safe_upstream_detail(e))

    if st.get("state") == "not_found":
        raise HTTPException(status_code=404, detail={"code": "JOB_NOT_FOUND", "message": "job not found", "job_id": job_id})

    # если хочешь request_id в модели — добавь поле в JobStatusResponse
    return JobStatusResponse(**st)


@router.delete("/clients/{email}", dependencies=[Depends(require_token)])
async def api_remove_client(
    request: Request,
    email: str,
    inbound_tag: Optional[str] = Query(default=None),
    async_: bool = Query(False, alias="async"),
):
    """
    Remove client by email from inbound.

    Sync:
      DELETE /clients/{email}?inbound_tag=vless-in

    Async:
      DELETE /clients/{email}?inbound_tag=vless-in&async=true
      -> enqueue remove_client job
    """
    inbound_tag = inbound_tag or settings.default_inbound_tag

    if async_:
        try:
            job_id = await enqueue_job("remove_client", {"email": email, "inbound_tag": inbound_tag})
            return {"job_id": job_id, "request_id": request.state.request_id}
        except Exception as e:
            log.exception("enqueue remove_client failed", extra={"request_id": request.state.request_id})
            return api_error(request, 502, "REDIS_ERROR", "queue backend error", _safe_upstream_detail(e))

    # sync path (still blocking grpcio)
    try:
        result = await run_in_threadpool(lambda: remove_client(email=email, inbound_tag=inbound_tag))

        # ✅ очищаем dedupe после удаления
        try:
            n = await clear_issue_dedupe_cache(telegram_id=email, inbound_tag=inbound_tag)
            log.info(f"[CACHE] cleared issue dedupe keys={n} email={email} tag={inbound_tag}")
        except Exception as e:
            log.error(f"[CACHE] clear dedupe failed email={email} tag={inbound_tag} err={str(e)[:200]}")

        return {"result": result, "request_id": request.state.request_id}

    except Exception as e:
        log.exception("remove_client failed", extra={"email": email, "tag": inbound_tag, "request_id": request.state.request_id})
        return api_error(request, 502, "UPSTREAM_ERROR", "upstream service error", _safe_upstream_detail(e))


@router.post(
    "/clients/issue",
    response_model=JobEnqueueResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_token)],
    summary="Issue new client (async): worker generates UUID, adds to Xray, builds link, optional notify.",
)
async def api_issue_client(request: Request, req: IssueClientRequest, async_: bool = Query(True, alias="async")):
    """
    Async by default:
      - API enqueues issue_client job
      - worker:
          * generates UUID
          * add_client(...)
          * builds vless:// link from env
          * optional notify
      - client polls GET /jobs/{job_id}
    """
    if not async_:
        raise HTTPException(status_code=400, detail={"code": "SYNC_DISABLED", "message": "sync mode disabled; use /clients/issue?async=true"})

    try:
        job_id, deduped = await enqueue_issue_job(req.model_dump())
        return JobEnqueueResponse(job_id=job_id, deduped=deduped)
    except Exception as e:
        log.exception("enqueue_issue_job failed", extra={"request_id": request.state.request_id})
        return api_error(request, 502, "REDIS_ERROR", "queue backend error", _safe_upstream_detail(e))
