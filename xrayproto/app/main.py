import time
from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from .auth import require_token
from .config import settings
from .queue import enqueue_job, get_job
from .xray import xray_runtime_status, add_client, remove_client

app = FastAPI(title="Xray Manager API", version="0.3.0")


class ClientAddRequest(BaseModel):
    uuid: str = Field(..., description="VLESS UUID")
    email: str = Field(..., description="Unique identifier used for remove")
    inbound_tag: str = Field(default_factory=lambda: settings.default_inbound_tag)
    level: int = 0
    flow: str = ""


class JobEnqueueResponse(BaseModel):
    job_id: str


@app.get("/health/full", dependencies=[Depends(require_token)])
def health_full():
    """
    Full health check (container-friendly):
    - checks Xray API port
    - calls GetSysStats via grpcurl
    """
    x = xray_runtime_status()
    return {
        "time": int(time.time()),
        "xray": x,
        "ok": bool(x.get("ok")),
    }


@app.get("/xray/status", dependencies=[Depends(require_token)])
def xray_status():
    """
    Same as health but only Xray-related info.
    """
    return xray_runtime_status()


@app.post("/clients", dependencies=[Depends(require_token)])
def api_add_client(req: ClientAddRequest, async_: bool = Query(False, alias="async")):
    if async_:
        job_id = enqueue_job("add_client", req.model_dump())
        return JobEnqueueResponse(job_id=job_id)

    try:
        result = add_client(req.uuid, req.email, req.inbound_tag, req.level, req.flow)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/clients/{email}", dependencies=[Depends(require_token)])
def api_remove_client(
    email: str,
    inbound_tag: str = settings.default_inbound_tag,
    async_: bool = Query(False, alias="async"),
):
    if async_:
        job_id = enqueue_job("remove_client", {"email": email, "inbound_tag": inbound_tag})
        return JobEnqueueResponse(job_id=job_id)

    try:
        result = remove_client(email=email, inbound_tag=inbound_tag)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/jobs/{job_id}", dependencies=[Depends(require_token)])
def api_job_get(job_id: str):
    return get_job(job_id)
