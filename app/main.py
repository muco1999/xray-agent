from fastapi import FastAPI
from app.andpoints.endpoints_status_xray_clients import router as router_status_xray_clients
from app.andpoints.endpoints_work_clients import router as router_work_clients

import uuid

from fastapi import HTTPException, Request

from app.andpoints.logger import log
from app.andpoints.tools import api_error

app = FastAPI(title="Xray Agent API", version="1.0.0")







@app.middleware("http")
async def request_id_middleware(request: Request, call_next):
    rid = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = rid

    try:
        response = await call_next(request)
    except HTTPException as e:
        # handled by exception handler below
        raise e
    except Exception as e:
        log.exception("Unhandled error", extra={"request_id": rid})
        return api_error(request, 500, "INTERNAL_ERROR", "internal server error", {"exception": type(e).__name__})

    response.headers["X-Request-ID"] = rid
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    # Normalize HTTPException
    if isinstance(exc.detail, dict):
        code = exc.detail.get("code", "HTTP_ERROR")
        message = exc.detail.get("message", "request failed")
        details = {k: v for k, v in exc.detail.items() if k not in ("code", "message")}
    else:
        code = "HTTP_ERROR"
        message = str(exc.detail)
        details = {}
    return api_error(request, exc.status_code, code, message, details)






app.include_router(router_status_xray_clients)
app.include_router(router_work_clients)



