
from __future__ import annotations

from typing import Any, Dict, Optional

from fastapi import Request

from starlette.responses import JSONResponse


# ----------------------------
# Error normalization
# ----------------------------

def _err_payload(
    request_id: str,
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": request_id,
            "details": details or {},
        }
    }


def api_error(
    request: Request,
    http_status: int,
    code: str,
    message: str,
    details: Optional[Dict[str, Any]] = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=http_status,
        content=_err_payload(request.state.request_id, code, message, details),
    )


def _safe_upstream_detail(exc: Exception) -> Dict[str, Any]:
    """
    Не допускаем утечку внутренностей: возвращаем только тип ошибки.
    Полный текст логируем на сервере.
    """
    return {"exception": type(exc).__name__}



