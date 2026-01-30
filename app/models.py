"""
Pydantic models for Xray Agent API.

Основные контракты:
- IssueClientRequest: запрос на выдачу нового клиента (server генерирует UUID через worker)
- JobEnqueueResponse: ответ на постановку задачи в очередь (202 Accepted)
- JobStatusResponse: статус задачи и результат (если done)
- ClientAddRequest: legacy/sync add model (если где-то ещё используешь)
"""

from __future__ import annotations

from typing import Optional, Literal, Any, Dict
from pydantic import BaseModel, Field, field_validator

from app.config import settings


class IssueClientRequest(BaseModel):
    """
    Request to issue a new VLESS client.

    telegram_id:
      - numeric string (Telegram user_id)
      - будет использован как Xray user.email (уникальный ключ на inbound)
    flow:
      - если None -> берём DEFAULT_FLOW из .env
    """
    telegram_id: str = Field(..., description="Telegram user_id as numeric string (used as Xray user email)")
    inbound_tag: str = Field(default_factory=lambda: settings.default_inbound_tag, description="Xray inbound tag")
    level: int = Field(default=0, ge=0, le=255, description="Xray user level")
    flow: Optional[str] = Field(default=None, description="If null -> DEFAULT_FLOW from .env")

    @field_validator("telegram_id")
    @classmethod
    def validate_telegram_id(cls, v: str) -> str:
        vv = v.strip()
        if not vv.isdigit():
            raise ValueError("telegram_id must be numeric string, e.g. '123456789'")
        return vv


class JobEnqueueResponse(BaseModel):
    """
    Response for async enqueue endpoints.
    """
    status: Literal["queued"] = "queued"
    job_id: str = Field(..., description="Job identifier for polling GET /jobs/{job_id}")
    deduped: bool = Field(default=False, description="True if request was deduplicated by idempotency key")


class IssueClientResult(BaseModel):
    """
    Result produced by worker for issue_client job.
    This payload is also suitable for sending to notify-service.
    """
    uuid: str = Field(..., description="Generated VLESS UUID")
    email: str = Field(..., description="Telegram user_id used as Xray user email")
    inbound_tag: str = Field(..., description="Inbound tag")
    link: str = Field(..., description="Full vless:// link")


class JobStatusResponse(BaseModel):
    """
    Job status document stored in Redis.
    """
    id: str
    state: Literal["queued", "running", "done", "error"]
    ts: int = Field(..., description="Unix timestamp (seconds)")
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None


class ClientAddRequest(BaseModel):
    """
    Legacy model: add client with given UUID/email.
    Обычно используется для прямого /clients add. В твоём новом флоу это делает worker.

    email:
      - уникальный идентификатор пользователя в Xray inbound
      - у тебя фактически telegram_id (digits)
    uuid:
      - VLESS UUID
    """
    uuid: str = Field(..., description="VLESS UUID")
    email: str = Field(..., description="Unique identifier used for remove (telegram_id recommended)")
    inbound_tag: str = Field(default_factory=lambda: settings.default_inbound_tag)
    level: int = 0
    flow: str = ""
