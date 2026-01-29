"""
FastAPI entrypoint for Xray Agent.

Этот сервис предоставляет удалённое управление Xray через gRPC API:
- health/status
- add/remove VLESS users (через AlterInbound + TypedMessage)
- получение пользователей inbound: users/emails/uuids
- async очередь через Redis (опционально, ?async=true)

Авторизация:
- Заголовок: Authorization: Bearer <API_TOKEN>
"""

from fastapi import FastAPI, Depends, HTTPException, Query
from pydantic import BaseModel, Field

from app.auth import require_token
from app.config import settings
from app.queue import enqueue_job, get_job
from app.xray import (
    xray_runtime_status,
    add_client,
    remove_client,
    inbound_users,
    inbound_users_count,
    inbound_emails,
    inbound_uuids,
)

app = FastAPI(title="Xray Agent API", version="1.0.0")


class ClientAddRequest(BaseModel):
    """
    Запрос на добавление клиента в inbound.

    email:
      - служит уникальным идентификатором пользователя
      - используется для удаления
    uuid:
      - VLESS UUID
    inbound_tag:
      - tag inbound в Xray, куда добавляем пользователя (обычно "vless-in")
    """
    uuid: str = Field(..., description="VLESS UUID")
    email: str = Field(..., description="Unique identifier used for remove")
    inbound_tag: str = Field(default_factory=lambda: settings.default_inbound_tag)
    level: int = 0
    flow: str = ""


class JobEnqueueResponse(BaseModel):
    """Ответ при async=true: возвращаем job_id для опроса статуса."""
    job_id: str


@app.get("/health/full", dependencies=[Depends(require_token)])
def health_full():
    """
    Полная проверка работоспособности:
    - порт Xray API открыт
    - grpcurl доступен
    - GetSysStats успешен (через proto, без reflection)

    Возвращает ok=true только если все проверки прошли.
    """
    status = xray_runtime_status()
    return {"time": status.get("time"), "xray": status, "ok": bool(status.get("ok"))}


@app.get("/xray/status", dependencies=[Depends(require_token)])
def xray_status():
    """
    Быстрый статус Xray API:
    - открыт ли порт gRPC
    - есть ли grpcurl
    - (опционально) sys stats
    """
    return xray_runtime_status()


@app.post("/clients", dependencies=[Depends(require_token)])
def api_add_client(req: ClientAddRequest, async_: bool = Query(False, alias="async")):
    """
    Добавить клиента в inbound.

    Sync режим:
      POST /clients
      -> выполняем grpcurl AlterInbound сразу

    Async режим:
      POST /clients?async=true
      -> кладём задачу в Redis, возвращаем job_id
      -> worker выполнит добавление
    """
    if async_:
        job_id = enqueue_job("add_client", req.model_dump())
        return JobEnqueueResponse(job_id=job_id)

    try:
        return {"result": add_client(req.uuid, req.email, req.inbound_tag, req.level, req.flow)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/clients/{email}", dependencies=[Depends(require_token)])
def api_remove_client(
    email: str,
    inbound_tag: str = settings.default_inbound_tag,
    async_: bool = Query(False, alias="async"),
):
    """
    Удалить клиента по email из inbound.

    Sync:
      DELETE /clients/{email}?inbound_tag=vless-in

    Async:
      DELETE /clients/{email}?inbound_tag=vless-in&async=true
    """
    if async_:
        job_id = enqueue_job("remove_client", {"email": email, "inbound_tag": inbound_tag})
        return JobEnqueueResponse(job_id=job_id)

    try:
        return {"result": remove_client(email=email, inbound_tag=inbound_tag)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/inbounds/{tag}/users", dependencies=[Depends(require_token)])
def api_inbound_users(tag: str):
    """
    Вернуть сырые данные пользователей inbound (как отдаёт Xray GetInboundUsers).
    """
    return {"result": inbound_users(tag)}


@app.get("/inbounds/{tag}/users/count", dependencies=[Depends(require_token)])
def api_inbound_users_count(tag: str):
    """Количество пользователей inbound."""
    return {"result": inbound_users_count(tag)}


@app.get("/inbounds/{tag}/emails", dependencies=[Depends(require_token)])
def api_inbound_emails(tag: str):
    """Список email пользователей inbound."""
    return {"result": inbound_emails(tag)}


@app.get("/inbounds/{tag}/uuids", dependencies=[Depends(require_token)])
def api_inbound_uuids(tag: str):
    """
    Список UUID пользователей inbound (только VLESS Account).
    UUID извлекается из TypedMessage.value -> protobuf decode.
    """
    return {"result": inbound_uuids(tag)}


@app.get("/jobs/{job_id}", dependencies=[Depends(require_token)])
def api_job_get(job_id: str):
    """
    Получить статус async-задачи из Redis.
    states:
      - queued
      - running
      - done
      - error
    """
    return get_job(job_id)
