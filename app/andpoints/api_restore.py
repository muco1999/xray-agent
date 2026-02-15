from __future__ import annotations

import asyncio
import time
from typing import Optional, List, Set

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.auth import require_token
# Твой grpcio адаптер (путь поправь под свой проект)


from app.xray import inbound_emails, add_client, inbound_users_count, xray_runtime_status

router = APIRouter(prefix="/xray", tags=["xray-restore"])


# ----------------- MODELS -----------------
class RestoreItem(BaseModel):
    email: str = Field(..., description="email в Xray (лучше user_id строкой)")
    uuid: str = Field(..., description="готовый UUID клиента")
    level: int = 0
    flow: str = "xtls-rprx-vision"


class RestoreIn(BaseModel):
    inbound_tag: str = "vless-in"
    items: List[RestoreItem]
    precheck: bool = True
    concurrency: int = 20


class RestoreOut(BaseModel):
    inbound_tag: str
    total: int
    before_count: Optional[int] = None
    after_count: Optional[int] = None
    exists: int
    added: int
    skipped: int
    errors: int
    duration_ms: float
    error_samples: list[str]


class AddUserIn(BaseModel):
    inbound_tag: str = "vless-in"
    email: str
    uuid: str
    level: int = 0
    flow: str = "xtls-rprx-vision"
    precheck: bool = True


class AddUserOut(BaseModel):
    ok: bool
    inbound_tag: str
    email: str
    uuid: str
    detail: str = ""


# ----------------- HELPERS -----------------
def _is_already_exists(msg: str) -> bool:
    m = (msg or "").lower()
    # grpcio может возвращать разные формулировки
    return ("already" in m and "exist" in m) or ("alreadyexists" in m) or ("duplicate" in m)


async def _fetch_exists_set(tag: str) -> Optional[Set[str]]:
    """
    Возвращает set(email) текущих пользователей inbound.
    Если Xray недоступен/ошибка — кидаем исключение (это лучше чем тихо ломать restore).
    """
    emails = await asyncio.to_thread(inbound_emails, tag)
    return set(emails)


# ----------------- ENDPOINTS -----------------
@router.get("/health", dependencies=[Depends(require_token)])
async def health():
    # возвращает состояние xray api + sys stats
    return await asyncio.to_thread(xray_runtime_status)


@router.post("/add_user", response_model=AddUserOut, dependencies=[Depends(require_token)])
async def add_user(payload: AddUserIn):
    try:
        exists_set: Optional[Set[str]] = None
        if payload.precheck:
            exists_set = await _fetch_exists_set(payload.inbound_tag)
            if payload.email in exists_set:
                return AddUserOut(
                    ok=True,
                    inbound_tag=payload.inbound_tag,
                    email=payload.email,
                    uuid=payload.uuid,
                    detail="exists (precheck)",
                )

        await asyncio.to_thread(
            add_client,
            payload.uuid,
            payload.email,
            payload.inbound_tag,
            payload.level,
            payload.flow,
        )
        return AddUserOut(ok=True, inbound_tag=payload.inbound_tag, email=payload.email, uuid=payload.uuid, detail="added")

    except Exception as e:
        msg = str(e)
        if _is_already_exists(msg):
            return AddUserOut(ok=True, inbound_tag=payload.inbound_tag, email=payload.email, uuid=payload.uuid, detail="already exists")
        raise HTTPException(status_code=502, detail=msg[:1200])


@router.post("/restore", response_model=RestoreOut, dependencies=[Depends(require_token)])
async def restore(payload: RestoreIn):
    t0 = time.perf_counter()
    tag = payload.inbound_tag
    total = len(payload.items)

    # counts до восстановления (не критично, но полезно)
    try:
        before_count = await asyncio.to_thread(inbound_users_count, tag)
    except Exception:
        before_count = None

    exists_set: Optional[Set[str]] = None
    if payload.precheck and total > 0:
        # один раз снимаем текущий список email
        exists_set = await _fetch_exists_set(tag)

    # статистика
    exists = 0
    added = 0
    skipped = 0
    errors = 0
    samples: list[str] = []

    # очередь без создания 50k task-ов сразу
    q: asyncio.Queue[Optional[RestoreItem]] = asyncio.Queue()
    for it in payload.items:
        q.put_nowait(it)

    sem = asyncio.Semaphore(max(1, min(payload.concurrency, 100)))

    async def worker():
        nonlocal exists, added, skipped, errors, samples, exists_set
        while True:
            item = await q.get()
            if item is None:
                q.task_done()
                return

            async with sem:
                try:
                    # 1) precheck
                    if exists_set is not None and item.email in exists_set:
                        exists += 1
                    else:
                        # 2) add
                        await asyncio.to_thread(
                            add_client,
                            item.uuid,
                            item.email,
                            tag,
                            int(item.level),
                            item.flow,
                        )
                        added += 1
                        if exists_set is not None:
                            exists_set.add(item.email)

                except Exception as e:
                    msg = str(e)
                    if _is_already_exists(msg):
                        # если precheck был выключен/устарел — это норм
                        skipped += 1
                        if exists_set is not None:
                            exists_set.add(item.email)
                    else:
                        errors += 1
                        if len(samples) < 20:
                            samples.append(f"{item.email}: {msg[:220]}")

                finally:
                    # мягкая пауза, чтобы не “прибить” gRPC
                    await asyncio.sleep(0.005)

            q.task_done()

    workers = [asyncio.create_task(worker()) for _ in range(max(1, min(payload.concurrency, 100)))]
    await q.join()
    for _ in workers:
        q.put_nowait(None)
    await asyncio.gather(*workers)

    # counts после
    try:
        after_count = await asyncio.to_thread(inbound_users_count, tag)
    except Exception:
        after_count = None

    dt = (time.perf_counter() - t0) * 1000.0

    return RestoreOut(
        inbound_tag=tag,
        total=total,
        before_count=before_count,
        after_count=after_count,
        exists=exists,
        added=added,
        skipped=skipped,
        errors=errors,
        duration_ms=round(dt, 2),
        error_samples=samples,
    )