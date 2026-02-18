# api_restore.py
from __future__ import annotations

from typing import Optional, Set, Iterable, Tuple, List
import asyncio
import time
import logging
from uuid import uuid4

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from app.auth import require_token
from app.xray import add_client, inbound_users_count, xray_runtime_status, AlreadyExistsError, inbound_emails

# ✅ grpc.aio adapter (async)

router = APIRouter(prefix="/xray", tags=["xray-restore"])
log = logging.getLogger("restore")


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
def _is_already_exists_msg(msg: str) -> bool:
    m = (msg or "").lower()
    # grpc может возвращать разные формулировки
    return ("already" in m and "exist" in m) or ("alreadyexists" in m) or ("duplicate" in m)


def _is_already_exists_exc(exc: Exception) -> bool:
    if isinstance(exc, AlreadyExistsError):
        return True
    return _is_already_exists_msg(str(exc))


async def _fetch_exists_set(tag: str) -> Set[str]:
    """
    Возвращает set(email) текущих пользователей inbound.
    Если Xray недоступен/ошибка — кидаем исключение (это лучше чем тихо ломать restore).
    """
    emails = await inbound_emails(tag)
    return set(emails)


def _norm_email(s: str) -> str:
    return (s or "").strip().casefold()


def _dedupe_items(items: Iterable["RestoreItem"]) -> Tuple[List["RestoreItem"], int]:
    """
    Сейчас dedupe по (email, uuid). Если у тебя уникальность по email —
    замени key на (_norm_email(it.email),).
    """
    seen: Set[Tuple[str, str]] = set()
    out: List["RestoreItem"] = []
    dup = 0

    for it in items:
        key = (_norm_email(str(it.email)), str(it.uuid))
        if key in seen:
            dup += 1
            continue
        seen.add(key)
        out.append(it)

    return out, dup


# ----------------- ENDPOINTS -----------------
@router.get("/health", dependencies=[Depends(require_token)])
async def health():
    # возвращает состояние xray api + sys stats
    return await xray_runtime_status()


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

        await add_client(
            payload.uuid,
            payload.email,
            payload.inbound_tag,
            payload.level,
            payload.flow,
        )
        return AddUserOut(ok=True, inbound_tag=payload.inbound_tag, email=payload.email, uuid=payload.uuid, detail="added")

    except Exception as e:
        if _is_already_exists_exc(e):
            return AddUserOut(ok=True, inbound_tag=payload.inbound_tag, email=payload.email, uuid=payload.uuid, detail="already exists")
        raise HTTPException(status_code=502, detail=str(e)[:1200])


@router.post("/restore", response_model=RestoreOut, dependencies=[Depends(require_token)])
async def restore(payload: RestoreIn) -> RestoreOut:
    req_id = uuid4().hex[:10]
    t0 = time.perf_counter()

    tag = payload.inbound_tag
    items_in = payload.items or []
    total_in = len(items_in)

    workers_n = max(1, min(int(getattr(payload, "concurrency", 20) or 20), 100))
    delay_ms = int(getattr(payload, "delay_ms", 0) or 0)
    delay_sec = max(0.0, delay_ms / 1000.0)

    # ⚠️ раньше была защита от thread explosion из-за grpcio blocking.
    # Теперь grpc.aio не блокирует event loop, но concurrency всё равно нужен,
    # чтобы не DDOS-ить Xray.
    q_maxsize = max(8, workers_n * 4)
    q: asyncio.Queue[Optional[RestoreItem]] = asyncio.Queue(maxsize=q_maxsize)

    timeout_sec = float(getattr(payload, "timeout_sec", 0) or 0)
    use_timeout = timeout_sec > 0

    # counts before
    try:
        before_count = await inbound_users_count(tag)
    except Exception:
        before_count = None

    if total_in == 0:
        try:
            after_count = await inbound_users_count(tag)
        except Exception:
            after_count = None

        dt = (time.perf_counter() - t0) * 1000.0
        return RestoreOut(
            inbound_tag=tag,
            total=0,
            before_count=before_count,
            after_count=after_count,
            exists=0,
            added=0,
            skipped=0,
            errors=0,
            duration_ms=round(dt, 2),
            error_samples=[],
        )

    items, dup_count = _dedupe_items(items_in)
    total = len(items)

    exists_set: Optional[Set[str]] = None
    if payload.precheck:
        try:
            raw = await _fetch_exists_set(tag)
            exists_set = {_norm_email(x) for x in raw}
        except Exception as e:
            raise HTTPException(status_code=502, detail=f"precheck failed: {str(e)[:1200]}")

    log.info(
        "restore start req_id=%s tag=%s total_in=%d total_dedup=%d dup=%d workers=%d precheck=%s delay_ms=%d timeout_sec=%s",
        req_id, tag, total_in, total, dup_count, workers_n, bool(exists_set), delay_ms, timeout_sec if use_timeout else "off"
    )

    async def producer() -> None:
        try:
            for it in items:
                await q.put(it)
        finally:
            for _ in range(workers_n):
                await q.put(None)

    async def worker() -> tuple[int, int, int, int, list[str]] | None:
        w_exists = 0
        w_added = 0
        w_skipped = 0
        w_errors = 0
        w_samples: list[str] = []

        while True:
            item = await q.get()
            try:
                if item is None:
                    return w_exists, w_added, w_skipped, w_errors, w_samples

                if exists_set is not None and _norm_email(item.email) in exists_set:
                    w_exists += 1
                    continue

                try:
                    await add_client(
                        item.uuid,
                        item.email,
                        tag,
                        int(item.level),
                        item.flow,
                    )
                    w_added += 1
                except Exception as e:
                    if _is_already_exists_exc(e):
                        w_skipped += 1
                    else:
                        w_errors += 1
                        if len(w_samples) < 20:
                            w_samples.append(f"{item.email}: {str(e)[:220]}")

                if delay_sec:
                    await asyncio.sleep(delay_sec)
            finally:
                q.task_done()

    async def run_all() -> list[tuple[int, int, int, int, list[str]]]:
        prod_task = asyncio.create_task(producer())
        w_tasks = [asyncio.create_task(worker()) for _ in range(workers_n)]
        try:
            await prod_task
            await q.join()
            return await asyncio.gather(*w_tasks)
        finally:
            for t in w_tasks:
                if not t.done():
                    t.cancel()

    try:
        if use_timeout:
            async with asyncio.timeout(timeout_sec):
                results = await run_all()
        else:
            results = await run_all()
    except TimeoutError:
        raise HTTPException(status_code=504, detail=f"restore timeout after {timeout_sec:.1f}s")

    exists = 0
    added = 0
    skipped = dup_count
    errors = 0
    samples: list[str] = []

    for w_exists, w_added, w_skipped, w_errors, w_samples in results:
        exists += w_exists
        added += w_added
        skipped += w_skipped
        errors += w_errors
        for s in w_samples:
            if len(samples) >= 20:
                break
            samples.append(s)

    try:
        after_count = await inbound_users_count(tag)
    except Exception:
        after_count = None

    dt = (time.perf_counter() - t0) * 1000.0

    log.info(
        "restore done req_id=%s tag=%s total=%d before=%s after=%s exists=%d added=%d skipped=%d errors=%d duration_ms=%.2f",
        req_id, tag, total, before_count, after_count, exists, added, skipped, errors, dt,
    )

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