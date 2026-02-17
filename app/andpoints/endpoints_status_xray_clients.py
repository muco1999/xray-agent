# endpoints_status_xray_clients.py

from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from app.logger import log
from app.settings import settings

from app.auth import require_token

router = APIRouter(tags=["xray-logfile"])



LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("xray_status")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )

# -----------------------------------------------------------------------------
# 🧠 Regex под ТВОЙ access.log
# Поддерживает:
#   from 109.252.151.127:1989 accepted tcp:host:443 [vless-in -> direct] email: 796...
#   from tcp:109.252.151.127:1986 accepted udp:8.8.8.8:53 [vless-in -> direct] email: 796...
# Игнорируем rejected
# -----------------------------------------------------------------------------
XRAY_ACCESS_RE = re.compile(
    r"""
    (?P<ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)
    \s+from\s+(?:(?P<src_proto>tcp|udp):)?(?P<src_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<src_port>\d+)
    \s+(?P<result>accepted|rejected)\s+
    (?P<proto>tcp|udp):(?P<dst>[^ ]+)
    \s+\[(?P<flow>[^\]]+)\]
    (?:\s+email:\s*(?P<email>\S+))?
    """,
    re.VERBOSE,
)

def _parse_ts_to_epoch(ts: str) -> float:
    fmt = "%Y/%m/%d %H:%M:%S.%f" if "." in ts else "%Y/%m/%d %H:%M:%S"
    # access.log без TZ → считаем как локальное время сервера.
    # Если хочешь строго UTC — можно заменить на timezone-aware.
    dt = datetime.strptime(ts, fmt)
    return time.mktime(dt.timetuple()) + dt.microsecond / 1_000_000.0

def _epoch_to_iso(epoch: float) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat()

# -----------------------------------------------------------------------------
# 🧾 TTL-кэш + lock
# -----------------------------------------------------------------------------
@dataclass
class CacheEntry:
    ts: float
    value: Optional[Dict[str, Any]]

_STATUS_CACHE = CacheEntry(ts=0.0, value=None)
_STATUS_CACHE_LOCK = asyncio.Lock()

# -----------------------------------------------------------------------------
# 📥 Чтение последних строк файла
# -----------------------------------------------------------------------------
async def read_access_log_tail(path: str, max_lines: int = settings.tail_max_lines) -> List[str]:
    """
    Быстро читаем tail файла. Для простоты и надёжности читаем целиком и берём хвост.
    На твоих объёмах это ок, но если файл станет гигабайтами — сделаем оптимизированный tail.
    """
    p = Path(path)
    if not p.exists():
        log.warning("XRAY access log not found (guard will skip)", path=str(p))
        return []


    # IO в threadpool
    def _read() -> List[str]:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            lines = f.read().splitlines()
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        return lines

    return await run_in_threadpool(_read)

# -----------------------------------------------------------------------------
# 🧩 Парсинг строк access.log -> события
# -----------------------------------------------------------------------------
def parse_xray_access_lines(lines: List[str], inbound_tag: str) -> List[Dict[str, Any]]:
    needle = f"[{inbound_tag} ->"
    events: List[Dict[str, Any]] = []

    for ln in lines:
        # быстрое отсеивание: нужен наш inbound и accepted
        if needle not in ln or " accepted " not in ln:
            continue

        m = XRAY_ACCESS_RE.search(ln)
        if not m:
            continue

        if m.group("result") != "accepted":
            continue

        email = (m.group("email") or "").strip()
        if not email:
            # accepted, но без email — нам не подходит для антишаринга
            continue

        try:
            t = _parse_ts_to_epoch(m.group("ts"))
        except Exception:
            continue

        dst = m.group("dst")
        host = dst.rsplit(":", 1)[0] if ":" in dst else dst

        events.append(
            {
                "t": t,
                "email": email,
                "src_ip": m.group("src_ip"),
                "proto": m.group("proto"),
                "dst": dst,
                "host": host,
            }
        )

    return events

# -----------------------------------------------------------------------------
# 📊 Агрегация
# -----------------------------------------------------------------------------
def aggregate_status(events, now, online_window_sec, devices_limit, ip_active_ttl_sec):
    per_email_ip_last: Dict[str, Dict[str, float]] = defaultdict(dict)
    per_email_last: Dict[str, float] = defaultdict(float)
    per_email_hosts: Dict[str, Counter] = defaultdict(Counter)
    per_email_events: Dict[str, int] = defaultdict(int)

    for e in events:
        email = e["email"]
        ip = e["src_ip"]
        prev = per_email_ip_last[email].get(ip, 0.0)
        if e["t"] > prev:
            per_email_ip_last[email][ip] = e["t"]
        per_email_last[email] = max(per_email_last[email], e["t"])
        per_email_hosts[email][e["host"]] += 1
        per_email_events[email] += 1

    clients: List[Dict[str, Any]] = []
    online_count = 0
    suspicious_count = 0

    for email, ip_last in per_email_ip_last.items():
        last_seen = per_email_last[email]
        online = (now - last_seen) <= online_window_sec
        if online:
            online_count += 1

        active_ips = {ip for ip, t in ip_last.items() if (now - t) <= ip_active_ttl_sec}
        devices = len(active_ips)

        suspicious = devices > devices_limit
        if suspicious:
            suspicious_count += 1

        clients.append(
            {
                "email": email,
                "online": online,
                "last_seen_epoch": last_seen,
                "last_seen_iso_utc": _epoch_to_iso(last_seen),
                "last_seen_ago_sec": round(max(0.0, now - last_seen), 3),
                "unique_ips": sorted(active_ips),
                "devices_estimate": devices,
                "events": per_email_events[email],
                "top_hosts": [{"host": h, "hits": c} for h, c in per_email_hosts[email].most_common(8)],
                "suspicious": suspicious,
            }
        )

    clients.sort(key=lambda x: (not x["online"], x["last_seen_ago_sec"]))

    return {
        "window_events": len(events),
        "clients_total_seen": len(per_email_ip_last),
        "clients_online": online_count,
        "suspicious_clients": suspicious_count,
        "clients": clients,
    }

# -----------------------------------------------------------------------------
# 🌐 ss:443
# -----------------------------------------------------------------------------
async def get_established_443_count() -> int:
    cmd = ["ss", "-Hnt", "state", "established", "sport", "=", ":443"]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    if proc.returncode != 0:
        logger.warning("ss failed rc=%s err=%s", proc.returncode, (err.decode(errors="ignore") or "")[:200])
        return -1
    return sum(1 for ln in out.decode(errors="ignore").splitlines() if ln.strip())

# -----------------------------------------------------------------------------
# ✅ snapshot (file-based)
# -----------------------------------------------------------------------------
async def build_xray_status_snapshot() -> Dict[str, Any]:
    now = time.time()

    if _STATUS_CACHE.value is not None and (now - _STATUS_CACHE.ts) < settings.cache_ttl_sec:
        return _STATUS_CACHE.value

    async with _STATUS_CACHE_LOCK:
        now2 = time.time()
        if _STATUS_CACHE.value is not None and (now2 - _STATUS_CACHE.ts) < settings.cache_ttl_sec:
            return _STATUS_CACHE.value

        t0 = time.time()
        lines = await read_access_log_tail(settings.access_log_path, max_lines=settings.tail_max_lines)
        events = await run_in_threadpool(parse_xray_access_lines, lines, settings.default_inbound_tag)

        # фильтруем по WINDOW_SEC (по времени события)
        cutoff = now2 - settings.window_sec
        events = [e for e in events if e["t"] >= cutoff]

        agg = await run_in_threadpool(aggregate_status, events, now2, settings.online_window_sec, settings.devices_limit, settings.ip_active_ttl_sec)
        est_443 = await get_established_443_count()

        dur_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": True,
            "source": f"logfile:{settings.access_log_path}",
            "ts_epoch": now2,
            "ts_iso_utc": _epoch_to_iso(now2),
            "window_sec": settings.window_sec,
            "online_window_sec": settings.online_window_sec,
            "devices_limit": settings.devices_limit,
            "inbound_tag": settings.default_inbound_tag,
            "connections_established_443": est_443,
            "parse_ms": dur_ms,
            **agg,
        }

        _STATUS_CACHE.ts = now2
        _STATUS_CACHE.value = payload
        return payload

# -----------------------------------------------------------------------------
# 🩺 Healthcheck: logfile
# -----------------------------------------------------------------------------
@router.get("/health/logfile", dependencies=[Depends(require_token)])
async def health_logfile():
    try:
        lines = await read_access_log_tail(settings.access_log_path, max_lines=5)
        return {"ok": True, "source": f"logfile:{settings.access_log_path}", "tail_lines": len(lines)}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"logfile unavailable: {e}")

# -----------------------------------------------------------------------------
# 🚀 Endpoint: статус клиентов
# -----------------------------------------------------------------------------
@router.get("/xray/status/clients", dependencies=[Depends(require_token)])
async def xray_status_clients(request: Request):
    try:
        st = await build_xray_status_snapshot()
        return {"ok": True, "endpoint": "/xray/status/clients", "request_id": getattr(request.state, "request_id", None), **st}
    except Exception as e:
        logger.exception("xray_status_clients failed")
        return {
            "ok": False,
            "endpoint": "/xray/status/clients",
            "error": str(e),
            "source": f"logfile:{settings.access_log_path}",
            "request_id": getattr(request.state, "request_id", None),
        }