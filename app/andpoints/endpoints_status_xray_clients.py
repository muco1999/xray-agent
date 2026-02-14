from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from starlette.concurrency import run_in_threadpool

from app.auth import require_token

router = APIRouter(tags=["xray-logfile"])

# -----------------------------------------------------------------------------
# ⚙️ Настройки
# -----------------------------------------------------------------------------
XRAY_INBOUND_TAG = os.getenv("XRAY_INBOUND_TAG", "vless-in")

WINDOW_SEC = int(os.getenv("WINDOW_SEC", str(10 * 60)))
ONLINE_WINDOW_SEC = int(os.getenv("ONLINE_WINDOW_SEC", "240"))
DEVICES_LIMIT = int(os.getenv("DEVICES_LIMIT", "2"))

ACCESS_LOG_PATH = os.getenv("XRAY_ACCESS_LOG", "/var/log/xray/access.log")

TAIL_MAX_LINES = int(os.getenv("TAIL_MAX_LINES", "30000"))  # сколько последних строк читаем
CACHE_TTL_SEC = float(os.getenv("CACHE_TTL_SEC", "2.0"))

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
async def read_access_log_tail(path: str, max_lines: int = TAIL_MAX_LINES) -> List[str]:
    """
    Быстро читаем tail файла. Для простоты и надёжности читаем целиком и берём хвост.
    На твоих объёмах это ок, но если файл станет гигабайтами — сделаем оптимизированный tail.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)

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
def aggregate_status(events: List[Dict[str, Any]], now: float, online_window_sec: int, devices_limit: int) -> Dict[str, Any]:
    per_email_ips: Dict[str, set] = defaultdict(set)
    per_email_last: Dict[str, float] = defaultdict(float)
    per_email_hosts: Dict[str, Counter] = defaultdict(Counter)
    per_email_events: Dict[str, int] = defaultdict(int)

    for e in events:
        email = e["email"]
        per_email_ips[email].add(e["src_ip"])
        per_email_last[email] = max(per_email_last[email], e["t"])
        per_email_hosts[email][e["host"]] += 1
        per_email_events[email] += 1

    clients: List[Dict[str, Any]] = []
    online_count = 0
    suspicious_count = 0

    for email, ips in per_email_ips.items():
        last_seen = per_email_last[email]
        online = (now - last_seen) <= online_window_sec
        if online:
            online_count += 1

        devices = len(ips)
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
                "unique_ips": sorted(ips),
                "devices_estimate": devices,
                "events": per_email_events[email],
                "top_hosts": [{"host": h, "hits": c} for h, c in per_email_hosts[email].most_common(8)],
                "suspicious": suspicious,
            }
        )

    clients.sort(key=lambda x: (not x["online"], x["last_seen_ago_sec"]))

    return {
        "window_events": len(events),
        "clients_total_seen": len(per_email_ips),
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

    if _STATUS_CACHE.value is not None and (now - _STATUS_CACHE.ts) < CACHE_TTL_SEC:
        return _STATUS_CACHE.value

    async with _STATUS_CACHE_LOCK:
        now2 = time.time()
        if _STATUS_CACHE.value is not None and (now2 - _STATUS_CACHE.ts) < CACHE_TTL_SEC:
            return _STATUS_CACHE.value

        t0 = time.time()
        lines = await read_access_log_tail(ACCESS_LOG_PATH, max_lines=TAIL_MAX_LINES)
        events = await run_in_threadpool(parse_xray_access_lines, lines, XRAY_INBOUND_TAG)

        # фильтруем по WINDOW_SEC (по времени события)
        cutoff = now2 - WINDOW_SEC
        events = [e for e in events if e["t"] >= cutoff]

        agg = await run_in_threadpool(aggregate_status, events, now2, ONLINE_WINDOW_SEC, DEVICES_LIMIT)
        est_443 = await get_established_443_count()

        dur_ms = int((time.time() - t0) * 1000)
        payload = {
            "ok": True,
            "source": f"logfile:{ACCESS_LOG_PATH}",
            "ts_epoch": now2,
            "ts_iso_utc": _epoch_to_iso(now2),
            "window_sec": WINDOW_SEC,
            "online_window_sec": ONLINE_WINDOW_SEC,
            "devices_limit": DEVICES_LIMIT,
            "inbound_tag": XRAY_INBOUND_TAG,
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
        lines = await read_access_log_tail(ACCESS_LOG_PATH, max_lines=5)
        return {"ok": True, "source": f"logfile:{ACCESS_LOG_PATH}", "tail_lines": len(lines)}
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
            "source": f"logfile:{ACCESS_LOG_PATH}",
            "request_id": getattr(request.state, "request_id", None),
        }