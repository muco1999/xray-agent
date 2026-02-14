"""
📌 Xray (VLESS + REALITY) — Production status endpoint на основе journald (journalctl -o json)

Что делает этот модуль:
- Читает последние логи Xray через journald:
    journalctl -u xray --since "<N seconds ago>" -o json
- Берёт точный timestamp из journald (__REALTIME_TIMESTAMP) ✅
- Парсит сообщения Xray вида: `... accepted ... email: <id>`
- Фильтрует только нужный inbound: `[vless-in -> ...]`
- Собирает статистику по каждому клиенту (email):
  - online: был ли трафик за последние ONLINE_WINDOW_SEC секунд
  - devices_estimate: количество уникальных IP за окно WINDOW_SEC (≈ минимум устройств)
  - unique_ips: список IP
  - last_seen: когда последний раз был трафик (точно по journald)
  - events: сколько событий accepted
  - top_hosts: топ доменов/хостов куда ходил клиент
  - suspicious: если устройств больше DEVICES_LIMIT (подозрение на шаринг)
- Собирает общую метрику по серверу:
  - connections_established_443: сколько сейчас ESTABLISHED TCP соединений на 443 (без привязки к email)

⚠️ Важно:
- Xray не сообщает "устройство" напрямую. Мы оцениваем "устройство" как уникальный внешний IP.
- На мобильной сети IP может меняться (LTE/Wi-Fi), поэтому devices_estimate иногда может быть >1 даже у одного человека.
- "online" определяется по появлению новых accepted-событий. Если клиент "висит" тихо — он может считаться offline.
  Поэтому ONLINE_WINDOW_SEC лучше держать 180–300 при connIdle=300.

Требования:
- Xray запущен как systemd unit: `xray` (XRAY_SYSTEMD_UNIT)
- inbound tag: `vless-in` (XRAY_INBOUND_TAG)
- FastAPI сервис имеет права читать journald:
    - либо root,
    - либо пользователь в группе systemd-journal.
- Python 3.10+

Рекомендация для продакшна:
- Включён TTL-кэш + asyncio.Lock(), чтобы не дёргать journalctl параллельно.
- Есть /health/journal — возвращает 200 только если journald читается успешно.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import HTTPException, Request
from starlette.concurrency import run_in_threadpool

from app.auth import require_token

from fastapi import APIRouter, Depends


router = APIRouter(tags=["xray-journald"])

# -----------------------------------------------------------------------------
# 🔐 Security (заглушка)
# -----------------------------------------------------------------------------



# -----------------------------------------------------------------------------
# ⚙️ Настройки (можно вынести в env/config)
# -----------------------------------------------------------------------------
XRAY_SYSTEMD_UNIT = "xray"       # systemd unit name ✅
XRAY_INBOUND_TAG = "vless-in"    # inbound tag как в логах: [vless-in -> direct] ✅

WINDOW_SEC = 10 * 60             # окно анализа логов (10 минут)
ONLINE_WINDOW_SEC = 240          # "онлайн", если активность была <= ONLINE_WINDOW_SEC
DEVICES_LIMIT = 2                # если уникальных IP > DEVICES_LIMIT -> suspicious

JOURNAL_MAX_LINES = 25000        # ограничение по строкам, чтобы не съесть память
JOURNAL_TIMEOUT_SEC = 8.0        # timeout на вызов journalctl
CACHE_TTL_SEC = 2.0              # TTL кэш для ответа /xray/status/clients

# Логирование
LOG_LEVEL = "INFO"
logger = logging.getLogger("xray_status")
if not logger.handlers:
    logging.basicConfig(
        level=getattr(logging, LOG_LEVEL, logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


# -----------------------------------------------------------------------------
# 🧠 Regex для парсинга сообщения Xray (MESSAGE из journald)
# -----------------------------------------------------------------------------
# Пример сообщения:
# 2026/02/06 11:52:31.289090 from 94.25.174.100:14169 accepted tcp:www.youtube.com:443 [vless-in -> direct] email: 7313853417
XRAY_LINE_RE = re.compile(
    r"""
    (?P<ts>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?)
    .*?\sfrom\s(?P<src_ip>\d{1,3}(?:\.\d{1,3}){3}):(?P<src_port>\d+)
    \saccepted\s(?P<proto>tcp|udp):(?P<dst>[^ ]+)
    .*?\semail:\s(?P<email>\S+)
    """,
    re.VERBOSE,
)


# -----------------------------------------------------------------------------
# 🧾 TTL-кэш + lock (защита от параллельных вызовов journalctl)
# -----------------------------------------------------------------------------
@dataclass
class CacheEntry:
    ts: float
    value: Optional[Dict[str, Any]]


_STATUS_CACHE = CacheEntry(ts=0.0, value=None)
_STATUS_CACHE_LOCK = asyncio.Lock()


# -----------------------------------------------------------------------------
# 🕒 Journald timestamp
# -----------------------------------------------------------------------------
def _journald_realtime_to_epoch(entry: Dict[str, Any]) -> float:
    """
    __REALTIME_TIMESTAMP в journald: микросекунды с эпохи (Unix).
    Пример: "1707212345678901"
    """
    v = entry.get("__REALTIME_TIMESTAMP")
    if not v:
        # fallback: сейчас
        return time.time()

    try:
        # journald может вернуть как строку
        micro = int(v)
        return micro / 1_000_000.0
    except Exception:
        return time.time()


def _epoch_to_iso(epoch: float) -> str:
    """
    Красивый ISO-вид времени в UTC.
    """
    dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
    return dt.isoformat()


# -----------------------------------------------------------------------------
# 📥 Чтение journald (journalctl -o json)
# -----------------------------------------------------------------------------
async def read_journalctl_json(
    unit: str,
    since_seconds: int,
    max_lines: int = JOURNAL_MAX_LINES,
    timeout_sec: float = JOURNAL_TIMEOUT_SEC,
) -> List[Dict[str, Any]]:
    """
    Читаем journald за последние `since_seconds` секунд, формат JSON per-line.
    Каждая строка stdout — отдельный JSON объект.

    Команда:
      journalctl -u xray --since "<N seconds ago>" -o json --no-pager
    """
    cmd = [
        "journalctl",
        "-u", unit,
        "--since", f"{since_seconds} seconds ago",
        "--no-pager",
        "-o", "json",
    ]

    t0 = time.time()
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    try:
        out, err = await asyncio.wait_for(proc.communicate(), timeout=timeout_sec)
    except asyncio.TimeoutError:
        proc.kill()
        await proc.communicate()
        raise RuntimeError(f"journalctl timeout after {timeout_sec}s")

    if proc.returncode != 0:
        msg = (err.decode(errors="ignore") or "").strip()
        raise RuntimeError(f"journalctl failed rc={proc.returncode}: {msg[:500]}")

    lines = out.decode(errors="ignore").splitlines()
    if len(lines) > max_lines:
        lines = lines[-max_lines:]

    entries: List[Dict[str, Any]] = []
    bad = 0
    for ln in lines:
        ln = ln.strip()
        if not ln:
            continue
        try:
            entries.append(json.loads(ln))
        except Exception:
            bad += 1

    dur_ms = int((time.time() - t0) * 1000)
    logger.info(
        "journalctl_json ok | unit=%s since=%ss lines=%d parsed=%d bad=%d dur_ms=%d",
        unit, since_seconds, len(lines), len(entries), bad, dur_ms
    )
    return entries


# -----------------------------------------------------------------------------
# 🧩 Парсинг journald entries -> события
# -----------------------------------------------------------------------------
def parse_xray_entries(
    entries: List[Dict[str, Any]],
    inbound_tag: str = XRAY_INBOUND_TAG,
) -> List[Dict[str, Any]]:
    """
    Превращает journald JSON entries в список событий.
    Фильтруем только нужный inbound: `[vless-in -> ...]`.

    В journald сообщение обычно в полях:
      - "MESSAGE"
      - иногда "SYSLOG_IDENTIFIER" и т.п.

    Берём entry["MESSAGE"].
    """
    needle = f"[{inbound_tag} ->"
    events: List[Dict[str, Any]] = []

    for ent in entries:
        msg = ent.get("MESSAGE")
        if not msg or not isinstance(msg, str):
            continue

        # быстрый фильтр inbound
        if needle not in msg:
            continue

        m = XRAY_LINE_RE.search(msg)
        if not m:
            continue

        # точное время берём из journald
        t_epoch = _journald_realtime_to_epoch(ent)

        dst = m.group("dst")  # www.youtube.com:443
        host = dst.rsplit(":", 1)[0] if ":" in dst else dst

        events.append(
            {
                "t": t_epoch,
                "email": m.group("email").strip(),
                "src_ip": m.group("src_ip"),
                "proto": m.group("proto"),
                "dst": dst,
                "host": host,
            }
        )

    return events


# -----------------------------------------------------------------------------
# 📊 Агрегация событий -> статус
# -----------------------------------------------------------------------------
def aggregate_status(
    events: List[Dict[str, Any]],
    now: float,
    online_window_sec: int,
    devices_limit: Optional[int] = DEVICES_LIMIT,
) -> Dict[str, Any]:
    """
    Строит агрегированную статистику по email.
    """
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
        suspicious = bool(devices_limit and devices > devices_limit)
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
                "top_hosts": [
                    {"host": h, "hits": c}
                    for h, c in per_email_hosts[email].most_common(8)
                ],
                "suspicious": suspicious,
            }
        )

    # сортировка: online выше, затем самые "свежие" выше
    clients.sort(key=lambda x: (not x["online"], x["last_seen_ago_sec"]))

    return {
        "window_events": len(events),
        "clients_total_seen": len(per_email_ips),
        "clients_online": online_count,
        "suspicious_clients": suspicious_count,
        "clients": clients,
    }


# -----------------------------------------------------------------------------
# 🌐 Доп. метрика: активные TCP-сессии на 443 (без email)
# -----------------------------------------------------------------------------
async def get_established_443_count() -> int:
    """
    Возвращает количество ESTABLISHED TCP соединений на порту 443.
    Это НЕ связывается с email, но полезно для общего статуса нагрузки.

    Используем максимально совместимый синтаксис ss:
      ss -Hnt state established sport = :443
    """
    cmd = ["ss", "-Hnt", "state", "established", "sport", "=", ":443"]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()

    if proc.returncode != 0:
        # не падаем, просто возвращаем -1
        logger.warning("ss failed rc=%s err=%s", proc.returncode, (err.decode(errors="ignore") or "").strip()[:200])
        return -1

    lines = out.decode(errors="ignore").splitlines()
    return sum(1 for ln in lines if ln.strip())


# -----------------------------------------------------------------------------
# ✅ Главная функция статуса (TTL cache + lock)
# -----------------------------------------------------------------------------
async def build_xray_status_snapshot(
    unit: str = XRAY_SYSTEMD_UNIT,
    inbound_tag: str = XRAY_INBOUND_TAG,
    window_sec: int = WINDOW_SEC,
    online_window_sec: int = ONLINE_WINDOW_SEC,
    devices_limit: int = DEVICES_LIMIT,
) -> Dict[str, Any]:
    """
    Строит snapshot статуса. Используется эндпоинтом /xray/status/clients.

    Кэшируем на CACHE_TTL_SEC, чтобы:
    - не дёргать journalctl часто
    - снизить нагрузку на CPU/IO
    """
    now = time.time()

    # Быстрый путь: если кэш свежий — отдаём его
    if _STATUS_CACHE.value is not None and (now - _STATUS_CACHE.ts) < CACHE_TTL_SEC:
        return _STATUS_CACHE.value

    # Чтобы не запустить N journalctl параллельно — lock
    async with _STATUS_CACHE_LOCK:
        # Повторная проверка после lock (вдруг кто-то уже обновил)
        now2 = time.time()
        if _STATUS_CACHE.value is not None and (now2 - _STATUS_CACHE.ts) < CACHE_TTL_SEC:
            return _STATUS_CACHE.value

        # 1) читаем journald json
        entries = await read_journalctl_json(unit, since_seconds=window_sec)

        # 2) парсинг+агрегация — в threadpool, чтобы не блокировать event loop
        events = await run_in_threadpool(parse_xray_entries, entries, inbound_tag)
        agg = await run_in_threadpool(aggregate_status, events, now2, online_window_sec, devices_limit)

        # 3) метрика нагрузки
        est_443 = await get_established_443_count()

        payload = {
            "ok": True,
            "source": f"journald:{unit}",
            "ts_epoch": now2,
            "ts_iso_utc": _epoch_to_iso(now2),
            "window_sec": window_sec,
            "online_window_sec": online_window_sec,
            "devices_limit": devices_limit,
            "inbound_tag": inbound_tag,
            "connections_established_443": est_443,
            **agg,
        }

        _STATUS_CACHE.ts = now2
        _STATUS_CACHE.value = payload
        return payload


# -----------------------------------------------------------------------------
# 🩺 Healthcheck: проверка доступности journald
# -----------------------------------------------------------------------------
@router.get("/health/journal", dependencies=[Depends(require_token)])
async def health_journal():
    """
    Healthcheck для мониторинга.

    Возвращает 200 только если:
    - journalctl успешно выполняется
    - мы можем прочитать хотя бы 1 короткое окно (например 5 секунд)

    Если прав/доступа нет — вернёт 503.
    """
    try:
        # маленькое окно, чтобы было быстро
        _ = await read_journalctl_json(XRAY_SYSTEMD_UNIT, since_seconds=5, max_lines=200, timeout_sec=3.0)
        return {"ok": True, "source": f"journald:{XRAY_SYSTEMD_UNIT}"}
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"journald unavailable: {e}")


# -----------------------------------------------------------------------------
# 🚀 Endpoint: статус клиентов
# -----------------------------------------------------------------------------
@router.get("/xray/status/clients", dependencies=[Depends(require_token)])
async def xray_status_clients(request: Request):
    """
    Возвращает детальный статус клиентов Xray (VLESS + REALITY) на основе journald.

    Что именно отдаёт:
    - сколько клиентов было видно за окно
    - сколько сейчас онлайн
    - сколько потенциально "подозрительных" (шаринг ключа)
    - по каждому клиенту:
        - online / offline
        - last_seen (epoch + ISO UTC)
        - unique_ips (≈ устройства)
        - devices_estimate
        - топ доменов
        - количество событий
    - общее количество активных TCP-сессий на :443

    Ошибки:
    - если journalctl недоступен/нет прав — вернёт ok:false и error.
      Для мониторинга используйте /health/journal (он отдаёт 503).
    """
    try:
        st = await build_xray_status_snapshot()

        return {
            "ok": True,
            "endpoint": "/xray/status/clients",
            "request_id": getattr(request.state, "request_id", None),
            **st,
        }

    except Exception as e:
        logger.exception("xray_status_clients failed")
        return {
            "ok": False,
            "endpoint": "/xray/status/clients",
            "error": str(e),
            "source": f"journald:{XRAY_SYSTEMD_UNIT}",
            "request_id": getattr(request.state, "request_id", None),
        }