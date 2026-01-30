"""
Worker процесс для Xray Agent.

Запускается отдельным процессом/контейнером.
Читает Redis LIST очередь через BRPOP и выполняет задачи:

- add_client (legacy)
- remove_client (legacy)
- issue_client:
    * генерирует UUID
    * добавляет пользователя в Xray (AlterInbound AddUserOperation)
    * формирует vless:// ссылку из параметров .env
    * опционально отправляет payload на внешний notify-сервис (/v1/notify)

Жизненный цикл job хранится в Redis через set_job_state(job_id, state, ...),
а API может отдавать статус/результат через GET /jobs/{job_id}.
"""

from __future__ import annotations

import json
import logging
import time
import traceback
import uuid
from typing import Any, Dict, Tuple

import requests

from app.config import settings
from app.redis_client import r
from app.queue import QUEUE_KEY, set_job_state
from app.xray import add_client, remove_client

log = logging.getLogger("xray-agent-worker")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")


# -----------------------------
# Генерация ссылки подключения
# -----------------------------
def build_vless_link(user_uuid: str, email: str, flow: str) -> str:
    """
    Сборка vless:// ссылки из server-side параметров в .env.

    Обязательные переменные окружения:
      PUBLIC_HOST, PUBLIC_PORT
      REALITY_SNI, REALITY_FP, REALITY_PBK, REALITY_SID

    Примечания:
      - flow может быть пустым (если так задумано),
        но обычно используется xtls-rprx-vision.
      - email здесь = telegram_id (строка цифр), используется как тег в #VPN-...
    """
    missing = []
    if not settings.public_host:
        missing.append("PUBLIC_HOST")
    if not settings.reality_sni:
        missing.append("REALITY_SNI")
    if not settings.reality_pbk:
        missing.append("REALITY_PBK")
    if not settings.reality_sid:
        missing.append("REALITY_SID")
    if missing:
        raise RuntimeError(f"Не заданы обязательные env-параметры: {', '.join(missing)}")

    port = int(getattr(settings, "public_port", 443) or 443)
    fp = getattr(settings, "reality_fp", "chrome") or "chrome"

    # flow опционален
    flow_q = f"&flow={flow}" if flow else ""

    return (
        f"vless://{user_uuid}@{settings.public_host}:{port}"
        f"?encryption=none"
        f"{flow_q}"
        f"&security=reality"
        f"&sni={settings.reality_sni}"
        f"&fp={fp}"
        f"&pbk={settings.reality_pbk}"
        f"&sid={settings.reality_sid}"
        f"&type=tcp"
        f"#VPN-{email}"
    )


# -----------------------------
# Отправка результата на notify-сервис (опционально)
# -----------------------------
def _notify_config() -> Tuple[str | None, str | None, int, int]:
    """
    Читает параметры notify из settings (которые берутся из .env).

    NOTIFY_URL - если не задан, notify отключён (skipped)
    NOTIFY_API_KEY - если задан, отправляем как X-API-Key
    NOTIFY_TIMEOUT_SEC - таймаут HTTP запроса
    NOTIFY_RETRIES - количество попыток с экспоненциальным backoff
    """
    notify_url = getattr(settings, "notify_url", None)
    notify_key = getattr(settings, "notify_api_key", None)
    timeout = int(getattr(settings, "notify_timeout_sec", 10))
    retries = int(getattr(settings, "notify_retries", 3))
    return notify_url, notify_key, timeout, retries


def notify_external(payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Отправляет payload на внешний notify сервис.

    Payload (контракт):
      {
        "uuid": "<uuid>",
        "email": "<telegram_id>",
        "inbound_tag": "vless-in",
        "link": "vless://..."
      }

    Поведение:
      - если NOTIFY_URL не задан => пропускаем (skipped)
      - иначе POST на NOTIFY_URL
      - ретраи + backoff
    """
    notify_url, notify_key, timeout, retries = _notify_config()
    if not notify_url:
        return {"skipped": True, "reason": "NOTIFY_URL не задан"}

    headers = {"Content-Type": "application/json"}
    if notify_key:
        headers["X-API-Key"] = notify_key

    last_err: str | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(notify_url, json=payload, headers=headers, timeout=timeout)
            if 200 <= resp.status_code < 300:
                return {"skipped": False, "status_code": resp.status_code}

            # 4xx/5xx
            last_err = f"HTTP {resp.status_code}: {resp.text[:500]}"
        except Exception as e:
            last_err = f"{type(e).__name__}: {e}"

        # экспоненциальная задержка: 1, 2, 4, 8... (макс 8 сек)
        time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(f"Notify не удалось после {retries} попыток: {last_err}")


# -----------------------------
# Парсинг job и валидация полей
# -----------------------------
def _parse_job(raw: Any) -> dict:
    """Декодирует bytes/str и парсит JSON job."""
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="replace")
    return json.loads(raw)


def _require_field(obj: dict, key: str) -> Any:
    """Гарантирует наличие поля в dict, иначе ValueError."""
    if key not in obj:
        raise ValueError(f"Отсутствует обязательное поле '{key}'")
    return obj[key]


# -----------------------------
# Исполнение задач
# -----------------------------
def handle(job: dict) -> dict:
    """
    Выполняет одну задачу.

    Канонический формат job:
      {
        "id": "<job_id>",
        "kind": "issue_client" | "add_client" | "remove_client",
        "payload": {...},
        "ts": <int> (опционально)
      }
    """
    kind = _require_field(job, "kind")
    payload = _require_field(job, "payload")

    if kind == "add_client":
        # legacy: добавить заранее заданного пользователя
        user_uuid = _require_field(payload, "uuid")
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")
        level = int(payload.get("level", 0))
        flow = payload.get("flow", "") or ""
        return add_client(user_uuid, email, inbound_tag, level, flow)

    if kind == "remove_client":
        email = _require_field(payload, "email")
        inbound_tag = _require_field(payload, "inbound_tag")
        return remove_client(email, inbound_tag)

    if kind == "issue_client":
        # новый флоу: UUID генерирует сервер (worker)
        telegram_id = str(_require_field(payload, "telegram_id")).strip()
        inbound_tag = str(payload.get("inbound_tag") or settings.default_inbound_tag)
        level = int(payload.get("level", 0))
        flow = payload.get("flow")
        flow = (flow if flow is not None else settings.default_flow) or ""

        user_uuid = str(uuid.uuid4())

        # 1) Добавить пользователя в Xray
        add_client(user_uuid, telegram_id, inbound_tag, level, flow)

        # 2) Собрать ссылку
        link = build_vless_link(user_uuid, telegram_id, flow)

        issued = {
            "uuid": user_uuid,
            "email": telegram_id,
            "inbound_tag": inbound_tag,
            "link": link,
        }


        # 3) Отправить на notify (если включено)
        notify_info = notify_external(issued)

        # Результат сохраняем в job-state (и можно вытащить через GET /jobs/{job_id})
        return {"issued": issued, "notify": notify_info}

    raise RuntimeError(f"Неизвестный тип задачи kind={kind}")


# -----------------------------
# Главный цикл worker
# -----------------------------
def main():
    log.info("worker запущен, очередь=%s", QUEUE_KEY)

    while True:
        item = r.brpop(QUEUE_KEY, timeout=1)
        if item is None:
            continue

        _, raw = item

        try:
            job = _parse_job(raw)
        except Exception:
            # плохой payload — job_id может отсутствовать, поэтому только логируем
            log.error("невалидный payload в очереди raw=%r err=%s", raw, traceback.format_exc())
            continue

        job_id = job.get("id")
        if not job_id:
            log.error("job без id: %r", job)
            continue

        # помечаем running
        set_job_state(job_id, "running")
        log.info("job running id=%s kind=%s", job_id, job.get("kind"))

        try:
            res = handle(job)
            if not isinstance(res, dict):
                res = {"raw": res}
            set_job_state(job_id, "done", result=res)
            log.info("job done id=%s", job_id)
        except Exception as e:
            err = str(e) + "\n" + traceback.format_exc()
            set_job_state(job_id, "error", error=err)
            log.error("job error id=%s err=%s", job_id, err)


if __name__ == "__main__":
    main()
