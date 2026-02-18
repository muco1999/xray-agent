# /xray-agent/app/workers/xray_guard/main.py
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Optional

from app.andpoints.endpoints_status_xray_clients import build_xray_status_snapshot
from app.logger import log
from app.settings import settings, bot  # ожидается: settings + aiogram.Bot
from app.utils import format_minutes
from app.workers.xray_guard.analyzer import extract_violations
from app.workers.xray_guard.queue import GuardRedis
from app.xray import remove_client

# ✅ grpc.aio adapter (async)

try:
    from app.queue import clear_issue_dedupe_cache  # если есть в агенте
except Exception:  # pragma: no cover
    clear_issue_dedupe_cache = None


@dataclass(frozen=True)
class GuardConfig:
    inbound_tag: str
    devices_limit: int
    ban_grace_sec: int
    warn_cooldown_sec: int
    disable_cooldown_sec: int
    active_seen_sec: int
    interval_sec: int
    notify_timeout_sec: float


def _as_tg_id(email: str) -> Optional[int]:
    try:
        return int(str(email).strip())
    except Exception:
        return None


async def _send(tg_id: int, text: str, timeout_sec: float) -> None:
    try:
        await asyncio.wait_for(bot.send_message(tg_id, text, parse_mode="HTML"), timeout=timeout_sec)
    except Exception:
        return None


async def guard_once(cfg: GuardConfig, gr: GuardRedis) -> None:
    now = int(time.time())
    snap = await build_xray_status_snapshot()

    violations = extract_violations(snap, cfg.devices_limit, active_seen_sec=cfg.active_seen_sec)

    # map: email -> Violation
    viol_map = {v.email: v for v in violations}

    # 1) WARN/BAN for violators
    for v in violations:
        email = v.email
        tg_id = _as_tg_id(email)

        keys = gr.keys(cfg.inbound_tag, email)
        warned_at_s = await gr.get(keys.warned_at)

        # WARN
        if warned_at_s is None:
            if await gr.allow_once(keys.warn, cfg.warn_cooldown_sec):
                ttl = max(cfg.warn_cooldown_sec, cfg.ban_grace_sec + cfg.active_seen_sec + 30)
                await gr.setex(keys.warned_at, ttl, str(now))

                if tg_id:
                    await _send(
                        tg_id,
                        (
                            "⚠️ <b>Обнаружено превышение устройств</b>\n\n"
                            f"🔒 Лимит: <b>{cfg.devices_limit}</b>\n"
                            f"📱 Сейчас: <b>{v.devices}</b>\n\n"
                            f"⏳ Исправьте в течение <b>{format_minutes(cfg.ban_grace_sec)}</b>\n"
                            "Иначе профиль будет отключён автоматически."
                        ),
                        cfg.notify_timeout_sec,
                    )
            continue

        # BAN after grace
        try:
            warned_at = int(warned_at_s)
        except Exception:
            warned_at = now

        # защита от “старого warned_at” (хвосты окна)
        if now - warned_at > (cfg.ban_grace_sec + cfg.active_seen_sec + 60):
            await gr.delete(keys.warned_at)
            continue

        if now - warned_at < cfg.ban_grace_sec:
            continue

        if await gr.allow_once(keys.ban, cfg.disable_cooldown_sec):
            log.warning(
                "[XRAY_GUARD] BAN remove_client",
                extra={"email": email, "tag": cfg.inbound_tag, "devices": v.devices, "limit": cfg.devices_limit},
            )

            # ✅ remove_client — теперь async grpc.aio
            try:
                await remove_client(email=email, inbound_tag=cfg.inbound_tag)
            except Exception:
                log.exception("remove_client failed", extra={"email": email, "tag": cfg.inbound_tag})
                continue

            # очистка dedupe/issue-cache если есть
            if clear_issue_dedupe_cache is not None:
                try:
                    await clear_issue_dedupe_cache(telegram_id=email, inbound_tag=cfg.inbound_tag)
                except Exception:
                    pass

            await gr.delete(keys.warned_at)

            if tg_id:
                await _send(
                    tg_id,
                    (
                        "⛔ <b>Профиль отключён</b>\n\n"
                        "Нарушение (несколько устройств) не было устранено после предупреждения.\n"
                        "Отключите лишние устройства и получите доступ заново через бота."
                    ),
                    cfg.notify_timeout_sec,
                )

    # 2) THANKS for users who fixed (были warned_at, но уже не нарушают)
    clients = snap.get("clients") or []
    for row in clients:
        if not isinstance(row, dict):
            continue
        email = str(row.get("email") or "").strip()
        if not email:
            continue

        if email in viol_map:
            continue  # всё ещё нарушает

        keys = gr.keys(cfg.inbound_tag, email)
        warned_at_s = await gr.get(keys.warned_at)
        if warned_at_s is None:
            continue

        # исправился
        await gr.delete(keys.warned_at)

        tg_id = _as_tg_id(email)
        if tg_id and await gr.allow_once(keys.thanks, 1800):
            await _send(
                tg_id,
                (
                    "✅ <b>Спасибо! Нарушение устранено</b>\n\n"
                    "Сейчас подключений в норме. Продолжайте пользоваться VPN."
                ),
                cfg.notify_timeout_sec,
            )


async def guard_loop(cfg: GuardConfig) -> None:
    gr = GuardRedis()
    while True:
        try:
            await guard_once(cfg, gr)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("guard_once failed")
        await asyncio.sleep(cfg.interval_sec)


def _cfg_from_settings() -> GuardConfig:
    return GuardConfig(
        inbound_tag=str(settings.default_inbound_tag),
        devices_limit=int(settings.devices_limit),
        ban_grace_sec=int(settings.ban_grace_sec),
        warn_cooldown_sec=int(settings.warn_cooldown_sec),
        disable_cooldown_sec=int(settings.disable_cooldown_sec),
        active_seen_sec=int(settings.active_seen_sec),
        interval_sec=int(settings.interval_sec),
        notify_timeout_sec=float(settings.notify_timeout_sec),
    )


async def main() -> None:
    cfg = _cfg_from_settings()
    await guard_loop(cfg)


if __name__ == "__main__":
    asyncio.run(main())