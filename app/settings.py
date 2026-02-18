# app/settings.py
from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from aiogram import Bot
from aiogram.client.default import DefaultBotProperties

from app.logger import log


class XraySettings(BaseSettings):
    """
    ЕДИНСТВЕННЫЙ источник конфигурации для xray-agent.

    Загружается из:
      1) переменных окружения
      2) .env (если существует)

    Все имена env-переменных задаются через alias=...
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # -----------------------------------------------------------------------------
    # 🧩 Telegram bot (уведомления пользователю)
    # -----------------------------------------------------------------------------
    bot_token: str = Field(default="", alias="BOT_TOKEN")
    """
    Токен бота, через которого агент отправляет пользователю:
      - предупреждение (WARN)
      - уведомление о бане (BAN)
      - благодарность, если исправился (THANKS)
    """

    # -----------------------------------------------------------------------------
    # 🧠 Redis (состояние guard’а)
    # -----------------------------------------------------------------------------
    redis_url: str = Field(default="redis://127.0.0.1:6379/0", alias="REDIS_URL")
    """
    Redis нужен ТОЛЬКО для временного состояния:
      - warned_at (когда предупреждали)
      - дедупликация сообщений (allow_once locks)
      - cooldown на бан/спасибо
    """

    # -----------------------------------------------------------------------------
    # 🧰 Xray gRPC API (реальное удаление клиента)
    # -----------------------------------------------------------------------------
    xray_api_addr: str = Field(default="127.0.0.1:10085", alias="XRAY_API_ADDR")
    """
    Адрес gRPC API Xray (обычно localhost:10085).
    Используется функцией remove_client(email, inbound_tag).
    """

    default_inbound_tag: str = Field(default="vless-in", alias="XRAY_INBOUND_TAG")
    """
    inbound_tag, в котором живут клиенты.
    Если у тебя несколько inbound’ов — можно расширить до списка, но сейчас 1 достаточно.
    """

    proto_root: str = Field(default="/srv/proto", alias="XRAY_PROTO_ROOT")
    """
    Путь до proto-файлов для grpc клиента (если remove_client использует proto from disk).
    """

    # -----------------------------------------------------------------------------
    # 📄 Access log (источник данных для проверки устройств)
    # -----------------------------------------------------------------------------
    access_log_path: str = Field(default="/var/log/xray/access.log", alias="XRAY_ACCESS_LOG")
    """
    Файл access.log Xray, который парсит build_xray_status_snapshot().
    """

    tail_max_lines: int = Field(default=30000, alias="TAIL_MAX_LINES")
    """
    Сколько последних строк читать из access.log на один проход.
    Чем больше — тем точнее оценка devices_estimate, но больше CPU/IO.
    """

    window_sec: int = Field(default=10 * 60, alias="WINDOW_SEC")
    """
    Окно анализа (сек): какие записи из лога считаем “актуальными” для подсчёта устройств.
    Например 600 сек = 10 минут.
    """

    online_window_sec: int = Field(default=240, alias="ONLINE_WINDOW_SEC")
    """
    Окно для “онлайна” (сек): за сколько секунд считать клиента online.
    Используется для online_* метрик (если ты их показываешь).
    """

    ip_active_ttl_sec: int = Field(default=120, alias="IP_ACTIVE_TTL_SEC")
    """
    TTL активности IP (сек): помогает точнее считать "устройство" по уникальным IP.
    """

    cache_ttl_sec: float = Field(default=2.0, alias="CACHE_TTL_SEC")
    """
    TTL кэша snapshot’а (сек), если у тебя включён кэш в status endpoint/парсере.
    """

    # -----------------------------------------------------------------------------
    # 🛡️ Guard: частота и правила (WARN → GRACE → BAN → THANKS)
    # -----------------------------------------------------------------------------
    interval_sec: int = Field(default=20, alias="XRAY_GUARD_INTERVAL_SEC")
    """
    Период запуска проверки (сек). Например 10–30 сек.
    """

    devices_limit: int = Field(default=2, alias="XRAY_GUARD_DEVICES_LIMIT")
    """
    Лимит устройств на клиента.
    Считается по devices_estimate (из access.log).
    """

    ban_grace_sec: int = Field(default=15 * 60, alias="XRAY_GUARD_BAN_GRACE_SEC")
    """
    Сколько даём времени после WARN, чтобы пользователь отключил лишние устройства.
    Если не исправился → BAN (remove_client).
    """

    active_seen_sec: int = Field(default=600, alias="XRAY_GUARD_ACTIVE_SEEN_SEC")
    """
    Защита от “хвостов окна”:
    баним/предупреждаем ТОЛЬКО если last_seen_ago_sec <= active_seen_sec.
    Иначе клиент не активен и можно словить ложный бан.
    """

    warn_cooldown_sec: int = Field(default=300, alias="XRAY_GUARD_WARN_COOLDOWN_SEC")
    """
    Анти-спам WARN: не отправлять предупреждение чаще, чем раз в N секунд.
    """

    disable_cooldown_sec: int = Field(default=1800, alias="XRAY_GUARD_DISABLE_COOLDOWN_SEC")
    """
    Анти-повтор BAN: если BAN уже сработал, не пытаться повторять в течение N секунд.
    """

    reset_warn_cooldown_on_resolve: bool = Field(default=False, alias="XRAY_GUARD_RESET_WARN_ON_RESOLVE")
    """
    Если True — когда пользователь исправился, можно сбросить warn-cooldown lock,
    чтобы следующее нарушение снова сразу предупреждало (на твой выбор).
    """

    notify_timeout_sec: int = Field(default=10, alias="NOTIFY_TIMEOUT_SEC")
    """
    Таймаут на отправку Telegram сообщения (сек).
    """

    # -----------------------------------------------------------------------------
    # 🔐 (опционально) FastAPI эндпоинты (если агент отдаёт status наружу)
    # -----------------------------------------------------------------------------
    api_token: str = Field(default="CHANGE_ME", alias="API_TOKEN")
    """
    Токен для защиты эндпоинтов (если ты их оставляешь).
    Если эндпоинты не нужны — можешь игнорировать.
    """

    port: int = Field(default=18000, alias="PORT")
    """
    Порт FastAPI сервиса агента (если сервис поднимается).
    """

    status_path: str = Field(default="/xray/status/clients", alias="XRAY_GUARD_STATUS_PATH")
    """
    Путь эндпоинта статуса (если ты его оставляешь).
    На guard loop не влияет, потому что guard читает логи напрямую.
    """

    endpoints_token: str = Field(default="", alias="API_TOKEN_ENDPOINDS")
    """
    Токен для эндпоинтов (если отдельно используешь).
    Обычно лучше оставить один api_token и не дублировать.
    """

    grpc_timeout_sec: int = Field(default=30, alias="grpc_timeout_sec")
    notify_total_timeout_sec: int = Field(default=30, alias="notify_total_timeout_sec")

    # -----------------------------------------------------------------------------
    # 🌐 (необязательно) Параметры генерации VLESS ссылки (если агент этим занимается)
    # -----------------------------------------------------------------------------
    public_host: str | None = Field(default=None, alias="PUBLIC_HOST")
    public_port: int = Field(default=443, alias="PUBLIC_PORT")

    reality_sni: str | None = Field(default=None, alias="REALITY_SNI")
    reality_fp: str = Field(default="chrome", alias="REALITY_FP")
    reality_pbk: str | None = Field(default=None, alias="REALITY_PBK")
    reality_sid: str | None = Field(default=None, alias="REALITY_SID")

    default_flow: str = Field(default="", alias="DEFAULT_FLOW")


# ----------------------------------------------------------------------
# singleton settings + telegram bot
# ----------------------------------------------------------------------
settings = XraySettings()



try:

    if settings.bot_token:
        bot = Bot(
            token=settings.bot_token,
            default=DefaultBotProperties(parse_mode="HTML"),
        )
    else:
        raise Exception
except Exception:
    log.error("No Token bot")
    pass
