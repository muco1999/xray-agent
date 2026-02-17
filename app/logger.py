# app/logger.py
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from logging.handlers import TimedRotatingFileHandler
from typing import Any, Dict, Tuple

# ----------------------------
# Базовые уровни логирования
# ----------------------------
logging.getLogger().setLevel(logging.INFO)

logging.getLogger("vpn_bot").setLevel(logging.DEBUG)
logging.getLogger("asyncssh").setLevel(logging.ERROR)
logging.getLogger("aiogram").setLevel(logging.WARNING)
logging.getLogger("sqlalchemy").setLevel(logging.WARNING)


# ===============================
# Цветной форматтер для консоли
# ===============================
class ColorFormatter(logging.Formatter):
    COLORS = {
        "DEBUG": "\033[94m",
        "INFO": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "CRITICAL": "\033[95m",
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        original_levelname = record.levelname
        color = self.COLORS.get(original_levelname, self.COLORS["RESET"])
        reset = self.COLORS["RESET"]

        record.levelname = f"{color}{original_levelname}{reset}"
        try:
            return super().format(record)
        finally:
            record.levelname = original_levelname


# ===============================
# Основной логгер
# ===============================
class MyLogger:
    """
    Совместим со стандартным logging.Logger + поддерживает "структурные kwargs":

    - log.info("hello")
    - log.info("x=%s y=%s", x, y)
    - log.exception("failed %s", err)
    - log.info("...", extra={...})

    Дополнительно (важно):
    - log.info("...", user_id=123, path="/var/log/xray/access.log")
      => НЕ падает, а добавляет контекст в сообщение + кладёт в extra.
    """

    # ключи, которые stdlib logging реально понимает как kwargs
    _STD_KWARGS = {"exc_info", "stack_info", "stacklevel", "extra"}

    # зарезервированные атрибуты LogRecord — нельзя класть в extra под такими ключами
    _RESERVED_RECORD_ATTRS = {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "message", "asctime",
    }

    def __init__(self, name: str, retention_days: int = 10):
        self.name = name
        self.retention_days = retention_days

        # --- куда писать логи ---
        # 1) Если задан LOG_DIR — используем его (удобно для Docker/systemd)
        # 2) Иначе рядом с этим файлом: app/logs
        env_dir = os.getenv("LOG_DIR")
        if env_dir:
            self.log_dir = Path(env_dir).expanduser().resolve()
        else:
            base_dir = Path(__file__).resolve().parent
            self.log_dir = base_dir / "logs"

        # Гарантируем создание каталогов
        self.log_dir.mkdir(parents=True, exist_ok=True)

        # Ранний фейл с понятным текстом (вместо PermissionError глубоко в logging)
        if not os.access(self.log_dir, os.W_OK):
            raise PermissionError(f"Нет прав на запись в каталог логов: {self.log_dir}")

        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)

        file_formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        console_formatter = ColorFormatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Чтобы не плодить хендлеры при повторных импортах
        if not getattr(self.logger, "_configured_by_mylogger", False):
            # ----------------------------
            # FILE: ротация в полночь + retention_days
            # ----------------------------
            # Файл будет вида: vpn_bot.log, а ротация создаст vpn_bot.log.2026-02-09 и т.п.
            log_file = self.log_dir / f"{self.name}.log"

            file_handler = TimedRotatingFileHandler(
                filename=str(log_file),
                when="midnight",
                interval=1,
                backupCount=self.retention_days,
                encoding="utf-8",
                utc=False,
                delay=True,  # файл откроется при первом логе (меньше проблем при старте)
            )
            file_handler.setLevel(logging.DEBUG)
            file_handler.setFormatter(file_formatter)

            # ----------------------------
            # CONSOLE
            # ----------------------------
            console_handler = logging.StreamHandler(stream=sys.stdout)
            console_handler.setLevel(logging.DEBUG)
            console_handler.setFormatter(console_formatter)

            self.logger.addHandler(file_handler)
            self.logger.addHandler(console_handler)

            # Не дублировать через root logger
            self.logger.propagate = False

            # Маркер конфигурации
            self.logger._configured_by_mylogger = True  # type: ignore[attr-defined]

            # Небольшая метка старта (полезно при дебаге)
            self.logger.debug("Logger initialized | log_dir=%s | pid=%s", self.log_dir, os.getpid())

    # ---------------------------
    # Internal helpers
    # ---------------------------
    def _split_kwargs(self, kwargs: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Делит kwargs на:
          - std_kwargs: то, что понимает stdlib logging (extra/exc_info/stack_info/stacklevel)
          - ctx: произвольные структурные поля (user_id, path, server_id, ...)
        """
        std_kwargs: Dict[str, Any] = {}
        ctx: Dict[str, Any] = {}

        for k, v in kwargs.items():
            if k in self._STD_KWARGS:
                std_kwargs[k] = v
            else:
                ctx[k] = v
        return std_kwargs, ctx

    def _sanitize_extra(self, ctx: Dict[str, Any]) -> Dict[str, Any]:
        """
        Делает ctx безопасным для LogRecord.extra:
        - не затирает зарезервированные имена
        - не кладёт ключи, начинающиеся с '_'
        """
        safe: Dict[str, Any] = {}
        for k, v in ctx.items():
            key = str(k)
            if key.startswith("_") or key in self._RESERVED_RECORD_ATTRS:
                key = f"ctx_{key}"
            safe[key] = v
        return safe

    @staticmethod
    def _ctx_to_suffix(ctx: Dict[str, Any]) -> str:
        """
        Человекочитаемая приписка к сообщению.
        Важно: это гарантирует видимость контекста даже если formatter не выводит extra.
        """
        if not ctx:
            return ""
        parts = [f"{k}={v!r}" for k, v in ctx.items()]
        return " | " + " ".join(parts)

    def _log_with_ctx(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        std_kwargs, ctx = self._split_kwargs(kwargs)

        # merge extra
        extra = dict(std_kwargs.get("extra") or {})
        if ctx:
            extra.update(self._sanitize_extra(ctx))
            std_kwargs["extra"] = extra

        # чтобы контекст был виден всегда — дописываем в message
        if ctx:
            msg = f"{msg}{self._ctx_to_suffix(ctx)}"

        self.logger.log(level, msg, *args, **std_kwargs)

    # ---------------------------
    # Стандартные методы Logger
    # ---------------------------
    def debug(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(logging.WARNING, msg, *args, **kwargs)

    def error(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg: str, *args: Any, **kwargs: Any) -> None:
        kwargs.setdefault("exc_info", True)
        self._log_with_ctx(logging.ERROR, msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: Any, **kwargs: Any) -> None:
        self._log_with_ctx(level, msg, *args, **kwargs)


# ===============================
# Экземпляр
# ===============================
log = MyLogger("vpn_bot")