"""
Асинхронный адаптер для работы с локальным Xray Runtime API по gRPC.

Назначение:
- Проверка состояния локального Xray через GetSysStats
- Добавление/удаление пользователей в inbound через AlterInbound
- Получение списка пользователей inbound
- Получение количества пользователей inbound
- Утилиты для получения email/UUID пользователей

Ключевая архитектурная идея:
- Этот модуль должен работать ИЗ Docker-контейнера с Xray,
  который запущен НА ХОСТОВОЙ МАШИНЕ.
- Для этого контейнер должен запускаться с network_mode: host,
  а XRAY_API_ADDR должен быть вида 127.0.0.1:10085.

Если контейнер НЕ запущен с network_mode: host, то 127.0.0.1
внутри контейнера будет указывать на сам контейнер, а не на хост.
В таком случае health/full будет ложно считать Xray недоступным.

Особенности:
- Полностью асинхронный grpc.aio
- Самовосстанавливающийся gRPC-канал
- Подробное логирование на каждом шаге
- Поддержка mock-режима через XRAY_MOCK=true

Готово для использования в FastAPI / aiogram / worker-процессах.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Awaitable, Callable, Dict, Optional, TypeVar

import grpc
from google.protobuf.json_format import MessageToDict
from grpc import StatusCode

from app.settings import settings
from app.utils import is_tcp_open, parse_hostport

from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
from xrayproto.app.proxyman.command import command_pb2_grpc as proxyman_cmd_pb2_grpc
from xrayproto.app.stats.command import command_pb2 as stats_cmd_pb2
from xrayproto.app.stats.command import command_pb2_grpc as stats_cmd_pb2_grpc
from xrayproto.common.protocol import user_pb2
from xrayproto.common.serial import typed_message_pb2
from xrayproto.proxy.vless import account_pb2 as vless_account_pb2


# =============================================================================
# Базовые настройки и логгер
# =============================================================================

XRAY_MOCK = os.getenv("XRAY_MOCK", "").strip().lower() in {"1", "true", "yes", "on"}
log = logging.getLogger("xray-agent.xray")

T = TypeVar("T")


# =============================================================================
# Исключения
# =============================================================================

class AlreadyExistsError(Exception):
    """Пользователь уже существует в inbound."""


# =============================================================================
# Конфигурация
# =============================================================================

def _xray_addr() -> str:
    """
    Возвращает адрес Xray gRPC API.

    Ожидаемый production-формат для host-local Xray:
        127.0.0.1:10085
    """
    value = str(getattr(settings, "xray_api_addr", "") or "").strip()
    if not value:
        raise RuntimeError("Не задан settings.xray_api_addr")
    return value


def _rpc_timeout_sec(default: float = 10.0) -> float:
    """Таймаут одного gRPC RPC."""
    try:
        return float(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return float(default)


def _connect_ready_timeout_sec(default: float = 2.0) -> float:
    """Таймаут ожидания готовности gRPC-канала."""
    try:
        return float(getattr(settings, "xray_connect_ready_timeout_sec", default))
    except Exception:
        return float(default)


def _grpc_inflight_limit(default: int = 40) -> int:
    """Ограничение числа одновременных gRPC-запросов."""
    try:
        return int(getattr(settings, "xray_grpc_inflight_limit", default))
    except Exception:
        return int(default)


def _grpc_call_timeout_sec(default: float = 4.0) -> float:
    """Общий таймаут обёртки _rpc()."""
    try:
        return float(getattr(settings, "xray_grpc_timeout_sec", default))
    except Exception:
        return float(default)


def _grpc_channel_options() -> list[tuple[str, int]]:
    """
    Настройки keepalive для grpc.aio.
    Подобраны умеренно консервативно, чтобы снизить риск залипания
    и не словить too_many_pings / ENHANCE_YOUR_CALM.
    """
    return [
        ("grpc.keepalive_permit_without_calls", 0),
        ("grpc.keepalive_time_ms", 120_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.http2.max_pings_without_data", 1),
        ("grpc.http2.min_time_between_pings_ms", 60_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 120_000),
    ]


# =============================================================================
# Вспомогательные функции логирования
# =============================================================================

def _mask(value: str, keep: int = 3) -> str:
    """
    Маскирует чувствительные данные для логов.
    Пример:
        abcdefgh -> abc***fgh
    """
    if not value:
        return ""
    value = str(value)
    if len(value) <= keep * 2:
        return "*" * len(value)
    return f"{value[:keep]}***{value[-keep:]}"


def _grpc_err_info(exc: grpc.RpcError) -> Dict[str, Any]:
    """
    Безопасно достаёт код и details из grpc-исключения.
    """
    code = None
    details = None

    try:
        code = exc.code()  # type: ignore[attr-defined]
    except Exception:
        pass

    try:
        details = exc.details()  # type: ignore[attr-defined]
    except Exception:
        details = str(exc)

    return {
        "code": str(code),
        "details": str(details)[:500],
    }


@asynccontextmanager
async def _rpc_log_ctx(op: str, **fields: Any):
    """
    Контекст структурного логирования RPC:
    - start
    - ok
    - fail
    - latency
    """
    started = time.perf_counter()
    payload = {"op": op, **fields}
    log.info("xray rpc start %s", json.dumps(payload, ensure_ascii=False))

    try:
        yield payload
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        log.info(
            "xray rpc ok %s",
            json.dumps({**payload, "ms": elapsed_ms}, ensure_ascii=False),
        )
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
        log.error(
            "xray rpc fail %s",
            json.dumps(
                {
                    **payload,
                    "ms": elapsed_ms,
                    "exc": type(exc).__name__,
                    "msg": str(exc)[:500],
                },
                ensure_ascii=False,
            ),
        )
        raise


# =============================================================================
# Вспомогательные функции protobuf
# =============================================================================

def _pb_to_dict(message: Any) -> dict:
    """
    Универсальное преобразование protobuf -> dict с совместимостью
    между разными версиями protobuf.
    """
    kwargs: dict[str, Any] = {"preserving_proto_field_name": True}

    for key, value in (
        ("including_default_value_fields", False),
        ("use_integers_for_enums", True),
        ("always_print_fields_with_no_presence", False),
    ):
        try:
            MessageToDict(message, **{**kwargs, key: value})
            kwargs[key] = value
        except TypeError:
            pass

    return MessageToDict(message, **kwargs)


def _typed_message_bytes(type_name: str, raw: bytes) -> typed_message_pb2.TypedMessage:
    """
    Собирает TypedMessage:
    - type = полное имя protobuf-типа
    - value = сериализованные bytes
    """
    return typed_message_pb2.TypedMessage(type=type_name, value=raw)


def _build_vless_account_bytes(user_uuid: str, flow: str) -> bytes:
    """
    Собирает xray.proxy.vless.Account и возвращает bytes.
    """
    account = vless_account_pb2.Account(id=user_uuid, flow=flow or "")
    return account.SerializeToString()


def _build_add_user_operation_typed(
    user_uuid: str,
    email: str,
    level: int,
    flow: str,
) -> typed_message_pb2.TypedMessage:
    """
    Собирает TypedMessage для AddUserOperation.
    """
    account_bytes = _build_vless_account_bytes(user_uuid=user_uuid, flow=flow)

    user = user_pb2.User(
        level=int(level),
        email=str(email),
        account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
    )

    operation = proxyman_cmd_pb2.AddUserOperation(user=user)
    return _typed_message_bytes(
        "xray.app.proxyman.command.AddUserOperation",
        operation.SerializeToString(),
    )


def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
    """
    Собирает TypedMessage для RemoveUserOperation.
    """
    operation = proxyman_cmd_pb2.RemoveUserOperation(email=str(email))
    return _typed_message_bytes(
        "xray.app.proxyman.command.RemoveUserOperation",
        operation.SerializeToString(),
    )


# =============================================================================
# Диагностика окружения
# =============================================================================

def _looks_like_loopback_host(host: str) -> bool:
    """Проверяет, что адрес указывает на loopback."""
    host = (host or "").strip().lower()
    return host in {"127.0.0.1", "localhost", "::1"}


def _running_inside_docker() -> bool:
    """
    Best-effort проверка, что код исполняется в контейнере Docker.
    """
    if os.path.exists("/.dockerenv"):
        return True

    try:
        with open("/proc/1/cgroup", "r", encoding="utf-8", errors="ignore") as fh:
            content = fh.read().lower()
        return "docker" in content or "containerd" in content or "kubepods" in content
    except Exception:
        return False


def _log_runtime_network_diagnostics() -> None:
    """
    Логирует важную диагностику по сетевой модели.
    """
    try:
        addr = _xray_addr()
        host, port = parse_hostport(addr)
        inside_docker = _running_inside_docker()
        loopback = _looks_like_loopback_host(host)

        log.info(
            "xray runtime network diagnostics | addr=%s | host=%s | port=%s | inside_docker=%s | loopback=%s",
            addr,
            host,
            port,
            inside_docker,
            loopback,
        )

        if inside_docker and loopback:
            log.info(
                "xray runtime использует loopback-адрес внутри контейнера. "
                "Это корректно ТОЛЬКО если контейнер запущен с network_mode=host."
            )
    except Exception as exc:
        log.warning("Не удалось выполнить сетевую диагностику Xray runtime: %s", str(exc)[:300])


# =============================================================================
# gRPC канал / stubs / синхронизация
# =============================================================================

_channel: Optional[grpc.aio.Channel] = None
_handler_stub: Optional[proxyman_cmd_pb2_grpc.HandlerServiceStub] = None
_stats_stub: Optional[stats_cmd_pb2_grpc.StatsServiceStub] = None

_channel_lock = asyncio.Lock()
_grpc_sem = asyncio.Semaphore(_grpc_inflight_limit())


def _create_channel() -> grpc.aio.Channel:
    """
    Создаёт новый grpc.aio insecure channel.
    """
    addr = _xray_addr()
    channel = grpc.aio.insecure_channel(addr, options=_grpc_channel_options())
    log.info("Создан новый gRPC-канал к Xray | addr=%s", addr)
    return channel


async def _close_channel_locked() -> None:
    """
    Закрывает текущий канал.
    Вызывать только под _channel_lock.
    """
    global _channel

    if _channel is None:
        return

    channel = _channel
    _channel = None

    try:
        await channel.close()
        log.info("gRPC-канал Xray закрыт")
    except Exception as exc:
        log.debug("Ошибка при закрытии gRPC-канала Xray: %s", str(exc)[:300])


def _reset_stubs_locked() -> None:
    """
    Сбрасывает кэшированные stubs.
    Вызывать только под _channel_lock.
    """
    global _handler_stub, _stats_stub
    _handler_stub = None
    _stats_stub = None
    log.info("Сброшены gRPC stubs Xray")


async def _get_or_create_channel_locked() -> grpc.aio.Channel:
    """
    Возвращает текущий канал либо создаёт новый.
    Вызывать только под _channel_lock.
    """
    global _channel
    if _channel is None:
        _channel = _create_channel()
    return _channel


async def _await_channel_ready(channel: grpc.aio.Channel, timeout: float) -> None:
    """
    Универсальное ожидание готовности grpc.aio channel.
    """
    if hasattr(channel, "channel_ready"):
        await asyncio.wait_for(channel.channel_ready(), timeout=timeout)  # type: ignore[attr-defined]
        return

    if hasattr(grpc.aio, "channel_ready_future"):
        future = grpc.aio.channel_ready_future(channel)  # type: ignore[attr-defined]
        await asyncio.wait_for(future, timeout=timeout)
        return

    raise RuntimeError("В grpc.aio недоступен API ожидания готовности канала")


async def _ensure_channel_ready() -> None:
    """
    Гарантирует, что gRPC-канал готов.

    Логика:
    - attempt #1: пробуем существующий/новый канал
    - при проблеме сбрасываем stubs, закрываем канал
    - attempt #2: создаём новый канал и ждём повторно
    """
    if XRAY_MOCK:
        return

    ready_timeout = _connect_ready_timeout_sec()
    addr = _xray_addr()

    async with _channel_lock:
        channel = await _get_or_create_channel_locked()

        try:
            await _await_channel_ready(channel, timeout=ready_timeout)
            log.debug("gRPC-канал готов | addr=%s", addr)
            return
        except Exception as exc:
            log.warning(
                "gRPC-канал не готов, attempt=1 | addr=%s | timeout=%.2f | err=%s",
                addr,
                ready_timeout,
                str(exc)[:300],
            )
            _reset_stubs_locked()
            await _close_channel_locked()

        channel = await _get_or_create_channel_locked()

        try:
            await _await_channel_ready(channel, timeout=ready_timeout)
            log.info("gRPC-канал восстановлен после пересоздания | addr=%s", addr)
        except Exception as exc:
            log.error(
                "Не удалось подготовить gRPC-канал Xray, attempt=2 | addr=%s | timeout=%.2f | err=%s",
                addr,
                ready_timeout,
                str(exc)[:300],
            )
            raise


async def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
    """
    Возвращает HandlerServiceStub.
    """
    global _handler_stub

    async with _channel_lock:
        if _handler_stub is None:
            channel = await _get_or_create_channel_locked()
            _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(channel)
            log.info("Создан HandlerServiceStub | addr=%s", _xray_addr())
        return _handler_stub


async def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
    """
    Возвращает StatsServiceStub.
    """
    global _stats_stub

    async with _channel_lock:
        if _stats_stub is None:
            channel = await _get_or_create_channel_locked()
            _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(channel)
            log.info("Создан StatsServiceStub | addr=%s", _xray_addr())
        return _stats_stub


def _raise_grpc_error(exc: grpc.RpcError, *, context: str) -> None:
    """
    Преобразует grpc.RpcError в RuntimeError с нормальным текстом.
    """
    info = _grpc_err_info(exc)
    raise RuntimeError(
        f"gRPC ошибка Xray ({context}): code={info['code']} details={info['details']}"
    ) from exc


# =============================================================================
# Общая RPC-обёртка
# =============================================================================

async def _rpc(
    make_coro: Callable[[], Awaitable[T]],
    *,
    op: str,
    addr: str,
    timeout: Optional[float] = None,
) -> T:
    """
    Общая production-обёртка для gRPC-вызовов.

    Что делает:
    - ограничивает параллелизм через semaphore
    - оборачивает вызов в asyncio.wait_for()
    - логирует start/ok/error/timeout
    """
    started = time.perf_counter()
    effective_timeout = _grpc_call_timeout_sec() if timeout is None else float(timeout)

    async with _grpc_sem:
        log.info(
            "Начало gRPC RPC | op=%s | addr=%s | timeout=%.2f",
            op,
            addr,
            effective_timeout,
        )

        try:
            result = await asyncio.wait_for(make_coro(), timeout=effective_timeout)
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            log.info(
                "Успешный gRPC RPC | op=%s | addr=%s | ms=%.2f",
                op,
                addr,
                elapsed_ms,
            )
            return result

        except asyncio.TimeoutError:
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            log.warning(
                "Таймаут gRPC RPC | op=%s | addr=%s | ms=%.2f",
                op,
                addr,
                elapsed_ms,
            )
            raise

        except asyncio.CancelledError:
            log.warning("gRPC RPC отменён | op=%s | addr=%s", op, addr)
            raise

        except Exception:
            elapsed_ms = round((time.perf_counter() - started) * 1000, 2)
            log.exception(
                "Ошибка gRPC RPC | op=%s | addr=%s | ms=%.2f",
                op,
                addr,
                elapsed_ms,
            )
            raise


# =============================================================================
# Public API: системная health-проверка
# =============================================================================

async def xray_api_sys_stats() -> Dict[str, Any] | None:
    """
    Получает системную статистику Xray через StatsService.GetSysStats.
    """
    if XRAY_MOCK:
        return {"mock": True, "sys_stats": {}}

    addr = _xray_addr()

    try:
        async with _rpc_log_ctx("GetSysStats", addr=addr):
            await _ensure_channel_ready()
            stub = await _get_stats_stub()
            request = stats_cmd_pb2.SysStatsRequest()

            response = await _rpc(
                lambda: stub.GetSysStats(request, timeout=_rpc_timeout_sec()),
                op="GetSysStats",
                addr=addr,
                timeout=_rpc_timeout_sec() + 0.5,
            )

            data = _pb_to_dict(response)

            log.info(
                "Получен ответ GetSysStats | addr=%s | keys=%s",
                addr,
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

    except grpc.RpcError as exc:
        log.error("gRPC ошибка в GetSysStats | addr=%s | info=%s", addr, _grpc_err_info(exc))
        _raise_grpc_error(exc, context="GetSysStats")
        return None


async def xray_runtime_status() -> Dict[str, Any]:
    """
    Возвращает полное runtime-состояние Xray для health-check endpoint.

    Логика:
    1. Разбираем адрес Xray API
    2. Проверяем, открыт ли TCP-порт
    3. Если порт открыт — вызываем GetSysStats
    4. Возвращаем нормализованный словарь

    Результат:
    {
        "ok": bool,
        "xray_api_addr": "...",
        "xray_api_port_open": bool,
        "grpcio_present": True,
        "time": unix_ts,
        "xray_api_sys_stats": {...}      # при успехе
        "error": "..."                   # при проблеме
        "xray_api_sys_stats_error": "..."# при gRPC-проблеме
    }
    """
    addr = _xray_addr()
    host, port = parse_hostport(addr)

    _log_runtime_network_diagnostics()

    port_open = is_tcp_open(host, port)

    status: Dict[str, Any] = {
        "ok": False,
        "xray_api_addr": addr,
        "xray_api_host": host,
        "xray_api_port": port,
        "xray_api_port_open": port_open,
        "grpcio_present": True,
        "time": int(time.time()),
    }

    if XRAY_MOCK:
        status["ok"] = True
        status["mock"] = True
        status["xray_api_sys_stats"] = {"mock": True}
        log.info("XRAY_MOCK=true: возвращён mock runtime status")
        return status

    if not port_open:
        status["error"] = (
            "Порт Xray API недоступен. "
            "Если приложение работает в Docker, а Xray на хосте, "
            "нужно использовать network_mode=host и XRAY_API_ADDR=127.0.0.1:PORT."
        )
        log.warning(
            "Порт Xray API закрыт | addr=%s | host=%s | port=%s",
            addr,
            host,
            port,
        )
        return status

    try:
        sys_stats = await xray_api_sys_stats()
        status["xray_api_sys_stats"] = sys_stats
        status["ok"] = True

        log.info(
            "Xray runtime status успешен | addr=%s | port_open=%s",
            addr,
            port_open,
        )
        return status

    except Exception as exc:
        status["xray_api_sys_stats_error"] = str(exc)[:500]
        log.warning(
            "Xray runtime status: GetSysStats завершился ошибкой | addr=%s | err=%s",
            addr,
            str(exc)[:500],
        )
        return status


# =============================================================================
# Классификация gRPC ошибок
# =============================================================================

def _is_user_not_found(exc: grpc.RpcError) -> bool:
    """
    Best-effort классификация ошибки "user not found".
    """
    try:
        details = (exc.details() or "").lower()  # type: ignore[attr-defined]
    except Exception:
        details = str(exc).lower()

    return "not found" in details and "user" in details


def _grpc_is_already_exists(exc: grpc.RpcError) -> bool:
    """
    Best-effort классификация ошибки "already exists".
    """
    try:
        code = exc.code()  # type: ignore[attr-defined]
    except Exception:
        code = None

    if code == StatusCode.ALREADY_EXISTS:
        return True

    try:
        details = (exc.details() or "").lower()  # type: ignore[attr-defined]
    except Exception:
        details = str(exc).lower()

    return (
        ("already" in details and "exist" in details)
        or ("duplicate" in details)
        or ("duplicated" in details)
    )


# =============================================================================
# Public API: работа с inbound users
# =============================================================================

async def remove_client(email: str, inbound_tag: str) -> Dict[str, Any] | None:
    """
    Удаляет пользователя из inbound по email.
    """
    if XRAY_MOCK:
        return {
            "mock": True,
            "action": "remove",
            "email": email,
            "inbound_tag": inbound_tag,
        }

    addr = _xray_addr()

    try:
        async with _rpc_log_ctx(
            "AlterInbound(RemoveUser)",
            addr=addr,
            inbound_tag=inbound_tag,
            email=_mask(email),
        ):
            await _ensure_channel_ready()

            operation = _build_remove_user_operation_typed(email=email)
            request = proxyman_cmd_pb2.AlterInboundRequest(
                tag=inbound_tag,
                operation=operation,
            )

            stub = await _get_handler_stub()

            response = await _rpc(
                lambda: stub.AlterInbound(request, timeout=_rpc_timeout_sec()),
                op="AlterInbound(RemoveUser)",
                addr=addr,
                timeout=_rpc_timeout_sec() + 0.5,
            )

            try:
                data = _pb_to_dict(response)
            except Exception as exc:
                log.warning(
                    "Не удалось преобразовать protobuf ответа RemoveUser в dict | addr=%s | tag=%s | email=%s | err=%s",
                    addr,
                    inbound_tag,
                    _mask(email),
                    str(exc)[:300],
                )
                return {}

            log.info(
                "Пользователь удалён из inbound | addr=%s | tag=%s | email=%s | resp_keys=%s",
                addr,
                inbound_tag,
                _mask(email),
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

    except grpc.RpcError as exc:
        if _is_user_not_found(exc):
            log.info(
                "Удаление пропущено: пользователь не найден | addr=%s | tag=%s | email=%s",
                addr,
                inbound_tag,
                _mask(email),
            )
            return {
                "ok": True,
                "skipped": True,
                "reason": "user not found",
                "email": str(email),
                "inbound_tag": str(inbound_tag),
            }

        log.error(
            "gRPC ошибка RemoveUser | addr=%s | tag=%s | email=%s | info=%s",
            addr,
            inbound_tag,
            _mask(email),
            _grpc_err_info(exc),
        )
        _raise_grpc_error(exc, context=f"AlterInbound(RemoveUser) tag={inbound_tag} email={email}")
        return None


async def inbound_users(tag: str) -> Dict[str, Any] | None:
    """
    Получает список пользователей inbound.

    Поддерживаются два варианта protobuf API:
    - GetInboundUsersRequest(tag=...)
    - fallback: GetInboundUserRequest(tag=..., email="")
    """
    if XRAY_MOCK:
        return {"users": []}

    addr = _xray_addr()

    try:
        async with _rpc_log_ctx("GetInboundUsers", addr=addr, inbound_tag=tag):
            await _ensure_channel_ready()
            stub = await _get_handler_stub()

            if hasattr(proxyman_cmd_pb2, "GetInboundUsersRequest"):
                request = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
                log.info("Используется GetInboundUsersRequest | addr=%s | tag=%s", addr, tag)
            else:
                request = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
                log.info("Используется fallback GetInboundUserRequest | addr=%s | tag=%s", addr, tag)

            response = await _rpc(
                lambda: stub.GetInboundUsers(request, timeout=_rpc_timeout_sec()),
                op="GetInboundUsers",
                addr=addr,
                timeout=_rpc_timeout_sec() + 0.5,
            )

            data = _pb_to_dict(response)
            users = data.get("users") if isinstance(data, dict) else None
            users_count = len(users) if isinstance(users, list) else None

            log.info(
                "Получен список пользователей inbound | addr=%s | tag=%s | users_count=%s",
                addr,
                tag,
                users_count,
            )
            return data

    except grpc.RpcError as exc:
        log.error("gRPC ошибка GetInboundUsers | addr=%s | tag=%s | info=%s", addr, tag, _grpc_err_info(exc))
        _raise_grpc_error(exc, context=f"GetInboundUsers tag={tag}")
        return None


async def inbound_users_count(tag: str) -> int | None:
    """
    Возвращает количество пользователей inbound.

    Поддерживаются разные версии protobuf API.
    """
    if XRAY_MOCK:
        return 0

    addr = _xray_addr()

    try:
        async with _rpc_log_ctx("GetInboundUsersCount", addr=addr, inbound_tag=tag):
            await _ensure_channel_ready()
            stub = await _get_handler_stub()

            if hasattr(proxyman_cmd_pb2, "GetInboundUsersCountRequest"):
                request = proxyman_cmd_pb2.GetInboundUsersCountRequest(tag=tag)
                log.info("Используется GetInboundUsersCountRequest | addr=%s | tag=%s", addr, tag)

                response = await _rpc(
                    lambda: stub.GetInboundUsersCount(request, timeout=_rpc_timeout_sec()),
                    op="GetInboundUsersCount",
                    addr=addr,
                    timeout=_rpc_timeout_sec() + 0.5,
                )

                data = _pb_to_dict(response)
                raw_count = data.get("count", 0)
                try:
                    parsed_count = int(raw_count)
                except Exception:
                    parsed_count = 0

                log.info(
                    "Получено количество пользователей inbound через GetInboundUsersCount | addr=%s | tag=%s | raw=%r | parsed=%d",
                    addr,
                    tag,
                    raw_count,
                    parsed_count,
                )
                return parsed_count

            # fallback: получаем список и считаем длину
            log.info(
                "GetInboundUsersCountRequest недоступен, используем fallback через GetInboundUsers | addr=%s | tag=%s",
                addr,
                tag,
            )
            data = await inbound_users(tag)
            users = (data or {}).get("users") or []
            count = len(users) if isinstance(users, list) else 0

            log.info(
                "Количество пользователей inbound вычислено через fallback | addr=%s | tag=%s | count=%d",
                addr,
                tag,
                count,
            )
            return count

    except grpc.RpcError as exc:
        log.error("gRPC ошибка GetInboundUsersCount | addr=%s | tag=%s | info=%s", addr, tag, _grpc_err_info(exc))
        _raise_grpc_error(exc, context=f"GetInboundUsersCount tag={tag}")
        return None


async def inbound_emails(tag: str) -> list[str]:
    """
    Возвращает список email пользователей inbound.
    """
    data = await inbound_users(tag)
    users = (data or {}).get("users") or []

    emails: list[str] = []

    for item in users:
        if isinstance(item, dict) and item.get("email"):
            emails.append(str(item["email"]))

    log.info(
        "Получены email пользователей inbound | addr=%s | tag=%s | count=%d",
        _xray_addr(),
        tag,
        len(emails),
    )
    return emails


async def inbound_uuids(tag: str) -> list[str]:
    """
    Возвращает список UUID пользователей inbound.

    UUID извлекается из TypedMessage VLESS Account.
    """
    data = await inbound_users(tag)
    users = (data or {}).get("users") or []

    uuids: list[str] = []

    for item in users:
        if not isinstance(item, dict):
            continue

        account = item.get("account") or {}
        if account.get("type") != "xray.proxy.vless.Account":
            continue

        encoded_value = account.get("value")
        if not encoded_value:
            continue

        try:
            raw = base64.b64decode(encoded_value)
            parsed_account = vless_account_pb2.Account()
            parsed_account.ParseFromString(raw)

            if getattr(parsed_account, "id", ""):
                uuids.append(parsed_account.id)
        except Exception:
            continue

    log.info(
        "Получены UUID пользователей inbound | addr=%s | tag=%s | count=%d",
        _xray_addr(),
        tag,
        len(uuids),
    )
    return uuids


async def add_client(
    user_uuid: str,
    email: str,
    inbound_tag: str,
    level: int = 0,
    flow: str = "",
) -> Dict[str, Any]:
    """
    Добавляет пользователя в inbound.
    """
    if XRAY_MOCK:
        return {
            "mock": True,
            "action": "add",
            "uuid": user_uuid,
            "email": email,
            "inbound_tag": inbound_tag,
            "level": level,
            "flow": flow,
        }

    addr = _xray_addr()
    effective_flow = flow or "xtls-rprx-vision"

    try:
        async with _rpc_log_ctx(
            "AlterInbound(AddUser)",
            addr=addr,
            inbound_tag=inbound_tag,
            email=_mask(email),
            uuid=_mask(user_uuid),
            level=int(level),
            flow=effective_flow,
        ):
            await _ensure_channel_ready()

            operation = _build_add_user_operation_typed(
                user_uuid=user_uuid,
                email=email,
                level=int(level),
                flow=effective_flow,
            )
            request = proxyman_cmd_pb2.AlterInboundRequest(
                tag=inbound_tag,
                operation=operation,
            )

            stub = await _get_handler_stub()

            response = await _rpc(
                lambda: stub.AlterInbound(request, timeout=_rpc_timeout_sec()),
                op="AlterInbound(AddUser)",
                addr=addr,
                timeout=_rpc_timeout_sec() + 0.5,
            )

            try:
                data = _pb_to_dict(response)
            except Exception as exc:
                log.warning(
                    "Не удалось преобразовать protobuf ответа AddUser в dict | addr=%s | tag=%s | email=%s | err=%s",
                    addr,
                    inbound_tag,
                    _mask(email),
                    str(exc)[:300],
                )
                return {}

            log.info(
                "Пользователь успешно добавлен в inbound | addr=%s | tag=%s | email=%s | resp_keys=%s",
                addr,
                inbound_tag,
                _mask(email),
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

    except grpc.RpcError as exc:
        if _grpc_is_already_exists(exc):
            log.debug(
                "Пользователь уже существует в inbound | addr=%s | tag=%s | email=%s | info=%s",
                addr,
                inbound_tag,
                _mask(email),
                _grpc_err_info(exc),
            )
            raise AlreadyExistsError(f"user already exists: tag={inbound_tag} email={email}") from exc

        log.error(
            "gRPC ошибка AddUser | addr=%s | tag=%s | email=%s | info=%s",
            addr,
            inbound_tag,
            _mask(email),
            _grpc_err_info(exc),
        )
        _raise_grpc_error(exc, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")
        return {}


