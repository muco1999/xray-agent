"""
Xray gRPC адаптер (grpcio).

Назначение:
- Проверка состояния Xray (через GetSysStats)
- Управление пользователями inbound (AlterInbound: AddUserOperation/RemoveUserOperation)
- Получение пользователей inbound (GetInboundUsers / GetInboundUsersCount)

Важное:
- Модуль СИНХРОННЫЙ (grpcio sync). В async-коде вызывать через threadpool/to_thread.
- XRAY_MOCK=true включает локальный режим без реального Xray.
- Реализован самовосстанавливающийся gRPC-канал:
  иногда grpcio "залипает" на сломанном соединении/после рестартов Xray.
  Если канал не готов, мы пересоздаём channel/stubs и пробуем снова.
  Это предотвращает вечное состояние job="running" в worker.
"""

from __future__ import annotations

import base64
import os
import threading
import time
from typing import Any, Dict, Tuple

import grpc
from google.protobuf.json_format import MessageToDict

from app.config import settings
from app.utils import parse_hostport, is_tcp_open

# Protobuf: TypedMessage / User / VLESS Account
from xrayproto.common.serial import typed_message_pb2
from xrayproto.common.protocol import user_pb2
from xrayproto.proxy.vless import account_pb2 as vless_account_pb2

# Proxyman/Handler service (AlterInbound, GetInboundUsers...)
from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
from xrayproto.app.proxyman.command import command_pb2_grpc as proxyman_cmd_pb2_grpc

# Stats service (GetSysStats)
from xrayproto.app.stats.command import command_pb2 as stats_cmd_pb2
from xrayproto.app.stats.command import command_pb2_grpc as stats_cmd_pb2_grpc


XRAY_MOCK = os.getenv("XRAY_MOCK", "").lower() in ("1", "true", "yes")


# =============================================================================
# Вспомогательное: protobuf -> dict (совместимость версий protobuf)
# =============================================================================
def _pb_to_dict(msg) -> dict:
    """
    Преобразование protobuf-сообщения в dict.

    Параметры MessageToDict отличаются между версиями protobuf, поэтому
    пробуем включить опции аккуратно.
    """
    kwargs = {"preserving_proto_field_name": True}
    for k, v in (
        ("including_default_value_fields", False),
        ("use_integers_for_enums", True),
        ("always_print_fields_with_no_presence", False),
    ):
        try:
            MessageToDict(msg, **{**kwargs, k: v})
            kwargs[k] = v
        except TypeError:
            # Опция не поддерживается текущей версией protobuf
            pass
    return MessageToDict(msg, **kwargs)


# =============================================================================
# TypedMessage builders (AlterInbound)
# =============================================================================
def _typed_message_bytes(type_name: str, msg_bytes: bytes) -> typed_message_pb2.TypedMessage:
    """
    TypedMessage: type=<полное имя proto>, value=<bytes SerializeToString>.
    """
    return typed_message_pb2.TypedMessage(type=type_name, value=msg_bytes)


def _build_vless_account_bytes(user_uuid: str, flow: str) -> bytes:
    """
    Собираем xray.proxy.vless.Account (bytes).
    """
    acc = vless_account_pb2.Account(id=user_uuid, flow=flow or "")
    return acc.SerializeToString()


def _build_add_user_operation_typed(user_uuid: str, email: str, level: int, flow: str) -> typed_message_pb2.TypedMessage:
    """
    Собираем TypedMessage для AddUserOperation.
    """
    account_bytes = _build_vless_account_bytes(user_uuid, flow)

    user = user_pb2.User(
        level=int(level),
        email=str(email),
        account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
    )

    op = proxyman_cmd_pb2.AddUserOperation(user=user)
    return _typed_message_bytes("xray.app.proxyman.command.AddUserOperation", op.SerializeToString())


def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
    """
    Собираем TypedMessage для RemoveUserOperation.
    """
    op = proxyman_cmd_pb2.RemoveUserOperation(email=str(email))
    return _typed_message_bytes("xray.app.proxyman.command.RemoveUserOperation", op.SerializeToString())


# =============================================================================
# gRPC: канал и stubs (самовосстановление)
# =============================================================================
_channel: grpc.Channel | None = None
_handler_stub: proxyman_cmd_pb2_grpc.HandlerServiceStub | None = None
_stats_stub: stats_cmd_pb2_grpc.StatsServiceStub | None = None

_lock = threading.Lock()


def _rpc_timeout_sec(default: int = 10) -> int:
    """
    Таймаут именно RPC-вызова (server-side + network).
    """
    try:
        return int(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return default


def _connect_ready_timeout_sec(default: float = 2.0) -> float:
    """
    Таймаут ожидания "канал готов" (channel_ready_future).
    Это отдельная защита от зависаний grpcio при проблемном соединении.
    """
    try:
        return float(getattr(settings, "xray_connect_ready_timeout_sec", default))
    except Exception:
        return default


def _grpc_channel_options() -> list[tuple[str, int]]:
    """
    Настройки keepalive, чтобы избежать ENHANCE_YOUR_CALM / too_many_pings.
    """
    return [
        ("grpc.keepalive_permit_without_calls", 0),
        ("grpc.keepalive_time_ms", 120_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.http2.max_pings_without_data", 1),
        ("grpc.http2.min_time_between_pings_ms", 60_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 120_000),
    ]


def _reset_channel_locked() -> None:
    """
    Сброс channel и stub-ов (вызывать только под _lock).
    """
    global _channel, _handler_stub, _stats_stub
    _channel = None
    _handler_stub = None
    _stats_stub = None


def _get_or_create_channel_locked() -> grpc.Channel:
    """
    Получить или создать gRPC channel (вызывать только под _lock).
    """
    global _channel
    if _channel is None:
        _channel = grpc.insecure_channel(settings.xray_api_addr, options=_grpc_channel_options())
    return _channel


def _ensure_channel_ready() -> None:
    """
    Гарантирует, что gRPC канал готов.

    Главная причина "вечного running":
    grpcio может "залипнуть" на полуживом соединении (особенно после рестарта Xray).
    В этом случае мы:
      1) ждём готовность канала короткое время
      2) если не готов — пересоздаём channel/stubs
      3) ждём ещё раз

    Если и после пересоздания не готов — выбрасываем исключение.
    """
    if XRAY_MOCK:
        return

    ready_timeout = _connect_ready_timeout_sec()

    with _lock:
        ch = _get_or_create_channel_locked()

        # Попытка №1
        try:
            grpc.channel_ready_future(ch).result(timeout=ready_timeout)
            return
        except Exception:
            # Пересоздаём и пробуем ещё раз
            _reset_channel_locked()
            ch = _get_or_create_channel_locked()

        # Попытка №2 (после reset)
        grpc.channel_ready_future(ch).result(timeout=ready_timeout)


def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
    """
    HandlerServiceStub (AlterInbound, GetInboundUsers...).
    """
    global _handler_stub
    with _lock:
        if _handler_stub is None:
            _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(_get_or_create_channel_locked())
        return _handler_stub


def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
    """
    StatsServiceStub (GetSysStats).
    """
    global _stats_stub
    with _lock:
        if _stats_stub is None:
            _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(_get_or_create_channel_locked())
        return _stats_stub


def _raise_grpc_error(e: grpc.RpcError, *, context: str) -> None:
    """
    Нормализуем gRPC ошибку в RuntimeError с контекстом.
    """
    code = e.code() if hasattr(e, "code") else None
    details = e.details() if hasattr(e, "details") else str(e)
    raise RuntimeError(f"grpcio failed ({context}): code={code} details={details}") from e


# =============================================================================
# Public API
# =============================================================================
def xray_api_sys_stats() -> Dict[str, Any]:
    """
    GetSysStats — главный health-сигнал.
    """
    if XRAY_MOCK:
        return {"mock": True, "sys_stats": {}}

    try:
        _ensure_channel_ready()
        stub = _get_stats_stub()
        req = stats_cmd_pb2.SysStatsRequest()
        resp = stub.GetSysStats(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context="GetSysStats")


def xray_runtime_status() -> Dict[str, Any]:
    """
    Быстрый статус Xray:
    - открывается ли TCP порт API
    - получается ли GetSysStats
    """
    host, port = parse_hostport(settings.xray_api_addr)
    port_open = is_tcp_open(host, port)

    st: Dict[str, Any] = {
        "xray_api_addr": settings.xray_api_addr,
        "xray_api_port_open": port_open,
        "grpcio_present": True,
        "time": int(time.time()),
    }

    if XRAY_MOCK:
        st["ok"] = True
        st["mock"] = True
        st["xray_api_sys_stats"] = {"mock": True}
        return st

    if not port_open:
        st["ok"] = False
        st["error"] = "Xray API port is not open"
        return st

    try:
        st["xray_api_sys_stats"] = xray_api_sys_stats()
        st["ok"] = True
    except Exception as e:
        st["ok"] = False
        # тут можно отдавать str(e) (для дебага), а можно скрывать
        st["xray_api_sys_stats_error"] = str(e)

    return st


def add_client(user_uuid: str, email: str, inbound_tag: str, level: int = 0, flow: str = "") -> Dict[str, Any]:
    """
    Добавить пользователя в inbound через AlterInbound + AddUserOperation.

    Совместимость: как раньше, возвращаем dict (_pb_to_dict(resp)) или {}.
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

    try:
        _ensure_channel_ready()
        op_tm = _build_add_user_operation_typed(user_uuid=user_uuid, email=email, level=int(level), flow=flow)
        req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
        resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
        try:
            return _pb_to_dict(resp)  # часто {}
        except Exception:
            return {}
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    """
    Удалить пользователя из inbound по email.
    """
    if XRAY_MOCK:
        return {"mock": True, "action": "remove", "email": email, "inbound_tag": inbound_tag}

    try:
        _ensure_channel_ready()
        op_tm = _build_remove_user_operation_typed(email=email)
        req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
        resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
        try:
            return _pb_to_dict(resp)
        except Exception:
            return {}
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"AlterInbound(RemoveUser) tag={inbound_tag} email={email}")


def inbound_users(tag: str) -> Dict[str, Any]:
    """
    Получить список пользователей inbound (сырые данные).
    По твоему proto используется GetInboundUserRequest(tag=..., email="") для list.
    """
    if XRAY_MOCK:
        return {"users": []}

    try:
        _ensure_channel_ready()
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsers(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsers tag={tag}")


def inbound_users_count(tag: str) -> int:
    """
    Количество пользователей inbound.
    Возвращаем int (нормализовано), чтобы API всегда отдавал число.
    """
    if XRAY_MOCK:
        return 0

    try:
        _ensure_channel_ready()
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsersCount(req, timeout=_rpc_timeout_sec())
        data: Dict[str, Any] = _pb_to_dict(resp)
        raw = data.get("count", 0)

        # protobuf/json_format может вернуть str или int — нормализуем
        try:
            return int(raw)
        except Exception:
            return 0
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsersCount tag={tag}")


def inbound_emails(tag: str) -> list[str]:
    """
    Утилита: вытянуть emails из inbound_users.
    """
    data = inbound_users(tag)
    users = data.get("users") or []
    out: list[str] = []
    for u in users:
        if isinstance(u, dict) and u.get("email"):
            out.append(str(u["email"]))
    return out


def inbound_uuids(tag: str) -> list[str]:
    """
    Утилита: вытянуть UUID из TypedMessage VLESS Account.
    MessageToDict обычно отдаёт TypedMessage.value как base64 строку.
    """
    data = inbound_users(tag)
    users = data.get("users") or []
    out: list[str] = []

    for u in users:
        if not isinstance(u, dict):
            continue

        acc = u.get("account") or {}
        if acc.get("type") != "xray.proxy.vless.Account":
            continue

        b64 = acc.get("value")
        if not b64:
            continue

        try:
            raw = base64.b64decode(b64)
            vless_acc = vless_account_pb2.Account()
            vless_acc.ParseFromString(raw)
            if getattr(vless_acc, "id", ""):
                out.append(vless_acc.id)
        except Exception:
            continue

    return out
