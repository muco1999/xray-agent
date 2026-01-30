"""
Xray gRPC adapter (grpcio version).

Вместо grpcurl используем нативный gRPC клиент на Python (grpcio):
- быстрее (без fork/exec)
- надёжнее под нагрузкой
- нормальные таймауты/ошибки gRPC

Reflection не нужен: мы работаем с локально сгенерированными protobuf классами (xrayproto.*)

AlterInbound требует TypedMessage:
- operation: TypedMessage(AddUserOperation|RemoveUserOperation)
- user.account: TypedMessage(xray.proxy.vless.Account)

XRAY_MOCK=true (env) включает локальный режим без реального Xray.
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict, Optional

import grpc
from google.protobuf.json_format import MessageToDict

from app.config import settings
from app.utils import parse_hostport, is_tcp_open

# Protobuf messages
from xrayproto.common.serial import typed_message_pb2
from xrayproto.common.protocol import user_pb2
# from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
from xrayproto.proxy.vless import account_pb2 as vless_account_pb2

# Protobuf gRPC stubs (важно: *_pb2_grpc должны быть в твоей генерации)
from xrayproto.app.proxyman.command import command_pb2_grpc as proxyman_cmd_pb2_grpc

# Stats service proto/stub
from xrayproto.app.stats.command import command_pb2 as stats_cmd_pb2
from xrayproto.app.stats.command import command_pb2_grpc as stats_cmd_pb2_grpc

from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2

XRAY_MOCK = os.getenv("XRAY_MOCK", "").lower() in ("1", "true", "yes")


# -----------------------------
# Helpers: protobuf -> dict
# -----------------------------
from google.protobuf.json_format import MessageToDict

def _pb_to_dict(msg) -> dict:
    """
    Protobuf -> dict с максимальной совместимостью между версиями protobuf.
    """
    kwargs = {
        "preserving_proto_field_name": True,
    }

    # Эти аргументы есть не во всех версиях protobuf/json_format
    for k, v in (
        ("including_default_value_fields", False),
        ("use_integers_for_enums", True),
        ("always_print_fields_with_no_presence", False),
    ):
        try:
            # пробуем передать аргумент — если версия не поддерживает, проигнорируем
            MessageToDict(msg, **{**kwargs, k: v})
            kwargs[k] = v
        except TypeError:
            pass

    return MessageToDict(msg, **kwargs)



# -----------------------------
# TypedMessage builders (без base64 туда-сюда)
# -----------------------------
def _typed_message_bytes(type_name: str, msg_bytes: bytes) -> typed_message_pb2.TypedMessage:
    """
    TypedMessage для Xray (grpcio вариант): напрямую bytes, без base64.
    """
    return typed_message_pb2.TypedMessage(type=type_name, value=msg_bytes)


def _build_vless_account_bytes(uuid: str, flow: str) -> bytes:
    """
    Собираем bytes для xray.proxy.vless.Account.
    """
    acc = vless_account_pb2.Account(id=uuid, flow=flow or "")
    return acc.SerializeToString()


def _build_add_user_operation_typed(uuid: str, email: str, level: int, flow: str) -> typed_message_pb2.TypedMessage:
    """
    Собрать operation = TypedMessage(AddUserOperation) для AlterInboundRequest.operation.
    """
    account_bytes = _build_vless_account_bytes(uuid, flow)
    user = user_pb2.User(
        level=level,
        email=email,
        account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
    )
    op = proxyman_cmd_pb2.AddUserOperation(user=user)
    return _typed_message_bytes("xray.app.proxyman.command.AddUserOperation", op.SerializeToString())


def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
    """
    operation = TypedMessage(RemoveUserOperation) для AlterInboundRequest.operation.
    """
    op = proxyman_cmd_pb2.RemoveUserOperation(email=email)
    return _typed_message_bytes("xray.app.proxyman.command.RemoveUserOperation", op.SerializeToString())


# -----------------------------
# gRPC client (канал + stubs, lazy singleton)
# -----------------------------
_channel: grpc.Channel | None = None
_handler_stub: proxyman_cmd_pb2_grpc.HandlerServiceStub | None = None
_stats_stub: stats_cmd_pb2_grpc.StatsServiceStub | None = None


def _get_channel() -> grpc.Channel:
    """
    Канал создаём один раз на процесс (и переиспользуем).
    """
    global _channel
    if _channel is None:
        # settings.xray_api_addr ожидается как "host:port" (например 127.0.0.1:10085)
        _channel = grpc.insecure_channel(
            settings.xray_api_addr,
            options=[
                ("grpc.keepalive_time_ms", 30_000),
                ("grpc.keepalive_timeout_ms", 10_000),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.keepalive_permit_without_calls", 1),
            ],
        )
    return _channel


def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
    global _handler_stub
    if _handler_stub is None:
        _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(_get_channel())
    return _handler_stub


def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
    global _stats_stub
    if _stats_stub is None:
        _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(_get_channel())
    return _stats_stub


def _rpc_timeout_sec(default: int = 10) -> int:
    try:
        return int(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return default


# -----------------------------
# Public API (same signatures/semantics)
# -----------------------------
def xray_api_sys_stats() -> Dict[str, Any]:
    """
    GetSysStats — удобный health сигнал.

    NOTE: если у тебя в proto request называется иначе — поправь строку ниже.
    Обычно это GetSysStatsRequest().
    """
    if XRAY_MOCK:
        return {"mock": True, "sys_stats": {}}

    stub = _get_stats_stub()

    # В большинстве сборок:
    # req = stats_cmd_pb2.GetSysStatsRequest()
    # resp = stub.GetSysStats(req, timeout=_rpc_timeout_sec())
    req = stats_cmd_pb2.GetSysStatsRequest()  # <-- если имя отличается, поменяй тут
    resp = stub.GetSysStats(req, timeout=_rpc_timeout_sec())

    return _pb_to_dict(resp)


def xray_runtime_status() -> Dict[str, Any]:
    """
    Сводный статус:
    - открыт ли порт Xray gRPC
    - sys stats (если доступно)

    В grpcio варианте grpcurl не нужен, поэтому grpcurl_present заменяем на grpcio_present.
    """
    host, port = parse_hostport(settings.xray_api_addr)
    port_open = is_tcp_open(host, port)

    status: Dict[str, Any] = {
        "xray_api_addr": settings.xray_api_addr,
        "xray_api_port_open": port_open,
        "grpcio_present": True,
        "time": int(time.time()),
    }

    if XRAY_MOCK:
        status["ok"] = True
        status["mock"] = True
        status["xray_api_sys_stats"] = {"mock": True}
        return status

    if not port_open:
        status["ok"] = False
        status["error"] = "Xray API port is not open"
        return status

    try:
        status["xray_api_sys_stats"] = xray_api_sys_stats()
        status["ok"] = True
    except Exception as e:
        status["ok"] = False
        status["xray_api_sys_stats_error"] = str(e)

    return status


def add_client(uuid: str, email: str, inbound_tag: str, level: int = 0, flow: str = "") -> Dict[str, Any]:
    """
    Добавить пользователя в inbound через AlterInbound + AddUserOperation.
    Возвращает dict (как и раньше): для Empty response будет {}.
    """
    if XRAY_MOCK:
        return {
            "mock": True,
            "action": "add",
            "uuid": uuid,
            "email": email,
            "inbound_tag": inbound_tag,
            "level": level,
            "flow": flow,
        }

    op_tm = _build_add_user_operation_typed(uuid=uuid, email=email, level=level, flow=flow)

    # В большинстве сборок:
    # req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
    req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)

    stub = _get_handler_stub()
    resp = stub.AlterInbound(req, timeout=_rpc_timeout_sec())

    # AlterInbound чаще всего возвращает google.protobuf.Empty -> MessageToDict вернёт {}
    # но resp может быть и Empty без полей.
    try:
        return _pb_to_dict(resp)
    except Exception:
        return {}


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    """
    Удалить пользователя из inbound по email.
    Возвращает dict (как и раньше): для Empty response будет {}.
    """
    if XRAY_MOCK:
        return {
            "mock": True,
            "action": "remove",
            "email": email,
            "inbound_tag": inbound_tag,
        }

    op_tm = _build_remove_user_operation_typed(email=email)
    req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)

    stub = _get_handler_stub()
    resp = stub.AlterInbound(req, timeout=_rpc_timeout_sec())
    try:
        return _pb_to_dict(resp)
    except Exception:
        return {}


def inbound_users(tag: str) -> Dict[str, Any]:
    """
    Сырые users inbound (grpcio).
    """
    if XRAY_MOCK:
        return {"users": []}

    stub = _get_handler_stub()

    # Обычно:
    # req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
    req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)  # <-- если имя отличается, поменяй тут
    resp = stub.GetInboundUsers(req, timeout=_rpc_timeout_sec())

    return _pb_to_dict(resp)


def inbound_users_count(tag: str) -> Dict[str, Any]:
    """
    Количество users inbound (grpcio).
    """
    if XRAY_MOCK:
        return {"count": "0"}

    stub = _get_handler_stub()

    # Обычно:
    # req = proxyman_cmd_pb2.GetInboundUsersCountRequest(tag=tag)
    req = proxyman_cmd_pb2.GetInboundUsersCountRequest(tag=tag)  # <-- если имя отличается, поменяй тут
    resp = stub.GetInboundUsersCount(req, timeout=_rpc_timeout_sec())

    return _pb_to_dict(resp)


def inbound_emails(tag: str) -> list[str]:
    """
    Список email пользователей inbound.
    """
    data = inbound_users(tag)
    users = data.get("users") or []
    return [u["email"] for u in users if isinstance(u, dict) and u.get("email")]


def inbound_uuids(tag: str) -> list[str]:
    """
    Достаём UUID из TypedMessage VLESS Account.

    user.account: {type: "...", value: "<base64>"}
    value -> bytes -> vless_account_pb2.Account -> .id

    В grpcio-ответе account.value часто приходит как base64-строка (через MessageToDict),
    поэтому оставляем прежнюю логику декодирования base64.
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
            # если попался невалидный value — просто пропускаем
            continue

    return out
