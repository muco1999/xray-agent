"""
Xray gRPC adapter (grpcio version).

Мы используем нативный gRPC клиент на Python (grpcio):
- быстрее, чем grpcurl (нет fork/exec)
- стабильнее под нагрузкой
- нормальные таймауты и gRPC ошибки

Reflection не нужен: работаем с локально сгенерированными protobuf классами (xrayproto.*)

AlterInbound требует TypedMessage:
- operation: TypedMessage(AddUserOperation|RemoveUserOperation)
- user.account: TypedMessage(xray.proxy.vless.Account)

ВАЖНО по твоим proto:
- RPC методы HandlerService.GetInboundUsers / GetInboundUsersCount в *_pb2_grpc.py
  используют request_serializer = GetInboundUserRequest.SerializeToString.
  Поэтому мы создаём GetInboundUserRequest(tag=..., email="") для list/count.

XRAY_MOCK=true (env) включает локальный режим без реального Xray.
"""

from __future__ import annotations

import base64
import os
import time
from typing import Any, Dict

import grpc
from google.protobuf.json_format import MessageToDict

from app.config import settings
from app.utils import parse_hostport, is_tcp_open

# Protobuf messages
from xrayproto.common.serial import typed_message_pb2
from xrayproto.common.protocol import user_pb2
from xrayproto.proxy.vless import account_pb2 as vless_account_pb2

# Proxyman/Handler service
from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
from xrayproto.app.proxyman.command import command_pb2_grpc as proxyman_cmd_pb2_grpc

# Stats service
from xrayproto.app.stats.command import command_pb2 as stats_cmd_pb2
from xrayproto.app.stats.command import command_pb2_grpc as stats_cmd_pb2_grpc


XRAY_MOCK = os.getenv("XRAY_MOCK", "").lower() in ("1", "true", "yes")


# -----------------------------
# Protobuf -> dict (совместимость protobuf/json_format)
# -----------------------------
def _pb_to_dict(msg) -> dict:
    """
    Protobuf -> dict с совместимостью между версиями protobuf.
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
            pass
    return MessageToDict(msg, **kwargs)


# -----------------------------
# TypedMessage builders (grpcio: bytes напрямую)
# -----------------------------
def _typed_message_bytes(type_name: str, msg_bytes: bytes) -> typed_message_pb2.TypedMessage:
    return typed_message_pb2.TypedMessage(type=type_name, value=msg_bytes)


def _build_vless_account_bytes(uuid: str, flow: str) -> bytes:
    acc = vless_account_pb2.Account(id=uuid, flow=flow or "")
    return acc.SerializeToString()


def _build_add_user_operation_typed(uuid: str, email: str, level: int, flow: str) -> typed_message_pb2.TypedMessage:
    account_bytes = _build_vless_account_bytes(uuid, flow)

    user = user_pb2.User(
        level=level,
        email=email,
        account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
    )

    op = proxyman_cmd_pb2.AddUserOperation(user=user)
    return _typed_message_bytes("xray.app.proxyman.command.AddUserOperation", op.SerializeToString())


def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
    op = proxyman_cmd_pb2.RemoveUserOperation(email=email)
    return _typed_message_bytes("xray.app.proxyman.command.RemoveUserOperation", op.SerializeToString())


# -----------------------------
# gRPC client (канал + stubs, singleton на процесс)
# -----------------------------
_channel: grpc.Channel | None = None
_handler_stub: proxyman_cmd_pb2_grpc.HandlerServiceStub | None = None
_stats_stub: stats_cmd_pb2_grpc.StatsServiceStub | None = None


def _rpc_timeout_sec(default: int = 10) -> int:
    try:
        return int(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return default


def _grpc_channel_options() -> list[tuple[str, int]]:
    """
    Анти-ENHANCE_YOUR_CALM "too_many_pings":
    - не шлём keepalive без вызовов
    - большие интервалы
    """
    return [
        ("grpc.keepalive_permit_without_calls", 0),
        ("grpc.keepalive_time_ms", 120_000),
        ("grpc.keepalive_timeout_ms", 10_000),
        ("grpc.http2.max_pings_without_data", 1),
        ("grpc.http2.min_time_between_pings_ms", 60_000),
        ("grpc.http2.min_ping_interval_without_data_ms", 120_000),
    ]


def _get_channel() -> grpc.Channel:
    global _channel
    if _channel is None:
        _channel = grpc.insecure_channel(settings.xray_api_addr, options=_grpc_channel_options())
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


def _raise_grpc_error(e: grpc.RpcError, *, context: str) -> None:
    code = e.code() if hasattr(e, "code") else None
    details = e.details() if hasattr(e, "details") else str(e)
    raise RuntimeError(f"grpcio failed ({context}): code={code} details={details}") from e


# -----------------------------
# Public API
# -----------------------------
def xray_api_sys_stats() -> Dict[str, Any]:
    """
    GetSysStats — health сигнал.

    В твоей версии proto request = SysStatsRequest (подтверждено).
    """
    if XRAY_MOCK:
        return {"mock": True, "sys_stats": {}}

    try:
        stub = _get_stats_stub()
        req = stats_cmd_pb2.SysStatsRequest()
        resp = stub.GetSysStats(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context="GetSysStats")


def xray_runtime_status() -> Dict[str, Any]:
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
    """Добавить пользователя в inbound через AlterInbound + AddUserOperation."""
    if XRAY_MOCK:
        return {"mock": True, "action": "add", "uuid": uuid, "email": email, "inbound_tag": inbound_tag, "level": level, "flow": flow}

    try:
        op_tm = _build_add_user_operation_typed(uuid=uuid, email=email, level=level, flow=flow)
        req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
        resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
        try:
            return _pb_to_dict(resp)  # часто {}
        except Exception:
            return {}
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    """Удалить пользователя из inbound по email."""
    if XRAY_MOCK:
        return {"mock": True, "action": "remove", "email": email, "inbound_tag": inbound_tag}

    try:
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
    Сырые users inbound.

    ВАЖНО: по твоему command_pb2_grpc.py GetInboundUsers принимает GetInboundUserRequest.
    Используем email="" как "list" запрос.
    """
    if XRAY_MOCK:
        return {"users": []}

    try:
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsers(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsers tag={tag}")


def inbound_users_count(tag: str) -> Dict[str, Any]:
    """
    Количество users inbound.

    ВАЖНО: по твоему command_pb2_grpc.py GetInboundUsersCount принимает GetInboundUserRequest.
    Используем email="".
    """
    if XRAY_MOCK:
        return {"count": "0"}

    try:
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsersCount(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsersCount tag={tag}")


def inbound_emails(tag: str) -> list[str]:
    data = inbound_users(tag)
    users = data.get("users") or []
    out: list[str] = []
    for u in users:
        if isinstance(u, dict) and u.get("email"):
            out.append(u["email"])
    return out


def inbound_uuids(tag: str) -> list[str]:
    """
    UUID из TypedMessage VLESS Account.
    MessageToDict обычно отдаёт TypedMessage.value как base64 string.
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
