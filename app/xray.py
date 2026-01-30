"""
Xray gRPC adapter (grpcio version) — production hardened.

- sync grpcio client (fast, stable)
- typed message operations for AlterInbound
- no reflection; uses generated xrayproto.*
- XRAY_MOCK=true enables mock mode

Important:
- This module is sync. Call from async code via threadpool/to_thread.
"""

from __future__ import annotations

import base64
import os
import threading
import time
from typing import Any, Dict, Optional, Tuple

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
# Errors
# -----------------------------
class XrayGrpcError(RuntimeError):
    """Structured gRPC error for upstream handling."""

    def __init__(self, *, context: str, grpc_code: Optional[str], grpc_details: str):
        super().__init__(f"grpcio failed ({context}): code={grpc_code} details={grpc_details}")
        self.context = context
        self.grpc_code = grpc_code
        self.grpc_details = grpc_details


def _raise_grpc_error(e: grpc.RpcError, *, context: str) -> None:
    code_obj = e.code() if hasattr(e, "code") else None
    grpc_code = None
    try:
        grpc_code = code_obj.name if code_obj else None
    except Exception:
        grpc_code = str(code_obj) if code_obj else None

    details = e.details() if hasattr(e, "details") else str(e)
    raise XrayGrpcError(context=context, grpc_code=grpc_code, grpc_details=str(details)) from e


# -----------------------------
# Protobuf -> dict (kwargs cached once)
# -----------------------------
_SUPPORTED_PB_KWARGS: Dict[str, Any] = {"preserving_proto_field_name": True}


def _init_pb_kwargs() -> Dict[str, Any]:
    base = {"preserving_proto_field_name": True}
    # use any protobuf message to probe supported kwargs
    probe = stats_cmd_pb2.SysStatsRequest()

    candidates: Tuple[Tuple[str, Any], ...] = (
        ("including_default_value_fields", False),
        ("use_integers_for_enums", True),
        ("always_print_fields_with_no_presence", False),
    )

    for k, v in candidates:
        try:
            MessageToDict(probe, **{**base, k: v})
            base[k] = v
        except TypeError:
            # older protobuf does not support it
            pass

    return base


_SUPPORTED_PB_KWARGS = _init_pb_kwargs()


def _pb_to_dict(msg) -> dict:
    return MessageToDict(msg, **_SUPPORTED_PB_KWARGS)


# -----------------------------
# TypedMessage builders
# -----------------------------
def _typed_message_bytes(type_name: str, msg_bytes: bytes) -> typed_message_pb2.TypedMessage:
    return typed_message_pb2.TypedMessage(type=type_name, value=msg_bytes)


def _build_vless_account_bytes(user_uuid: str, flow: str) -> bytes:
    acc = vless_account_pb2.Account(id=user_uuid, flow=flow or "")
    return acc.SerializeToString()


def _build_add_user_operation_typed(user_uuid: str, email: str, level: int, flow: str) -> typed_message_pb2.TypedMessage:
    account_bytes = _build_vless_account_bytes(user_uuid, flow)

    user = user_pb2.User(
        level=int(level),
        email=str(email),
        account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
    )

    op = proxyman_cmd_pb2.AddUserOperation(user=user)
    return _typed_message_bytes("xray.app.proxyman.command.AddUserOperation", op.SerializeToString())


def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
    op = proxyman_cmd_pb2.RemoveUserOperation(email=str(email))
    return _typed_message_bytes("xray.app.proxyman.command.RemoveUserOperation", op.SerializeToString())


# -----------------------------
# gRPC client (singleton per process, thread-safe init)
# -----------------------------
_channel: grpc.Channel | None = None
_handler_stub: proxyman_cmd_pb2_grpc.HandlerServiceStub | None = None
_stats_stub: stats_cmd_pb2_grpc.StatsServiceStub | None = None

_init_lock = threading.Lock()


def _rpc_timeout_sec(default: int = 10) -> int:
    try:
        return int(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return default


def _grpc_channel_options() -> list[tuple[str, int]]:
    """
    Anti-ENHANCE_YOUR_CALM "too_many_pings".
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
        with _init_lock:
            if _channel is None:
                _channel = grpc.insecure_channel(settings.xray_api_addr, options=_grpc_channel_options())
    return _channel


def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
    global _handler_stub
    if _handler_stub is None:
        with _init_lock:
            if _handler_stub is None:
                _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(_get_channel())
    return _handler_stub


def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
    global _stats_stub
    if _stats_stub is None:
        with _init_lock:
            if _stats_stub is None:
                _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(_get_channel())
    return _stats_stub


# -----------------------------
# Public API
# -----------------------------
def xray_api_sys_stats() -> Dict[str, Any]:
    """GetSysStats — health сигнал."""
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
        st["xray_api_sys_stats_error"] = type(e).__name__  # не протекаем строкой наружу
    return st


def add_client(user_uuid: str, email: str, inbound_tag: str, level: int = 0, flow: str = "") -> Dict[str, Any]:
    """Add user to inbound via AlterInbound + AddUserOperation."""
    if XRAY_MOCK:
        return {
            "mock": True,
            "action": "add",
            "uuid": user_uuid,
            "email": email,
            "inbound_tag": inbound_tag,
            "level": int(level),
            "flow": flow,
            "ok": True,
        }

    try:
        op_tm = _build_add_user_operation_typed(user_uuid=user_uuid, email=email, level=int(level), flow=flow)
        req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
        resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())

        # Xray часто отвечает пустым сообщением => нормализуем
        try:
            data = _pb_to_dict(resp)
            return {"ok": True, "response": data}
        except Exception:
            return {"ok": True, "response": {}}
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    """Remove user from inbound by email."""
    if XRAY_MOCK:
        return {"mock": True, "action": "remove", "email": email, "inbound_tag": inbound_tag, "ok": True}

    try:
        op_tm = _build_remove_user_operation_typed(email=email)
        req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
        resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
        try:
            data = _pb_to_dict(resp)
            return {"ok": True, "response": data}
        except Exception:
            return {"ok": True, "response": {}}
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"AlterInbound(RemoveUser) tag={inbound_tag} email={email}")


def inbound_users(tag: str) -> Dict[str, Any]:
    """
    Raw inbound users.

    NOTE: GetInboundUsers expects GetInboundUserRequest(tag, email).
    We use email="" for list.
    """
    if XRAY_MOCK:
        return {"users": []}

    try:
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsers(req, timeout=_rpc_timeout_sec())
        return _pb_to_dict(resp)
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsers tag={tag}")


def inbound_users_count(tag: str) -> int:
    """
    Returns normalized int count.
    """
    if XRAY_MOCK:
        return 0

    try:
        req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
        resp = _get_handler_stub().GetInboundUsersCount(req, timeout=_rpc_timeout_sec())
        data: Dict[str, Any] = _pb_to_dict(resp)
        raw = data.get("count", 0)
        try:
            return int(raw)
        except Exception:
            return 0
    except grpc.RpcError as e:
        _raise_grpc_error(e, context=f"GetInboundUsersCount tag={tag}")


def inbound_emails(tag: str) -> list[str]:
    data = inbound_users(tag)
    users = data.get("users") or []
    out: list[str] = []
    for u in users:
        if isinstance(u, dict) and u.get("email"):
            out.append(str(u["email"]))
    return out


def inbound_uuids(tag: str) -> list[str]:
    """
    Extract UUIDs from TypedMessage VLESS Account.
    MessageToDict usually returns account.value as base64 string.
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
