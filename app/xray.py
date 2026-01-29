"""
Xray gRPC adapter.

Мы используем grpcurl + локальные proto-файлы (/srv/proto), потому что на Xray часто отключён reflection.
Для AlterInbound нужен TypedMessage с protobuf bytes:
- AddUserOperation / RemoveUserOperation
- VLESS Account тоже TypedMessage (xray.proxy.vless.Account)

В этой сборке Xray нет InboundOperation, поэтому operation = TypedMessage(AddUserOperation|RemoveUserOperation).
"""

import base64
import json
import time
from typing import Any, Dict, Optional

from app.config import settings
from app.utils import run_cmd, parse_hostport, is_tcp_open

# Python protobuf classes (генерятся/подключаются в твоей сборке как xrayproto.*)
from xrayproto.common.serial import typed_message_pb2
from xrayproto.common.protocol import user_pb2
from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
from xrayproto.proxy.vless import account_pb2 as vless_account_pb2

# gRPC methods
ALTER_INBOUND_METHOD = "xray.app.proxyman.command.HandlerService.AlterInbound"
GET_SYS_STATS_METHOD = "xray.app.stats.command.StatsService.GetSysStats"
GET_INBOUND_USERS_METHOD = "xray.app.proxyman.command.HandlerService.GetInboundUsers"
GET_INBOUND_USERS_COUNT_METHOD = "xray.app.proxyman.command.HandlerService.GetInboundUsersCount"


def _typed_message(type_name: str, msg_bytes: bytes) -> Dict[str, Any]:
    """
    Представление TypedMessage для grpcurl JSON:
      - type: "xray...."
      - value: base64(bytes)
    """
    return {"type": type_name, "value": base64.b64encode(msg_bytes).decode("ascii")}


def _build_vless_account_typed(uuid: str, flow: str) -> Dict[str, Any]:
    """
    Создать TypedMessage для VLESS Account.

    В твоей схеме vless Account имеет поля:
      - id: string UUID
      - flow: string (optional)
    """
    acc = vless_account_pb2.Account(id=uuid, flow=flow or "")
    return _typed_message("xray.proxy.vless.Account", acc.SerializeToString())


def _build_add_user_operation_typed(uuid: str, email: str, level: int, flow: str) -> Dict[str, Any]:
    """
    Создать TypedMessage(AddUserOperation) для AlterInboundRequest.operation.

    Важно:
      - user.account тоже TypedMessage, поэтому мы создаём typed_message_pb2.TypedMessage вручную.
    """
    account_tm = _build_vless_account_typed(uuid, flow)

    user = user_pb2.User(
        level=level,
        email=email,
        account=typed_message_pb2.TypedMessage(
            type=account_tm["type"],
            value=base64.b64decode(account_tm["value"]),
        ),
    )

    op = proxyman_cmd_pb2.AddUserOperation(user=user)

    return _typed_message(
        "xray.app.proxyman.command.AddUserOperation",
        op.SerializeToString(),
    )


def _build_remove_user_operation_typed(email: str) -> Dict[str, Any]:
    """TypedMessage(RemoveUserOperation) для AlterInboundRequest.operation."""
    op = proxyman_cmd_pb2.RemoveUserOperation(email=email)
    return _typed_message(
        "xray.app.proxyman.command.RemoveUserOperation",
        op.SerializeToString(),
    )


def grpcurl_call(method: str, payload: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Dict[str, Any]:
    """
    Универсальный вызов grpcurl с proto.

    Мы выбираем конкретный .proto файл по имени метода.
    proto_root задаётся через env XRAY_PROTO_ROOT (обычно /srv/proto).
    """
    if method.startswith("xray.app.stats.command.StatsService."):
        proto_file = "app/stats/command/command.proto"
    elif method.startswith("xray.app.proxyman.command.HandlerService."):
        proto_file = "app/proxyman/command/command.proto"
    else:
        raise RuntimeError(f"Unknown method for proto mapping: {method}")

    cmd = [
        "grpcurl",
        "-plaintext",
        "-import-path",
        settings.proto_root,
        "-proto",
        proto_file,
    ]

    if payload is not None:
        cmd += ["-d", json.dumps(payload)]

    cmd += [settings.xray_api_addr, method]

    res = run_cmd(cmd, timeout=timeout)
    if res["rc"] != 0:
        raise RuntimeError(f"grpcurl failed: {res}")

    out = res["stdout"]
    if not out:
        return {}

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"raw": out}


def xray_api_sys_stats() -> Dict[str, Any]:
    """GetSysStats — удобный health сигнал (без reflection, с proto)."""
    return grpcurl_call(GET_SYS_STATS_METHOD)


def xray_runtime_status() -> Dict[str, Any]:
    """
    Сводный статус:
    - открыт ли порт Xray gRPC
    - есть ли grpcurl
    - sys stats (если всё доступно)
    """
    host, port = parse_hostport(settings.xray_api_addr)
    port_open = is_tcp_open(host, port)

    status: Dict[str, Any] = {
        "xray_api_addr": settings.xray_api_addr,
        "xray_api_port_open": port_open,
        "time": int(time.time()),
    }

    grpcurl_present = run_cmd(["bash", "-lc", "command -v grpcurl"], timeout=10)
    status["grpcurl_present"] = {"rc": grpcurl_present["rc"], "path": grpcurl_present["stdout"]}

    if not port_open:
        status["ok"] = False
        status["error"] = "Xray API port is not open"
        return status

    if grpcurl_present["rc"] != 0:
        status["ok"] = False
        status["error"] = "grpcurl is not available in PATH"
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
    op_tm = _build_add_user_operation_typed(uuid=uuid, email=email, level=level, flow=flow)
    payload = {"tag": inbound_tag, "operation": op_tm}
    return grpcurl_call(ALTER_INBOUND_METHOD, payload)


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    """Удалить пользователя из inbound по email."""
    op_tm = _build_remove_user_operation_typed(email=email)
    payload = {"tag": inbound_tag, "operation": op_tm}
    return grpcurl_call(ALTER_INBOUND_METHOD, payload)


def inbound_users(tag: str) -> Dict[str, Any]:
    """Сырые users inbound."""
    return grpcurl_call(GET_INBOUND_USERS_METHOD, {"tag": tag})


def inbound_users_count(tag: str) -> Dict[str, Any]:
    """Количество users inbound."""
    return grpcurl_call(GET_INBOUND_USERS_COUNT_METHOD, {"tag": tag})


def inbound_emails(tag: str) -> list[str]:
    """Список email пользователей inbound."""
    data = inbound_users(tag)
    users = data.get("users") or []
    return [u["email"] for u in users if u.get("email")]


def inbound_uuids(tag: str) -> list[str]:
    """
    Достаём UUID из TypedMessage VLESS Account.

    user.account: {type: "...", value: "<base64>"}
    value -> bytes -> vless_account_pb2.Account -> .id
    """
    data = inbound_users(tag)
    users = data.get("users") or []
    out: list[str] = []

    for u in users:
        acc = u.get("account") or {}
        if acc.get("type") != "xray.proxy.vless.Account":
            continue

        b64 = acc.get("value")
        if not b64:
            continue

        raw = base64.b64decode(b64)
        vless_acc = vless_account_pb2.Account()
        vless_acc.ParseFromString(raw)

        if getattr(vless_acc, "id", ""):
            out.append(vless_acc.id)

    return out
