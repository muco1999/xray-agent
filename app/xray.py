# """
# Xray gRPC адаптер (grpcio).
#
# Назначение:
# - Проверка состояния Xray (через GetSysStats)
# - Управление пользователями inbound (AlterInbound: AddUserOperation/RemoveUserOperation)
# - Получение пользователей inbound (GetInboundUsers / GetInboundUsersCount)
#
# Важное:
# - Модуль СИНХРОННЫЙ (grpcio sync). В async-коде вызывать через threadpool/to_thread.
# - XRAY_MOCK=true включает локальный режим без реального Xray.
# - Реализован самовосстанавливающийся gRPC-канал:
#   иногда grpcio "залипает" на сломанном соединении/после рестартов Xray.
#   Если канал не готов, мы пересоздаём channel/stubs и пробуем снова.
#   Это предотвращает вечное состояние job="running" в worker.
#
# Логирование (добавлено):
# - Структурные json-логи на каждый RPC (start/ok/fail + latency)
# - Всегда логируем addr/tag/email/uuid (email/uuid маскируются)
# - Логируем какой request использован для list/count (GetInboundUsersRequest vs GetInboundUserRequest)
# - Логируем grpc code/details при ошибках
# """
#
# from __future__ import annotations
#
# import base64
# import json
# import logging
# import os
# import threading
# import time
# from contextlib import contextmanager
# from typing import Any, Dict
#
# import grpc
# from google.protobuf.json_format import MessageToDict
#
# from app.settings import settings
#
# from app.utils import parse_hostport, is_tcp_open
#
# # Protobuf: TypedMessage / User / VLESS Account
# from xrayproto.common.serial import typed_message_pb2
# from xrayproto.common.protocol import user_pb2
# from xrayproto.proxy.vless import account_pb2 as vless_account_pb2
#
# # Proxyman/Handler service (AlterInbound, GetInboundUsers...)
# from xrayproto.app.proxyman.command import command_pb2 as proxyman_cmd_pb2
# from xrayproto.app.proxyman.command import command_pb2_grpc as proxyman_cmd_pb2_grpc
#
# # Stats service (GetSysStats)
# from xrayproto.app.stats.command import command_pb2 as stats_cmd_pb2
# from xrayproto.app.stats.command import command_pb2_grpc as stats_cmd_pb2_grpc
#
#
# XRAY_MOCK = os.getenv("XRAY_MOCK", "").lower() in ("1", "true", "yes")
#
# log = logging.getLogger("xray-agent.xray")
#
#
# # =============================================================================
# # Logging helpers
# # =============================================================================
# def _mask(s: str, keep: int = 3) -> str:
#     """
#     Маскирует строку для логов (email/uuid).
#     keep=3 => abc***xyz
#     """
#     if not s:
#         return ""
#     s = str(s)
#     if len(s) <= keep * 2:
#         return "*" * len(s)
#     return f"{s[:keep]}***{s[-keep:]}"
#
#
# def _grpc_err_info(e: grpc.RpcError) -> Dict[str, Any]:
#     code = None
#     details = None
#     try:
#         code = e.code()
#     except Exception:
#         pass
#     try:
#         details = e.details()
#     except Exception:
#         details = str(e)
#     return {"code": str(code), "details": str(details)[:500]}
#
#
# @contextmanager
# def _rpc_log_ctx(op: str, **fields):
#     """
#     Контекст логирования RPC: start/ok/fail + latency ms.
#     Логи в JSON строке, удобно для Loki/ELK.
#     """
#     t0 = time.perf_counter()
#     base = {"op": op, **fields}
#
#     log.info("xray rpc start %s", json.dumps(base, ensure_ascii=False))
#     try:
#         yield base
#         dt = round((time.perf_counter() - t0) * 1000, 2)
#         log.info("xray rpc ok %s", json.dumps({**base, "ms": dt}, ensure_ascii=False))
#     except Exception as e:
#         dt = round((time.perf_counter() - t0) * 1000, 2)
#         payload = {**base, "ms": dt, "exc": type(e).__name__, "msg": str(e)[:500]}
#         log.error("xray rpc fail %s", json.dumps(payload, ensure_ascii=False))
#         raise
#
#
# # =============================================================================
# # Вспомогательное: protobuf -> dict (совместимость версий protobuf)
# # =============================================================================
# def _pb_to_dict(msg) -> dict:
#     """
#     Преобразование protobuf-сообщения в dict.
#
#     Параметры MessageToDict отличаются между версиями protobuf, поэтому
#     пробуем включить опции аккуратно.
#     """
#     kwargs = {"preserving_proto_field_name": True}
#     for k, v in (
#         ("including_default_value_fields", False),
#         ("use_integers_for_enums", True),
#         ("always_print_fields_with_no_presence", False),
#     ):
#         try:
#             MessageToDict(msg, **{**kwargs, k: v})
#             kwargs[k] = v
#         except TypeError:
#             pass
#     return MessageToDict(msg, **kwargs)
#
#
# # =============================================================================
# # TypedMessage builders (AlterInbound)
# # =============================================================================
# def _typed_message_bytes(type_name: str, msg_bytes: bytes) -> typed_message_pb2.TypedMessage:
#     """
#     TypedMessage: type=<полное имя proto>, value=<bytes SerializeToString>.
#     """
#     return typed_message_pb2.TypedMessage(type=type_name, value=msg_bytes)
#
#
# def _build_vless_account_bytes(user_uuid: str, flow: str) -> bytes:
#     """
#     Собираем xray.proxy.vless.Account (bytes).
#     """
#     acc = vless_account_pb2.Account(id=user_uuid, flow=flow or "")
#     return acc.SerializeToString()
#
#
# def _build_add_user_operation_typed(user_uuid: str, email: str, level: int, flow: str) -> typed_message_pb2.TypedMessage:
#     """
#     Собираем TypedMessage для AddUserOperation.
#     """
#     account_bytes = _build_vless_account_bytes(user_uuid, flow)
#
#     user = user_pb2.User(
#         level=int(level),
#         email=str(email),
#         account=_typed_message_bytes("xray.proxy.vless.Account", account_bytes),
#     )
#
#     op = proxyman_cmd_pb2.AddUserOperation(user=user)
#     return _typed_message_bytes("xray.app.proxyman.command.AddUserOperation", op.SerializeToString())
#
#
# def _build_remove_user_operation_typed(email: str) -> typed_message_pb2.TypedMessage:
#     """
#     Собираем TypedMessage для RemoveUserOperation.
#     """
#     op = proxyman_cmd_pb2.RemoveUserOperation(email=str(email))
#     return _typed_message_bytes("xray.app.proxyman.command.RemoveUserOperation", op.SerializeToString())
#
#
# # =============================================================================
# # gRPC: канал и stubs (самовосстановление)
# # =============================================================================
# _channel: grpc.Channel | None = None
# _handler_stub: proxyman_cmd_pb2_grpc.HandlerServiceStub | None = None
# _stats_stub: stats_cmd_pb2_grpc.StatsServiceStub | None = None
#
# _lock = threading.Lock()
#
#
# def _rpc_timeout_sec(default: int = 10) -> int:
#     try:
#         return int(getattr(settings, "xray_rpc_timeout_sec", default))
#     except Exception:
#         return default
#
#
# def _connect_ready_timeout_sec(default: float = 2.0) -> float:
#     try:
#         return float(getattr(settings, "xray_connect_ready_timeout_sec", default))
#     except Exception:
#         return default
#
#
# def _grpc_channel_options() -> list[tuple[str, int]]:
#     """
#     Настройки keepalive, чтобы избежать ENHANCE_YOUR_CALM / too_many_pings.
#     """
#     return [
#         ("grpc.keepalive_permit_without_calls", 0),
#         ("grpc.keepalive_time_ms", 120_000),
#         ("grpc.keepalive_timeout_ms", 10_000),
#         ("grpc.http2.max_pings_without_data", 1),
#         ("grpc.http2.min_time_between_pings_ms", 60_000),
#         ("grpc.http2.min_ping_interval_without_data_ms", 120_000),
#     ]
#
#
# def _reset_channel_locked() -> None:
#     global _channel, _handler_stub, _stats_stub
#     _channel = None
#     _handler_stub = None
#     _stats_stub = None
#
#
# def _get_or_create_channel_locked() -> grpc.Channel:
#     global _channel
#     if _channel is None:
#         _channel = grpc.insecure_channel(settings.xray_api_addr, options=_grpc_channel_options())
#         log.info("xray channel created addr=%s", settings.xray_api_addr)
#     return _channel
#
#
# def _ensure_channel_ready() -> None:
#     if XRAY_MOCK:
#         return
#
#     ready_timeout = _connect_ready_timeout_sec()
#     log.debug("xray ensure_channel_ready addr=%s ready_timeout=%.2f", settings.xray_api_addr, ready_timeout)
#
#     with _lock:
#         ch = _get_or_create_channel_locked()
#
#         # attempt #1
#         try:
#             grpc.channel_ready_future(ch).result(timeout=ready_timeout)
#             return
#         except Exception as e:
#             log.warning(
#                 "xray channel not ready (attempt1) addr=%s err=%s",
#                 settings.xray_api_addr,
#                 str(e)[:300],
#             )
#             _reset_channel_locked()
#             ch = _get_or_create_channel_locked()
#
#         # attempt #2
#         log.debug("xray ensure_channel_ready retry addr=%s", settings.xray_api_addr)
#         grpc.channel_ready_future(ch).result(timeout=ready_timeout)
#
#
# def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
#     global _handler_stub
#     with _lock:
#         if _handler_stub is None:
#             _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(_get_or_create_channel_locked())
#             log.info("xray handler stub created addr=%s", settings.xray_api_addr)
#         return _handler_stub
#
#
# def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
#     global _stats_stub
#     with _lock:
#         if _stats_stub is None:
#             _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(_get_or_create_channel_locked())
#             log.info("xray stats stub created addr=%s", settings.xray_api_addr)
#         return _stats_stub
#
#
# def _raise_grpc_error(e: grpc.RpcError, *, context: str) -> None:
#     code = e.code() if hasattr(e, "code") else None
#     details = e.details() if hasattr(e, "details") else str(e)
#     raise RuntimeError(f"grpcio failed ({context}): code={code} details={details}") from e
#
#
# # =============================================================================
# # Public API
# # =============================================================================
# def xray_api_sys_stats() -> Dict[str, Any] | None:
#     if XRAY_MOCK:
#         return {"mock": True, "sys_stats": {}}
#
#     try:
#         with _rpc_log_ctx("GetSysStats", addr=settings.xray_api_addr):
#             _ensure_channel_ready()
#             stub = _get_stats_stub()
#             req = stats_cmd_pb2.SysStatsRequest()
#             resp = stub.GetSysStats(req, timeout=_rpc_timeout_sec())
#             data = _pb_to_dict(resp)
#             log.info(
#                 "xray GetSysStats response addr=%s keys=%s",
#                 settings.xray_api_addr,
#                 list(data.keys()) if isinstance(data, dict) else type(data).__name__,
#             )
#             return data
#     except grpc.RpcError as e:
#         log.error("xray grpc error GetSysStats addr=%s info=%s", settings.xray_api_addr, _grpc_err_info(e))
#         _raise_grpc_error(e, context="GetSysStats")
#
#
# def xray_runtime_status() -> Dict[str, Any]:
#     host, port = parse_hostport(settings.xray_api_addr)
#     port_open = is_tcp_open(host, port)
#
#     st: Dict[str, Any] = {
#         "xray_api_addr": settings.xray_api_addr,
#         "xray_api_port_open": port_open,
#         "grpcio_present": True,
#         "time": int(time.time()),
#     }
#
#     if XRAY_MOCK:
#         st["ok"] = True
#         st["mock"] = True
#         st["xray_api_sys_stats"] = {"mock": True}
#         return st
#
#     if not port_open:
#         st["ok"] = False
#         st["error"] = "Xray API port is not open"
#         log.warning("xray runtime_status port closed addr=%s", settings.xray_api_addr)
#         return st
#
#     try:
#         st["xray_api_sys_stats"] = xray_api_sys_stats()
#         st["ok"] = True
#     except Exception as e:
#         st["ok"] = False
#         st["xray_api_sys_stats_error"] = str(e)[:500]
#
#     return st
#
#
#
#
# def _is_user_not_found(e: grpc.RpcError) -> bool:
#     try:
#         details = (e.details() or "").lower()
#     except Exception:
#         details = str(e).lower()
#
#     return ("not found" in details) and ("user" in details)
#
#
# def remove_client(email: str, inbound_tag: str) -> Dict[str, Any] | None:
#     if XRAY_MOCK:
#         return {"mock": True, "action": "remove", "email": email, "inbound_tag": inbound_tag}
#
#     try:
#         with _rpc_log_ctx(
#             "AlterInbound(RemoveUser)",
#             addr=settings.xray_api_addr,
#             inbound_tag=inbound_tag,
#             email=_mask(email),
#         ):
#             _ensure_channel_ready()
#             op_tm = _build_remove_user_operation_typed(email=email)
#             req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
#
#             resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
#             try:
#                 data = _pb_to_dict(resp)
#                 log.info(
#                     "xray AlterInbound(RemoveUser) response addr=%s tag=%s email=%s resp_keys=%s",
#                     settings.xray_api_addr,
#                     inbound_tag,
#                     _mask(email),
#                     list(data.keys()) if isinstance(data, dict) else type(data).__name__,
#                 )
#                 return data
#             except Exception as e:
#                 log.warning(
#                     "xray pb_to_dict failed AlterInbound(RemoveUser) addr=%s tag=%s email=%s err=%s",
#                     settings.xray_api_addr,
#                     inbound_tag,
#                     _mask(email),
#                     str(e)[:300],
#                 )
#                 return {}
#     except grpc.RpcError as e:
#         if _is_user_not_found(e):
#             log.info(
#                 "xray remove skipped (user not found) addr=%s tag=%s email=%s",
#                 settings.xray_api_addr,
#                 inbound_tag,
#                 _mask(email),
#             )
#             return {
#                 "ok": True,
#                 "skipped": True,
#                 "reason": "user not found",
#                 "email": str(email),
#                 "inbound_tag": str(inbound_tag),
#             }
#
#         log.error(
#             "xray grpc error AlterInbound(RemoveUser) addr=%s tag=%s email=%s info=%s",
#             settings.xray_api_addr,
#             inbound_tag,
#             _mask(email),
#             _grpc_err_info(e),
#         )
#         _raise_grpc_error(e, context=f"AlterInbound(RemoveUser) tag={inbound_tag} email={email}")
#
#
# def inbound_users(tag: str) -> Dict[str, Any] | None:
#     """
#     Получить список пользователей inbound.
#     Поддержка разных версий proto:
#       - GetInboundUsersRequest(tag=...)
#       - fallback: GetInboundUserRequest(tag=..., email="")
#     """
#     if XRAY_MOCK:
#         return {"users": []}
#
#     try:
#         with _rpc_log_ctx("GetInboundUsers", addr=settings.xray_api_addr, inbound_tag=tag):
#             _ensure_channel_ready()
#             stub = _get_handler_stub()
#
#             if hasattr(proxyman_cmd_pb2, "GetInboundUsersRequest"):
#                 log.info("xray GetInboundUsers using GetInboundUsersRequest addr=%s tag=%s", settings.xray_api_addr, tag)
#                 req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
#             else:
#                 log.info("xray GetInboundUsers using GetInboundUserRequest(email='') addr=%s tag=%s", settings.xray_api_addr, tag)
#                 req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
#
#             resp = stub.GetInboundUsers(req, timeout=_rpc_timeout_sec())
#             data = _pb_to_dict(resp)
#
#             users = data.get("users") if isinstance(data, dict) else None
#             cnt = len(users) if isinstance(users, list) else None
#             log.info(
#                 "xray GetInboundUsers response addr=%s tag=%s users_count=%s keys=%s",
#                 settings.xray_api_addr,
#                 tag,
#                 cnt,
#                 list(data.keys()) if isinstance(data, dict) else type(data).__name__,
#             )
#
#             return data
#
#     except grpc.RpcError as e:
#         log.error("xray grpc error GetInboundUsers addr=%s tag=%s info=%s", settings.xray_api_addr, tag, _grpc_err_info(e))
#         _raise_grpc_error(e, context=f"GetInboundUsers tag={tag}")
#
#
# def inbound_users_count(tag: str) -> int | None:
#     """
#     Количество пользователей inbound.
#     Поддержка разных версий proto.
#     """
#     if XRAY_MOCK:
#         return 0
#
#     try:
#         with _rpc_log_ctx("GetInboundUsersCount", addr=settings.xray_api_addr, inbound_tag=tag):
#             _ensure_channel_ready()
#             stub = _get_handler_stub()
#
#             if hasattr(proxyman_cmd_pb2, "GetInboundUsersRequest"):
#                 log.info("xray GetInboundUsersCount using GetInboundUsersRequest addr=%s tag=%s", settings.xray_api_addr, tag)
#                 req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
#             else:
#                 log.info("xray GetInboundUsersCount using GetInboundUserRequest(email='') addr=%s tag=%s", settings.xray_api_addr, tag)
#                 req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")
#
#             resp = stub.GetInboundUsersCount(req, timeout=_rpc_timeout_sec())
#             data: Dict[str, Any] = _pb_to_dict(resp)
#             raw = data.get("count", 0)
#
#             try:
#                 val = int(raw)
#             except Exception:
#                 val = 0
#
#             log.info(
#                 "xray GetInboundUsersCount response addr=%s tag=%s raw=%r parsed=%d",
#                 settings.xray_api_addr,
#                 tag,
#                 raw,
#                 val,
#             )
#             return val
#
#     except grpc.RpcError as e:
#         log.error("xray grpc error GetInboundUsersCount addr=%s tag=%s info=%s", settings.xray_api_addr, tag, _grpc_err_info(e))
#         _raise_grpc_error(e, context=f"GetInboundUsersCount tag={tag}")
#
#
# def inbound_emails(tag: str) -> list[str]:
#     """
#     Утилита: вытянуть emails из inbound_users.
#     """
#     data = inbound_users(tag)
#     users = data.get("users") or []
#     out: list[str] = []
#     for u in users:
#         if isinstance(u, dict) and u.get("email"):
#             out.append(str(u["email"]))
#     log.info("xray inbound_emails addr=%s tag=%s count=%d", settings.xray_api_addr, tag, len(out))
#     return out
#
#
# def inbound_uuids(tag: str) -> list[str]:
#     """
#     Утилита: вытянуть UUID из TypedMessage VLESS Account.
#     MessageToDict обычно отдаёт TypedMessage.value как base64 строку.
#     """
#     data = inbound_users(tag)
#     users = data.get("users") or []
#     out: list[str] = []
#
#     for u in users:
#         if not isinstance(u, dict):
#             continue
#
#         acc = u.get("account") or {}
#         if acc.get("type") != "xray.proxy.vless.Account":
#             continue
#
#         b64 = acc.get("value")
#         if not b64:
#             continue
#
#         try:
#             raw = base64.b64decode(b64)
#             vless_acc = vless_account_pb2.Account()
#             vless_acc.ParseFromString(raw)
#             if getattr(vless_acc, "id", ""):
#                 out.append(vless_acc.id)
#         except Exception:
#             continue
#
#     log.info("xray inbound_uuids addr=%s tag=%s count=%d", settings.xray_api_addr, tag, len(out))
#     return out
#
#
#
#
# from typing import Any, Dict
# import grpc
# from grpc import StatusCode
#
# # твой AlreadyExistsError из эндпоинта (или общий модуль)
# class AlreadyExistsError(Exception):
#     pass
#
#
# def _grpc_is_already_exists(e: grpc.RpcError) -> bool:
#     """
#     Best-effort классификация "уже существует".
#     В идеале Xray должен отдавать StatusCode.ALREADY_EXISTS,
#     но часто такие системы возвращают UNKNOWN с текстом.
#     """
#     try:
#         code = e.code()  # type: ignore[attr-defined]
#     except Exception:
#         code = None
#
#     # 1) Нормальный вариант по статус-коду
#     if code == StatusCode.ALREADY_EXISTS:
#         return True
#
#     # 2) В реальности Xray/проксиман иногда возвращает UNKNOWN/INTERNAL + текст
#     try:
#         details = (e.details() or "").lower()  # type: ignore[attr-defined]
#     except Exception:
#         details = str(e).lower()
#
#     # Подстрой под реальные сообщения из твоих логов/ошибок
#     # Примеры паттернов: "already exists", "duplicat", "exists", "email already"
#     needles = (
#         "already exists",
#         "exists",
#         "duplicate",
#         "duplicated",
#         "email",
#         "user",
#     )
#     # важно: не делаем слишком широким, но пусть будет чуть надежнее
#     return ("already" in details and "exist" in details) or ("duplicate" in details)
#
#
# def add_client(user_uuid: str, email: str, inbound_tag: str, level: int = 0, flow: str = "") -> Dict[str, Any]:
#     if XRAY_MOCK:
#         return {
#             "mock": True,
#             "action": "add",
#             "uuid": user_uuid,
#             "email": email,
#             "inbound_tag": inbound_tag,
#             "level": level,
#             "flow": flow,
#         }
#
#     eff_flow = flow or "xtls-rprx-vision"
#
#     try:
#         with _rpc_log_ctx(
#             "AlterInbound(AddUser)",
#             addr=settings.xray_api_addr,
#             inbound_tag=inbound_tag,
#             email=_mask(email),
#             uuid=_mask(user_uuid),
#             level=int(level),
#             flow=eff_flow,
#         ):
#             _ensure_channel_ready()
#
#             op_tm = _build_add_user_operation_typed(
#                 user_uuid=user_uuid,
#                 email=email,
#                 level=int(level),
#                 flow=eff_flow,
#             )
#             req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)
#
#             # timeout ОБЯЗАТЕЛЕН, у тебя он уже есть 👍
#             resp = _get_handler_stub().AlterInbound(req, timeout=_rpc_timeout_sec())
#
#             # resp часто пустой — ок
#             try:
#                 data = _pb_to_dict(resp)
#             except Exception as e:
#                 log.warning(
#                     "xray pb_to_dict failed AlterInbound(AddUser) addr=%s tag=%s email=%s err=%s",
#                     settings.xray_api_addr,
#                     inbound_tag,
#                     _mask(email),
#                     str(e)[:300],
#                 )
#                 return {}
#
#             log.info(
#                 "xray AlterInbound(AddUser) ok addr=%s tag=%s email=%s resp_keys=%s",
#                 settings.xray_api_addr,
#                 inbound_tag,
#                 _mask(email),
#                 list(data.keys()) if isinstance(data, dict) else type(data).__name__,
#             )
#             return data
#
#     except grpc.RpcError as e:
#         # ✅ typed "already exists"
#         if _grpc_is_already_exists(e):
#             # можно залогировать на DEBUG, чтобы не шуметь
#             log.debug(
#                 "xray AlterInbound(AddUser) already exists addr=%s tag=%s email=%s info=%s",
#                 settings.xray_api_addr,
#                 inbound_tag,
#                 _mask(email),
#                 _grpc_err_info(e),
#             )
#             raise AlreadyExistsError(f"user already exists: tag={inbound_tag} email={email}") from e
#
#         # ❌ всё остальное — реальная ошибка инфраструктуры
#         log.error(
#             "xray grpc error AlterInbound(AddUser) addr=%s tag=%s email=%s info=%s",
#             settings.xray_api_addr,
#             inbound_tag,
#             _mask(email),
#             _grpc_err_info(e),
#         )
#         _raise_grpc_error(e, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")
#         raise  # на случай если _raise_grpc_error не бросает







"""
Xray gRPC адаптер (grpc.aio).

Назначение:
- Проверка состояния Xray (через GetSysStats)
- Управление пользователями inbound (AlterInbound: AddUserOperation/RemoveUserOperation)
- Получение пользователей inbound (GetInboundUsers / GetInboundUsersCount)

Важное:
- Модуль ПОЛНОСТЬЮ АСИНХРОННЫЙ (grpc.aio). Можно вызывать напрямую из async-кода (FastAPI/aiogram).
- XRAY_MOCK=true включает локальный режим без реального Xray.
- Реализован самовосстанавливающийся gRPC-канал:
  иногда grpc.aio "залипает" на сломанном соединении/после рестартов Xray.
  Если канал не готов, мы пересоздаём channel/stubs и пробуем снова.
  Это предотвращает вечное состояние job="running" в worker.

Логирование:
- Структурные json-логи на каждый RPC (start/ok/fail + latency)
- Всегда логируем addr/tag/email/uuid (email/uuid маскируются)
- Логируем grpc code/details при ошибках
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import grpc
from grpc import StatusCode
from google.protobuf.json_format import MessageToDict

from app.settings import settings
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

log = logging.getLogger("xray-agent.xray")


# =============================================================================
# Errors
# =============================================================================
class AlreadyExistsError(Exception):
    """Пользователь уже существует в inbound (best-effort классификация)."""


# =============================================================================
# Logging helpers
# =============================================================================
def _mask(s: str, keep: int = 3) -> str:
    """
    Маскирует строку для логов (email/uuid).
    keep=3 => abc***xyz
    """
    if not s:
        return ""
    s = str(s)
    if len(s) <= keep * 2:
        return "*" * len(s)
    return f"{s[:keep]}***{s[-keep:]}"


def _grpc_err_info(e: grpc.RpcError) -> Dict[str, Any]:
    code = None
    details = None
    try:
        code = e.code()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        details = e.details()  # type: ignore[attr-defined]
    except Exception:
        details = str(e)
    return {"code": str(code), "details": str(details)[:500]}


@asynccontextmanager
async def _rpc_log_ctx(op: str, **fields):
    """
    Контекст логирования RPC: start/ok/fail + latency ms.
    Логи в JSON строке, удобно для Loki/ELK.
    """
    t0 = time.perf_counter()
    base = {"op": op, **fields}

    log.info("xray rpc start %s", json.dumps(base, ensure_ascii=False))
    try:
        yield base
        dt = round((time.perf_counter() - t0) * 1000, 2)
        log.info("xray rpc ok %s", json.dumps({**base, "ms": dt}, ensure_ascii=False))
    except Exception as e:
        dt = round((time.perf_counter() - t0) * 1000, 2)
        payload = {**base, "ms": dt, "exc": type(e).__name__, "msg": str(e)[:500]}
        log.error("xray rpc fail %s", json.dumps(payload, ensure_ascii=False))
        raise


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
# gRPC: канал и stubs (самовосстановление) — grpc.aio
# =============================================================================
_channel: Optional[grpc.aio.Channel] = None
_handler_stub: Optional[proxyman_cmd_pb2_grpc.HandlerServiceStub] = None
_stats_stub: Optional[stats_cmd_pb2_grpc.StatsServiceStub] = None

_lock = asyncio.Lock()


def _rpc_timeout_sec(default: int = 10) -> int:
    try:
        return int(getattr(settings, "xray_rpc_timeout_sec", default))
    except Exception:
        return default


def _connect_ready_timeout_sec(default: float = 2.0) -> float:
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


async def _close_channel_locked() -> None:
    """
    Закрыть текущий aio channel (best-effort).
    Вызывать ТОЛЬКО под _lock.
    """
    global _channel
    if _channel is None:
        return
    ch = _channel
    _channel = None
    try:
        await ch.close()
    except Exception as e:
        log.debug("xray channel close failed addr=%s err=%s", getattr(settings, "xray_api_addr", "?"), str(e)[:200])


def _reset_stubs_locked() -> None:
    """
    Сбросить stubs. Вызывать ТОЛЬКО под _lock.
    """
    global _handler_stub, _stats_stub
    _handler_stub = None
    _stats_stub = None


def _create_channel() -> grpc.aio.Channel:
    ch = grpc.aio.insecure_channel(settings.xray_api_addr, options=_grpc_channel_options())
    log.info("xray channel created addr=%s", settings.xray_api_addr)
    return ch


async def _get_or_create_channel_locked() -> grpc.aio.Channel:
    """
    Получить/создать channel. Вызывать ТОЛЬКО под _lock.
    """
    global _channel
    if _channel is None:
        _channel = _create_channel()
    return _channel


async def _await_channel_ready(ch: grpc.aio.Channel, timeout: float) -> None:
    """
    Совместимое ожидание готовности grpc.aio канала.

    В разных версиях grpcio:
    - Есть ch.channel_ready() (корутина/awaitable)  ✅ чаще всего
    - Иногда есть grpc.aio.channel_ready_future(ch) (реже)
    """
    # 1) Preferred: method on channel
    if hasattr(ch, "channel_ready"):
        await asyncio.wait_for(ch.channel_ready(), timeout=timeout)  # type: ignore[attr-defined]
        return

    # 2) Fallback: module-level future (если вдруг доступно)
    if hasattr(grpc.aio, "channel_ready_future"):
        fut = grpc.aio.channel_ready_future(ch)  # type: ignore[attr-defined]
        await asyncio.wait_for(fut, timeout=timeout)
        return

    # 3) Last resort (очень редко нужно)
    raise RuntimeError("grpc.aio channel readiness API not found (upgrade grpcio)")


async def _ensure_channel_ready() -> None:
    """
    Проверяет готовность канала. При проблеме пересоздаёт channel/stubs и повторяет (2 попытки).
    """
    if XRAY_MOCK:
        return

    ready_timeout = _connect_ready_timeout_sec()
    log.debug("xray ensure_channel_ready addr=%s ready_timeout=%.2f", settings.xray_api_addr, ready_timeout)

    async with _lock:
        ch = await _get_or_create_channel_locked()

        # attempt #1
        try:
            await _await_channel_ready(ch, ready_timeout)
            return
        except Exception as e:
            log.warning(
                "xray channel not ready (attempt1) addr=%s err=%s",
                settings.xray_api_addr,
                str(e)[:300],
            )
            _reset_stubs_locked()
            await _close_channel_locked()
            ch = await _get_or_create_channel_locked()

        # attempt #2
        log.debug("xray ensure_channel_ready retry addr=%s", settings.xray_api_addr)
        await _await_channel_ready(ch, ready_timeout)



async def _get_handler_stub() -> proxyman_cmd_pb2_grpc.HandlerServiceStub:
    global _handler_stub
    async with _lock:
        if _handler_stub is None:
            ch = await _get_or_create_channel_locked()
            _handler_stub = proxyman_cmd_pb2_grpc.HandlerServiceStub(ch)
            log.info("xray handler stub created addr=%s", settings.xray_api_addr)
        return _handler_stub


async def _get_stats_stub() -> stats_cmd_pb2_grpc.StatsServiceStub:
    global _stats_stub
    async with _lock:
        if _stats_stub is None:
            ch = await _get_or_create_channel_locked()
            _stats_stub = stats_cmd_pb2_grpc.StatsServiceStub(ch)
            log.info("xray stats stub created addr=%s", settings.xray_api_addr)
        return _stats_stub


def _raise_grpc_error(e: grpc.RpcError, *, context: str) -> None:
    code = None
    details = None
    try:
        code = e.code()  # type: ignore[attr-defined]
    except Exception:
        pass
    try:
        details = e.details()  # type: ignore[attr-defined]
    except Exception:
        details = str(e)
    raise RuntimeError(f"grpc(aio) failed ({context}): code={code} details={details}") from e


# =============================================================================
# Public API (async)
# =============================================================================
async def xray_api_sys_stats() -> Dict[str, Any] | None:
    if XRAY_MOCK:
        return {"mock": True, "sys_stats": {}}

    try:
        async with _rpc_log_ctx("GetSysStats", addr=settings.xray_api_addr):
            await _ensure_channel_ready()
            stub = await _get_stats_stub()
            req = stats_cmd_pb2.SysStatsRequest()

            resp = await stub.GetSysStats(req, timeout=_rpc_timeout_sec())
            data = _pb_to_dict(resp)
            log.info(
                "xray GetSysStats response addr=%s keys=%s",
                settings.xray_api_addr,
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

    except grpc.RpcError as e:
        log.error("xray grpc error GetSysStats addr=%s info=%s", settings.xray_api_addr, _grpc_err_info(e))
        _raise_grpc_error(e, context="GetSysStats")
        return None  # unreachable


async def xray_runtime_status() -> Dict[str, Any]:
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
        log.warning("xray runtime_status port closed addr=%s", settings.xray_api_addr)
        return st

    try:
        st["xray_api_sys_stats"] = await xray_api_sys_stats()
        st["ok"] = True
    except Exception as e:
        st["ok"] = False
        st["xray_api_sys_stats_error"] = str(e)[:500]

    return st


def _is_user_not_found(e: grpc.RpcError) -> bool:
    try:
        details = (e.details() or "").lower()  # type: ignore[attr-defined]
    except Exception:
        details = str(e).lower()
    return ("not found" in details) and ("user" in details)


def _grpc_is_already_exists(e: grpc.RpcError) -> bool:
    """
    Best-effort классификация "уже существует".
    В идеале Xray должен отдавать StatusCode.ALREADY_EXISTS,
    но часто такие системы возвращают UNKNOWN с текстом.
    """
    try:
        code = e.code()  # type: ignore[attr-defined]
    except Exception:
        code = None

    # 1) Нормальный вариант по статус-коду
    if code == StatusCode.ALREADY_EXISTS:
        return True

    # 2) В реальности Xray/проксиман иногда возвращает UNKNOWN/INTERNAL + текст
    try:
        details = (e.details() or "").lower()  # type: ignore[attr-defined]
    except Exception:
        details = str(e).lower()

    # Подстрой под реальные сообщения из твоих логов/ошибок
    # Примеры паттернов: "already exists", "duplicat", "exists", "email already"
    needles = (
        "already exists",
        "exists",
        "duplicate",
        "duplicated",
        "email",
        "user",
    )

    # важно: не делаем слишком широким, но пусть будет чуть надежнее
    return ("already" in details and "exist" in details) or ("duplicate" in details)


async def remove_client(email: str, inbound_tag: str) -> Dict[str, Any] | None:
    if XRAY_MOCK:
        return {"mock": True, "action": "remove", "email": email, "inbound_tag": inbound_tag}

    try:
        async with _rpc_log_ctx(
            "AlterInbound(RemoveUser)",
            addr=settings.xray_api_addr,
            inbound_tag=inbound_tag,
            email=_mask(email),
        ):
            await _ensure_channel_ready()
            op_tm = _build_remove_user_operation_typed(email=email)
            req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)

            stub = await _get_handler_stub()
            resp = await stub.AlterInbound(req, timeout=_rpc_timeout_sec())

            try:
                data = _pb_to_dict(resp)
                log.info(
                    "xray AlterInbound(RemoveUser) response addr=%s tag=%s email=%s resp_keys=%s",
                    settings.xray_api_addr,
                    inbound_tag,
                    _mask(email),
                    list(data.keys()) if isinstance(data, dict) else type(data).__name__,
                )
                return data
            except Exception as e:
                log.warning(
                    "xray pb_to_dict failed AlterInbound(RemoveUser) addr=%s tag=%s email=%s err=%s",
                    settings.xray_api_addr,
                    inbound_tag,
                    _mask(email),
                    str(e)[:300],
                )
                return {}

    except grpc.RpcError as e:
        if _is_user_not_found(e):
            log.info(
                "xray remove skipped (user not found) addr=%s tag=%s email=%s",
                settings.xray_api_addr,
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
            "xray grpc error AlterInbound(RemoveUser) addr=%s tag=%s email=%s info=%s",
            settings.xray_api_addr,
            inbound_tag,
            _mask(email),
            _grpc_err_info(e),
        )
        _raise_grpc_error(e, context=f"AlterInbound(RemoveUser) tag={inbound_tag} email={email}")
        return None  # unreachable


async def inbound_users(tag: str) -> Dict[str, Any] | None:
    """
    Получить список пользователей inbound.
    Поддержка разных версий proto:
      - GetInboundUsersRequest(tag=...)
      - fallback: GetInboundUserRequest(tag=..., email="")
    """
    if XRAY_MOCK:
        return {"users": []}

    try:
        async with _rpc_log_ctx("GetInboundUsers", addr=settings.xray_api_addr, inbound_tag=tag):
            await _ensure_channel_ready()
            stub = await _get_handler_stub()

            if hasattr(proxyman_cmd_pb2, "GetInboundUsersRequest"):
                log.info("xray GetInboundUsers using GetInboundUsersRequest addr=%s tag=%s", settings.xray_api_addr, tag)
                req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
            else:
                log.info(
                    "xray GetInboundUsers using GetInboundUserRequest(email='') addr=%s tag=%s",
                    settings.xray_api_addr,
                    tag,
                )
                req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")

            resp = await stub.GetInboundUsers(req, timeout=_rpc_timeout_sec())
            data = _pb_to_dict(resp)

            users = data.get("users") if isinstance(data, dict) else None
            cnt = len(users) if isinstance(users, list) else None
            log.info(
                "xray GetInboundUsers response addr=%s tag=%s users_count=%s keys=%s",
                settings.xray_api_addr,
                tag,
                cnt,
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )

            return data

    except grpc.RpcError as e:
        log.error("xray grpc error GetInboundUsers addr=%s tag=%s info=%s", settings.xray_api_addr, tag, _grpc_err_info(e))
        _raise_grpc_error(e, context=f"GetInboundUsers tag={tag}")
        return None  # unreachable


async def inbound_users_count(tag: str) -> int | None:
    """
    Количество пользователей inbound.
    Поддержка разных версий proto.
    """
    if XRAY_MOCK:
        return 0

    try:
        async with _rpc_log_ctx("GetInboundUsersCount", addr=settings.xray_api_addr, inbound_tag=tag):
            await _ensure_channel_ready()
            stub = await _get_handler_stub()

            if hasattr(proxyman_cmd_pb2, "GetInboundUsersRequest"):
                log.info("xray GetInboundUsersCount using GetInboundUsersRequest addr=%s tag=%s", settings.xray_api_addr, tag)
                req = proxyman_cmd_pb2.GetInboundUsersRequest(tag=tag)
            else:
                log.info(
                    "xray GetInboundUsersCount using GetInboundUserRequest(email='') addr=%s tag=%s",
                    settings.xray_api_addr,
                    tag,
                )
                req = proxyman_cmd_pb2.GetInboundUserRequest(tag=tag, email="")

            resp = await stub.GetInboundUsersCount(req, timeout=_rpc_timeout_sec())
            data: Dict[str, Any] = _pb_to_dict(resp)
            raw = data.get("count", 0)

            try:
                val = int(raw)
            except Exception:
                val = 0

            log.info(
                "xray GetInboundUsersCount response addr=%s tag=%s raw=%r parsed=%d",
                settings.xray_api_addr,
                tag,
                raw,
                val,
            )
            return val

    except grpc.RpcError as e:
        log.error("xray grpc error GetInboundUsersCount addr=%s tag=%s info=%s", settings.xray_api_addr, tag, _grpc_err_info(e))
        _raise_grpc_error(e, context=f"GetInboundUsersCount tag={tag}")
        return None  # unreachable


async def inbound_emails(tag: str) -> list[str]:
    """
    Утилита: вытянуть emails из inbound_users.
    """
    data = await inbound_users(tag)
    users = (data or {}).get("users") or []
    out: list[str] = []
    for u in users:
        if isinstance(u, dict) and u.get("email"):
            out.append(str(u["email"]))
    log.info("xray inbound_emails addr=%s tag=%s count=%d", settings.xray_api_addr, tag, len(out))
    return out


async def inbound_uuids(tag: str) -> list[str]:
    """
    Утилита: вытянуть UUID из TypedMessage VLESS Account.
    MessageToDict обычно отдаёт TypedMessage.value как base64 строку.
    """
    data = await inbound_users(tag)
    users = (data or {}).get("users") or []
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

    log.info("xray inbound_uuids addr=%s tag=%s count=%d", settings.xray_api_addr, tag, len(out))
    return out


async def add_client(user_uuid: str, email: str, inbound_tag: str, level: int = 0, flow: str = "") -> Dict[str, Any]:
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

    eff_flow = flow or "xtls-rprx-vision"

    try:
        async with _rpc_log_ctx(
            "AlterInbound(AddUser)",
            addr=settings.xray_api_addr,
            inbound_tag=inbound_tag,
            email=_mask(email),
            uuid=_mask(user_uuid),
            level=int(level),
            flow=eff_flow,
        ):
            await _ensure_channel_ready()

            op_tm = _build_add_user_operation_typed(
                user_uuid=user_uuid,
                email=email,
                level=int(level),
                flow=eff_flow,
            )
            req = proxyman_cmd_pb2.AlterInboundRequest(tag=inbound_tag, operation=op_tm)

            # timeout ОБЯЗАТЕЛЕН
            stub = await _get_handler_stub()
            resp = await stub.AlterInbound(req, timeout=_rpc_timeout_sec())

            # resp часто пустой — ok
            try:
                data = _pb_to_dict(resp)
            except Exception as e:
                log.warning(
                    "xray pb_to_dict failed AlterInbound(AddUser) addr=%s tag=%s email=%s err=%s",
                    settings.xray_api_addr,
                    inbound_tag,
                    _mask(email),
                    str(e)[:300],
                )
                return {}

            log.info(
                "xray AlterInbound(AddUser) ok addr=%s tag=%s email=%s resp_keys=%s",
                settings.xray_api_addr,
                inbound_tag,
                _mask(email),
                list(data.keys()) if isinstance(data, dict) else type(data).__name__,
            )
            return data

    except grpc.RpcError as e:
        # typed "already exists"
        if _grpc_is_already_exists(e):
            log.debug(
                "xray AlterInbound(AddUser) already exists addr=%s tag=%s email=%s info=%s",
                settings.xray_api_addr,
                inbound_tag,
                _mask(email),
                _grpc_err_info(e),
            )
            raise AlreadyExistsError(f"user already exists: tag={inbound_tag} email={email}") from e

        log.error(
            "xray grpc error AlterInbound(AddUser) addr=%s tag=%s email=%s info=%s",
            settings.xray_api_addr,
            inbound_tag,
            _mask(email),
            _grpc_err_info(e),
        )
        _raise_grpc_error(e, context=f"AlterInbound(AddUser) tag={inbound_tag} email={email}")
        return {}  # unreachable
