import json
from typing import Any, Dict, Optional

from .config import settings
from .utils import run_cmd, parse_hostport, is_tcp_open

ALTER_INBOUND_METHOD = "xray.app.proxyman.command.HandlerService.AlterInbound"
GET_SYS_STATS_METHOD = "xray.app.stats.command.StatsService.GetSysStats"


def grpcurl_call(method: str, payload: Optional[Dict[str, Any]] = None, timeout: int = 20) -> Dict[str, Any]:
    """
    Call Xray gRPC API via grpcurl and return parsed JSON (or raw output).
    Works in containers (no systemctl needed).
    """
    cmd = ["grpcurl", "-plaintext"]
    if payload is not None:
        cmd += ["-d", json.dumps(payload)]
    cmd += [settings.xray_api_addr, method]

    res = run_cmd(cmd, timeout=timeout)
    if res["rc"] != 0:
        # важный момент: бросаем понятную ошибку (для FastAPI 500 detail)
        raise RuntimeError(f"grpcurl failed: {res}")

    out = res["stdout"]
    if not out:
        return {}

    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"raw": out}


def xray_api_sys_stats() -> Dict[str, Any]:
    return grpcurl_call(GET_SYS_STATS_METHOD)


def xray_runtime_status() -> Dict[str, Any]:
    """
    Container-friendly status:
    - checks TCP port open for Xray API
    - calls GetSysStats (gRPC) if port is open
    """
    host, port = parse_hostport(settings.xray_api_addr)
    port_open = is_tcp_open(host, port)

    status: Dict[str, Any] = {
        "xray_api_addr": settings.xray_api_addr,
        "xray_api_port_open": port_open,
    }

    # grpcurl presence (helpful for debugging)
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
    payload = {
        "tag": inbound_tag,
        "operation": {
            "add": {
                "user": {
                    "level": level,
                    "email": email,
                    "account": {"id": uuid, "flow": flow},
                }
            }
        },
    }
    return grpcurl_call(ALTER_INBOUND_METHOD, payload)


def remove_client(email: str, inbound_tag: str) -> Dict[str, Any]:
    payload = {"tag": inbound_tag, "operation": {"remove": {"email": email}}}
    return grpcurl_call(ALTER_INBOUND_METHOD, payload)
