import socket
import subprocess
from typing import Any, Dict, Tuple


def run_cmd(cmd: list[str], timeout: int = 20) -> Dict[str, Any]:
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
        return {
            "cmd": cmd,
            "rc": p.returncode,
            "stdout": (p.stdout or "").strip(),
            "stderr": (p.stderr or "").strip(),
            "timeout": False,
        }
    except subprocess.TimeoutExpired as e:
        return {
            "cmd": cmd,
            "rc": None,
            "stdout": (getattr(e, "stdout", "") or "").strip(),
            "stderr": (getattr(e, "stderr", "") or "").strip(),
            "timeout": True,
        }


def is_tcp_open(host: str, port: int, timeout: float = 0.7) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


def parse_hostport(addr: str) -> Tuple[str, int]:
    if ":" not in addr:
        raise ValueError("XRAY_API_ADDR must be host:port")
    host, port_s = addr.rsplit(":", 1)
    return host, int(port_s)

import math


def format_minutes(seconds: int) -> str:
    minutes = math.ceil(seconds / 60)

    if minutes % 10 == 1 and minutes % 100 != 11:
        word = "минута"
    elif 2 <= minutes % 10 <= 4 and not 12 <= minutes % 100 <= 14:
        word = "минуты"
    else:
        word = "минут"

    return f"{minutes} {word}"



