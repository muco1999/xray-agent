import socket
import subprocess
from typing import Any, Dict, Tuple

def run_cmd(cmd: list[str], timeout: int = 20) -> Dict[str, Any]:
    p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
    return {
        "cmd": cmd,
        "rc": p.returncode,
        "stdout": (p.stdout or "").strip(),
        "stderr": (p.stderr or "").strip(),
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
