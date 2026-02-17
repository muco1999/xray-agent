# analyzer.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(frozen=True)
class Violation:
    email: str
    devices: int
    unique_ips: List[str]
    last_seen_ago_sec: Optional[float]
    top_hosts: List[Dict[str, Any]]
    client_row: Dict[str, Any]


def extract_violations(
    snapshot: Dict[str, Any],
    devices_limit: int,
    *,
    active_seen_sec: int = 120,
) -> List[Violation]:
    """
    Берём ТОЛЬКО активные нарушения:
    - devices_estimate > devices_limit
    - last_seen_ago_sec <= active_seen_sec (если поле есть)
    """
    clients = snapshot.get("clients") or []
    out: List[Violation] = []

    for row in clients:
        try:
            email = str(row.get("email") or "").strip()
            if not email:
                continue

            devices = int(row.get("devices_estimate") or 0)
            if devices <= devices_limit:
                continue

            raw_last = row.get("last_seen_ago_sec", None)
            last_seen: Optional[float]
            if raw_last is None:
                last_seen = None
            else:
                last_seen = float(raw_last)

            # anti “tail of window”: не баним/не предупреждаем неактивных
            if last_seen is not None and last_seen > float(active_seen_sec):
                continue

            out.append(
                Violation(
                    email=email,
                    devices=devices,
                    last_seen_ago_sec=last_seen,
                    unique_ips=list(row.get("unique_ips") or []),
                    top_hosts=list(row.get("top_hosts") or []),
                    client_row=row,
                )
            )
        except Exception:
            continue

    return out