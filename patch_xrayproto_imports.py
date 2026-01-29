#!/usr/bin/env python3
from __future__ import annotations
import pathlib
import re

ROOT = pathlib.Path("xrayproto")
TOP = ["app", "common", "core", "proxy", "transport", "features", "infra"]

rules: list[tuple[re.Pattern, str]] = []

for p in TOP:
    # from core.xxx import ...
    rules.append((re.compile(rf"(^\s*from\s+){p}(\.)", re.M), rf"\1xrayproto.{p}\2"))
    # import core.xxx as ...
    rules.append((re.compile(rf"(^\s*import\s+){p}(\.)", re.M), rf"\1xrayproto.{p}\2"))

    # from core import something
    rules.append((re.compile(rf"(^\s*from\s+){p}(\s+import\s+)", re.M), rf"\1xrayproto.{p}\2"))
    # import core as something
    rules.append((re.compile(rf"(^\s*import\s+){p}(\s|$)", re.M), rf"\1xrayproto.{p}\2"))

def patch_file(path: pathlib.Path) -> bool:
    text = path.read_text(encoding="utf-8")
    new = text
    for rgx, repl in rules:
        new = rgx.sub(repl, new)
    if new != text:
        path.write_text(new, encoding="utf-8")
        return True
    return False

def main() -> None:
    if not ROOT.exists():
        raise SystemExit("xrayproto/ not found")

    changed = 0
    for py in ROOT.rglob("*.py"):
        if py.name.endswith("_pb2.py") or py.name.endswith("_pb2_grpc.py"):
            if patch_file(py):
                changed += 1

    print(f"patched files: {changed}")

if __name__ == "__main__":
    main()
