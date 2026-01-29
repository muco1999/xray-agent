#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

# ===== настройки =====
XRAY_CORE_REF="${XRAY_CORE_REF:-main}"   # можно: v1.8.8 или commit hash
TMP_DIR=".tmp_xray_core"
OUT_DIR="xrayproto"

echo "[1/7] Clean old output"
rm -rf "${TMP_DIR}" "${OUT_DIR}"
mkdir -p "${TMP_DIR}"

echo "[2/7] Download Xray-core (${XRAY_CORE_REF})"
# без git (быстрее/проще): tar.gz
curl -fsSL "https://github.com/XTLS/Xray-core/archive/refs/heads/${XRAY_CORE_REF}.tar.gz" \
  | tar -xz --strip-components=1 -C "${TMP_DIR}"

echo "[3/7] Ensure generator deps (grpcio-tools, protobuf)"
python - <<'PY'
import sys, pkgutil
missing=[]
for p in ("grpc_tools", "google.protobuf"):
    if pkgutil.find_loader(p) is None:
        missing.append(p)
if missing:
    print("Missing deps:", missing)
    sys.exit(1)
print("OK")
PY

echo "[4/7] Generate pb2 into project root (temporary)"
# генерит папки app/, common/, proxy/, ...
python -m grpc_tools.protoc \
  -I "${TMP_DIR}" \
  --python_out=. \
  --grpc_python_out=. \
  $(find "${TMP_DIR}" -name "*.proto")

echo "[5/7] Move generated top-level packages into ${OUT_DIR}/"
mkdir -p "${OUT_DIR}"
for d in app common core proxy transport features infra; do
  if [[ -d "${d}" ]]; then
    mv "${d}" "${OUT_DIR}/${d}"
  fi
done

echo "[6/7] Add __init__.py everywhere (packages)"
find "${OUT_DIR}" -type d -exec sh -lc 'test -f "$1/__init__.py" || : > "$1/__init__.py"' sh {} \;

echo "[7/7] Patch imports inside *_pb2*.py to use xrayproto.* namespace"
python - <<'PY'
from __future__ import annotations
import pathlib, re

ROOT = pathlib.Path("xrayproto")
TOP = ["app","common","core","proxy","transport","features","infra"]

patterns=[]
for p in TOP:
    patterns.append((re.compile(rf"(^\s*from\s+){p}(\.)", re.M), rf"\1xrayproto.{p}\2"))
    patterns.append((re.compile(rf"(^\s*import\s+){p}(\.)", re.M), rf"\1xrayproto.{p}\2"))

def patch(path: pathlib.Path) -> bool:
    t = path.read_text(encoding="utf-8")
    n = t
    for rgx, repl in patterns:
        n = rgx.sub(repl, n)
    if n != t:
        path.write_text(n, encoding="utf-8")
        return True
    return False

changed=0
for py in ROOT.rglob("*.py"):
    if py.name.endswith("_pb2.py") or py.name.endswith("_pb2_grpc.py"):
        if patch(py):
            changed += 1

print("patched files:", changed)
PY

echo "[DONE] Generated ${OUT_DIR}/"
echo "Next:"
echo "  git add xrayproto tools/vendor_xrayproto.sh"
echo "  git commit -m \"vendor xrayproto\""
echo "  git push"
