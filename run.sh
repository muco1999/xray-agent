#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

echo "[1/5] bootstrap (.env + свободные порты)"
chmod +x bootstrap.sh run.sh
./bootstrap.sh

echo "[2/5] docker compose down"
docker compose down --remove-orphans || true

echo "[3/5] build (no cache)"
docker compose build --no-cache

echo "[4/5] up"
docker compose up -d

echo "[5/5] status"
docker compose ps

API_TOKEN="$(grep -E '^API_TOKEN=' .env | tail -n1 | cut -d= -f2-)"
API_HOST="$(grep -E '^API_BIND_HOST=' .env | tail -n1 | cut -d= -f2-)"
API_PORT="$(grep -E '^API_BIND_PORT=' .env | tail -n1 | cut -d= -f2-)"

echo ""
echo "[TEST]"
echo "curl -s -H \"Authorization: Bearer ${API_TOKEN}\" http://${API_HOST}:${AP ййфI_PORT}/health/full | jq"
