#!/usr/bin/env bash
set -euo pipefail

# ===========================
# Xray Agent API test script (robust)
# ===========================
# Требования:
# - bash, curl
# - (опционально) jq для красивого вывода
#
# ENV:
#   BASE_URL="http://127.0.0.1:18000"
#   TOKEN="long_secret"
#   TAG="vless-in"
#   EMAIL="7313853417"
#   UUID="6b1c0138-b938-..."
#
# Optional "mutating" tests:
#   DO_ISSUE=1
#   DO_REMOVE=1
#   DO_ADD_USER=1
#   DO_RESTORE=1
#
# Robust options:
#   FAIL_FAST=1          # падать на ошибках health/full != 200
#   SHOW_HEADERS=1       # печатать все заголовки ответа
#   SHOW_BODY_ON_ERROR=1 # печатать body при http>=400
#
# Пример:
# BASE_URL="http://127.0.0.1:18000" TOKEN="..." TAG="vless-in" EMAIL="7313853417" DO_ISSUE=1 ./test_xray_agent_endpoints.sh

BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
TOKEN="${TOKEN:-}"
TAG="${TAG:-vless-in}"
EMAIL="${EMAIL:-7313853417}"
UUID="${UUID:-00000000-0000-0000-0000-000000000000}"

DO_ISSUE="${DO_ISSUE:-0}"
DO_REMOVE="${DO_REMOVE:-0}"
DO_ADD_USER="${DO_ADD_USER:-0}"
DO_RESTORE="${DO_RESTORE:-0}"

ISSUE_LEVEL="${ISSUE_LEVEL:-0}"
ISSUE_FLOW="${ISSUE_FLOW:-xtls-rprx-vision}"

RESTORE_CONCURRENCY="${RESTORE_CONCURRENCY:-20}"
RESTORE_PRECHECK="${RESTORE_PRECHECK:-true}"

CURL_TIMEOUT="${CURL_TIMEOUT:-20}"           # curl --max-time
JOB_POLL_TIMEOUT="${JOB_POLL_TIMEOUT:-40}"   # общий лимит poll job
JOB_POLL_INTERVAL="${JOB_POLL_INTERVAL:-1}"  # интервал poll

FAIL_FAST="${FAIL_FAST:-0}"
SHOW_HEADERS="${SHOW_HEADERS:-0}"
SHOW_BODY_ON_ERROR="${SHOW_BODY_ON_ERROR:-1}"

# -------- helpers --------
have_jq() { command -v jq >/dev/null 2>&1; }
req_id() { printf "test-%s" "$(date +%s)"; }

hdr_auth=()
if [[ -n "${TOKEN}" ]]; then
  hdr_auth=(-H "Authorization: Bearer ${TOKEN}")
else
  echo "⚠️ TOKEN пустой. Если require_token включён — запросы вернут 401."
fi

# Возвращает body в stdout.
# Метаданные (http_code, content-type, headers) печатает в stderr (чтобы pipe | pretty работал нормально).
curl_json() {
  local method="$1"; shift
  local url="$1"; shift
  local data="${1:-}"; shift || true

  local rid body_file headers_file http_code ct
  rid="$(req_id)"
  body_file="$(mktemp)"
  headers_file="$(mktemp)"

  {
    echo
    echo "➡️  ${method} ${url}"
    [[ -n "${data}" ]] && echo "   body: ${data}"
  } >&2

  if [[ -n "${data}" ]]; then
    http_code="$(
      curl -sS --max-time "${CURL_TIMEOUT}" \
        -o "${body_file}" -D "${headers_file}" \
        -w "%{http_code}" \
        -X "${method}" "${url}" \
        "${hdr_auth[@]}" \
        -H "Content-Type: application/json" \
        -H "X-Request-ID: ${rid}" \
        --data "${data}" \
      || echo "000"
    )"
  else
    http_code="$(
      curl -sS --max-time "${CURL_TIMEOUT}" \
        -o "${body_file}" -D "${headers_file}" \
        -w "%{http_code}" \
        -X "${method}" "${url}" \
        "${hdr_auth[@]}" \
        -H "X-Request-ID: ${rid}" \
      || echo "000"
    )"
  fi

  ct="$(grep -i '^content-type:' "${headers_file}" | tail -n1 | cut -d':' -f2- | tr -d '\r' | xargs || true)"

  {
    echo "   HTTP: ${http_code}"
    [[ -n "${ct}" ]] && echo "   Content-Type: ${ct}"
    if [[ "${SHOW_HEADERS}" == "1" ]]; then
      echo "   ---- headers ----"
      sed 's/\r$//' "${headers_file}"
      echo "   -----------------"
    fi
  } >&2

  # fail-fast logic only for health/full (caller will decide)
  # print body to stdout
  cat "${body_file}"

  rm -f "${body_file}" "${headers_file}"
}

# Pretty-print JSON if possible; otherwise print raw
pretty() {
  if have_jq; then
    if jq -e . >/dev/null 2>&1; then
      jq .
    else
      cat
    fi
  else
    cat
  fi
}

# -------- endpoints --------
HEALTH_FULL="${BASE_URL}/health/full"
XRAY_STATUS="${BASE_URL}/xray/status"
INBOUND_COUNT="${BASE_URL}/inbounds/${TAG}/users/count"
INBOUND_EMAILS="${BASE_URL}/inbounds/${TAG}/emails"

ISSUE="${BASE_URL}/clients/issue?async=true"
JOB="${BASE_URL}/jobs"
REMOVE="${BASE_URL}/clients/${EMAIL}?inbound_tag=${TAG}"
ADD_USER="${BASE_URL}/xray/add_user"
RESTORE="${BASE_URL}/xray/restore"

echo "=============================="
echo "🧪 Xray Agent API Smoke Tests"
echo "BASE_URL=${BASE_URL}"
echo "TAG=${TAG}"
echo "EMAIL=${EMAIL}"
echo "UUID=${UUID}"
echo "jq=$(have_jq && echo yes || echo no)"
echo "FAIL_FAST=${FAIL_FAST} SHOW_HEADERS=${SHOW_HEADERS} SHOW_BODY_ON_ERROR=${SHOW_BODY_ON_ERROR}"
echo "=============================="

# 1) /health/full
health_body="$(curl_json GET "${HEALTH_FULL}" || true)"
echo "${health_body}" | pretty || true

# Если FAIL_FAST=1 — проверяем, что health/full вернул JSON ok:true
if [[ "${FAIL_FAST}" == "1" ]]; then
  if have_jq; then
    ok_val="$(echo "${health_body}" | jq -r '.ok // empty' 2>/dev/null || true)"
    if [[ "${ok_val}" != "true" ]]; then
      echo "⛔ health/full not ok (ok=${ok_val}). Stop." >&2
      exit 2
    fi
  else
    # без jq: просто проверим что есть "ok"
    if ! echo "${health_body}" | grep -q '"ok"'; then
      echo "⛔ health/full seems not JSON ok. Stop." >&2
      exit 2
    fi
  fi
fi

# 2) /xray/status
curl_json GET "${XRAY_STATUS}" | pretty || true

# 3) inbound count
curl_json GET "${INBOUND_COUNT}" | pretty || true

# 4) inbound emails
curl_json GET "${INBOUND_EMAILS}" | pretty || true

# 5) add_user (optional)
if [[ "${DO_ADD_USER}" == "1" ]]; then
  payload=$(
    cat <<EOF
{"inbound_tag":"${TAG}","email":"${EMAIL}","uuid":"${UUID}","level":0,"flow":"${ISSUE_FLOW}","precheck":true}
EOF
  )
  curl_json POST "${ADD_USER}" "${payload}" | pretty || true
fi

# 6) restore (optional)
if [[ "${DO_RESTORE}" == "1" ]]; then
  payload=$(
    cat <<EOF
{
  "inbound_tag":"${TAG}",
  "precheck":${RESTORE_PRECHECK},
  "concurrency":${RESTORE_CONCURRENCY},
  "items":[
    {"email":"${EMAIL}","uuid":"${UUID}","level":0,"flow":"${ISSUE_FLOW}"}
  ]
}
EOF
  )
  curl_json POST "${RESTORE}" "${payload}" | pretty || true
fi

# 7) issue_client job (optional)
job_id=""
if [[ "${DO_ISSUE}" == "1" ]]; then
  payload=$(
    cat <<EOF
{"telegram_id":"${EMAIL}","inbound_tag":"${TAG}","level":${ISSUE_LEVEL},"flow":"${ISSUE_FLOW}"}
EOF
  )
  resp="$(curl_json POST "${ISSUE}" "${payload}" || true)"
  echo "${resp}" | pretty || true

  if have_jq; then
    job_id="$(echo "${resp}" | jq -r '.job_id // empty' 2>/dev/null || true)"
  else
    job_id="$(echo "${resp}" | sed -n 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  fi

  if [[ -z "${job_id}" ]]; then
    echo "⚠️  job_id не найден в ответе. Пропускаю poll." >&2
  else
    echo "🆔 job_id=${job_id}" >&2
  fi
fi

# 8) poll job
if [[ -n "${job_id}" ]]; then
  echo >&2
  echo "⏳ Polling job status (timeout=${JOB_POLL_TIMEOUT}s interval=${JOB_POLL_INTERVAL}s)" >&2
  t0="$(date +%s)"
  while true; do
    st="$(curl_json GET "${JOB}/${job_id}" || true)"
    echo "${st}" | pretty || true

    state=""
    if have_jq; then
      state="$(echo "${st}" | jq -r '.state // empty' 2>/dev/null || true)"
    else
      state="$(echo "${st}" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
    fi

    if [[ "${state}" == "done" || "${state}" == "error" ]]; then
      echo "✅ job finished with state=${state}" >&2
      break
    fi

    now="$(date +%s)"
    if (( now - t0 > JOB_POLL_TIMEOUT )); then
      echo "⛔ job poll timeout after ${JOB_POLL_TIMEOUT}s" >&2
      break
    fi

    sleep "${JOB_POLL_INTERVAL}"
  done
fi

# 9) remove client (optional)
if [[ "${DO_REMOVE}" == "1" ]]; then
  curl_json DELETE "${REMOVE}" | pretty || true
fi

echo
echo "✅ Done."
