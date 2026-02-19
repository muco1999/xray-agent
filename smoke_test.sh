#!/usr/bin/env bash
set -euo pipefail

# =========================
# Config (ENV overridable)
# =========================
BASE_URL="${BASE_URL:-http://194.135.33.76:18000}"

AUTH_HEADER_NAME="${AUTH_HEADER_NAME:-Authorization}"
AUTH_SCHEME="${AUTH_SCHEME:-Bearer}"     # <-- ВАЖНО: Bearer (не Bear)
API_TOKEN="${API_TOKEN:-}"

FOLLOW_REDIRECTS="${FOLLOW_REDIRECTS:-1}"

CURL_TIMEOUT="${CURL_TIMEOUT:-12}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-4}"

DEFAULT_TAG="${DEFAULT_TAG:-vless-in}"

TEST_EMAIL="${TEST_EMAIL:-smoke-999001}"
TEST_UUID="${TEST_UUID:-11111111-1111-1111-1111-111111111111}"
TEST_TG_ID="${TEST_TG_ID:-999001}"       # numeric string required

RL_MAX_RETRIES="${RL_MAX_RETRIES:-6}"
RL_FALLBACK_SLEEP_MS="${RL_FALLBACK_SLEEP_MS:-150}"

JQ_BIN="${JQ_BIN:-jq}"

# =========================
# Helpers
# =========================
ts() { date +"%Y-%m-%d %H:%M:%S"; }
log() { echo "$(ts) | $*"; }
ok() { echo "$(ts) | ✅ $*"; }
warn() { echo "$(ts) | ⚠️  $*" >&2; }
err() { echo "$(ts) | ❌ $*" >&2; }

have_jq() { command -v "$JQ_BIN" >/dev/null 2>&1; }

if [[ -z "$API_TOKEN" ]]; then
  err "API_TOKEN is empty. Run like: API_TOKEN='LONG_SECRET' $0"
  exit 2
fi

if ! [[ "$TEST_TG_ID" =~ ^[0-9]+$ ]]; then
  err "TEST_TG_ID must be numeric string, got: '$TEST_TG_ID'"
  exit 2
fi

auth_args=(-H "${AUTH_HEADER_NAME}: ${AUTH_SCHEME} ${API_TOKEN}")

curl_args=(
  -sS
  --connect-timeout "$CURL_CONNECT_TIMEOUT"
  --max-time "$CURL_TIMEOUT"
  "${auth_args[@]}"
)
[[ "$FOLLOW_REDIRECTS" == "1" ]] && curl_args+=(-L)

RESP_CODE=""
RESP_BODY=""
RESP_HEADERS=""
RESP_LOCATION=""

request() {
  local method="$1"; shift
  local path="$1"; shift
  local data="${1:-}"
  local url="${BASE_URL%/}${path}"

  local tmp_body tmp_hdr
  tmp_body="$(mktemp)"
  tmp_hdr="$(mktemp)"

  if [[ -n "$data" ]]; then
    RESP_CODE="$(curl "${curl_args[@]}" -X "$method" "$url" \
      -H "Content-Type: application/json" \
      -D "$tmp_hdr" -o "$tmp_body" -w "%{http_code}" \
      --data "$data" || echo "000")"
  else
    RESP_CODE="$(curl "${curl_args[@]}" -X "$method" "$url" \
      -D "$tmp_hdr" -o "$tmp_body" -w "%{http_code}" || echo "000")"
  fi

  RESP_BODY="$(cat "$tmp_body" 2>/dev/null || true)"
  RESP_HEADERS="$(cat "$tmp_hdr" 2>/dev/null || true)"
  RESP_LOCATION="$(grep -i '^location:' "$tmp_hdr" | tail -n1 | cut -d' ' -f2- | tr -d '\r' || true)"

  rm -f "$tmp_body" "$tmp_hdr"
}

request_with_rl_retry() {
  local method="$1"; shift
  local path="$1"; shift
  local data="${1:-}"

  local i
  for i in $(seq 1 "$RL_MAX_RETRIES"); do
    request "$method" "$path" "$data"

    if [[ "$RESP_CODE" != "429" ]]; then
      return 0
    fi

    local ms="$RL_FALLBACK_SLEEP_MS"
    if have_jq; then
      local parsed
      parsed="$(echo "$RESP_BODY" | "$JQ_BIN" -r '.retry_after_ms // empty' 2>/dev/null || true)"
      [[ -n "$parsed" && "$parsed" != "null" ]] && ms="$parsed"
    else
      local parsed
      parsed="$(echo "$RESP_BODY" | sed -n 's/.*"retry_after_ms":\([0-9]\+\).*/\1/p' | head -n1 || true)"
      [[ -n "$parsed" ]] && ms="$parsed"
    fi

    local sec
    sec="$(awk -v m="$ms" 'BEGIN{ s=m/1000.0; if(s<0.05)s=0.05; printf "%.3f", s }')"
    warn "RATE_LIMITED (429) -> sleep ${sec}s and retry (${i}/${RL_MAX_RETRIES})"
    sleep "$sec"
  done
  return 0
}

dump_fail() {
  local ctx="$1"
  err "$ctx: HTTP=${RESP_CODE}"
  [[ -n "$RESP_LOCATION" ]] && err "$ctx: Location=${RESP_LOCATION}"
  [[ -n "$RESP_BODY" ]] && echo "$RESP_BODY" | sed 's/^/    /'
}

expect_code_any() {
  local ctx="$1"; shift
  local c
  for c in "$@"; do
    [[ "$RESP_CODE" == "$c" ]] && return 0
  done
  dump_fail "$ctx (expected: $*)"
  return 1
}

json_assert() {
  local ctx="$1"
  local expr="$2"
  if have_jq; then
    echo "$RESP_BODY" | "$JQ_BIN" -e "$expr" >/dev/null || {
      err "$ctx: jq assert failed: $expr"
      echo "$RESP_BODY" | sed 's/^/    /'
      return 1
    }
  else
    warn "$ctx: jq not found, skipping strict JSON asserts"
  fi
}

run() {
  local name="$1"; shift
  log "▶️  $name"
  if "$@"; then
    ok "$name"
  else
    err "$name"
    return 1
  fi
  echo
}

# =========================
# Tests
# =========================

t_health_full() {
  request GET "/health/full"
  expect_code_any "GET /health/full" 200 503 || return 1
  json_assert "GET /health/full" '(.request_id? // .error.request_id? // "") | type=="string"' || return 1
}

t_xray_status() {
  request GET "/xray/status"
  expect_code_any "GET /xray/status" 200 || return 1
  json_assert "GET /xray/status" '. | type=="object"' || return 1
}

t_health_logfile() {
  request GET "/health/logfile"
  expect_code_any "GET /health/logfile" 200 503 || return 1
}

t_status_clients() {
  request GET "/xray/status/clients"
  expect_code_any "GET /xray/status/clients" 200 || return 1
  json_assert "GET /xray/status/clients" '(.ok | type=="boolean") or (.result? | type=="object")' || return 1
}

t_inbound_count() {
  request GET "/inbounds/${DEFAULT_TAG}/users/count"
  expect_code_any "GET /inbounds/{tag}/users/count" 200 || return 1
  json_assert "GET /inbounds/{tag}/users/count" '.result | type=="number"' || return 1
}

t_inbound_emails() {
  request GET "/inbounds/${DEFAULT_TAG}/emails"
  expect_code_any "GET /inbounds/{tag}/emails" 200 || return 1
  json_assert "GET /inbounds/{tag}/emails" '.result | type=="array"' || return 1
}

t_xray_health_prefixed() {
  request GET "/xray/health"
  expect_code_any "GET /xray/health" 200 || return 1
}

t_xray_add_user_prefixed() {
  local payload
  payload="{\"inbound_tag\":\"${DEFAULT_TAG}\",\"email\":\"${TEST_EMAIL}\",\"uuid\":\"${TEST_UUID}\",\"level\":0,\"flow\":\"xtls-rprx-vision\",\"precheck\":true}"
  request_with_rl_retry POST "/xray/add_user" "$payload"
  expect_code_any "POST /xray/add_user" 200 || return 1
  json_assert "POST /xray/add_user" '.ok == true' || return 1
}

t_xray_restore_prefixed_minimal() {
  local payload
  payload="{\"inbound_tag\":\"${DEFAULT_TAG}\",\"items\":[{\"email\":\"${TEST_EMAIL}\",\"uuid\":\"${TEST_UUID}\",\"level\":0,\"flow\":\"xtls-rprx-vision\"}],\"precheck\":true,\"concurrency\":5}"
  request_with_rl_retry POST "/xray/restore" "$payload"
  expect_code_any "POST /xray/restore" 200 || return 1
  json_assert "POST /xray/restore" '.total | type=="number"' || return 1
}

t_remove_client_sync() {
  request_with_rl_retry DELETE "/clients/${TEST_EMAIL}?inbound_tag=${DEFAULT_TAG}"
  expect_code_any "DELETE /clients/{email}" 200 404 502 || return 1
}

t_remove_client_async() {
  request_with_rl_retry DELETE "/clients/${TEST_EMAIL}?inbound_tag=${DEFAULT_TAG}&async=true"
  expect_code_any "DELETE /clients/{email}?async=true" 200 404 502 || return 1
}

t_issue_client_async_enqueue_and_poll() {
  local payload
  payload="{\"telegram_id\":\"${TEST_TG_ID}\",\"inbound_tag\":\"${DEFAULT_TAG}\"}"

  request_with_rl_retry POST "/clients/issue?async=true" "$payload"
  expect_code_any "POST /clients/issue?async=true" 202 502 || return 1
  [[ "$RESP_CODE" == "202" ]] || return 1

  have_jq || { warn "jq not found -> skipping job polling"; return 0; }

  local job_id
  job_id="$(echo "$RESP_BODY" | "$JQ_BIN" -r '.job_id // empty')"
  [[ -n "$job_id" ]] || { err "job_id empty"; echo "$RESP_BODY" | sed 's/^/    /'; return 1; }

  local i state
  for i in {1..30}; do
    request GET "/jobs/${job_id}"
    expect_code_any "GET /jobs/{job_id}" 200 || return 1
    state="$(echo "$RESP_BODY" | "$JQ_BIN" -r '.state // empty')"
    log "   job state=$state (try $i/30)"

    [[ "$state" == "done" ]] && return 0
    [[ "$state" == "error" ]] && { err "job error"; echo "$RESP_BODY" | sed 's/^/    /'; return 1; }
    sleep 0.5
  done

  err "job polling timeout"
  return 1
}

# =========================
# Run suite
# =========================
log "BASE_URL=$BASE_URL"
log "DEFAULT_TAG=$DEFAULT_TAG"
log "FOLLOW_REDIRECTS=$FOLLOW_REDIRECTS"
log "AUTH=${AUTH_HEADER_NAME}: ${AUTH_SCHEME} (set)"
log "TEST_EMAIL=$TEST_EMAIL"
log "TEST_TG_ID=$TEST_TG_ID"
echo

failed=0
run "GET /health/full" t_health_full || failed=$((failed+1))
run "GET /xray/status" t_xray_status || failed=$((failed+1))
run "GET /health/logfile" t_health_logfile || failed=$((failed+1))
run "GET /xray/status/clients" t_status_clients || failed=$((failed+1))
run "GET /inbounds/{tag}/users/count" t_inbound_count || failed=$((failed+1))
run "GET /inbounds/{tag}/emails" t_inbound_emails || failed=$((failed+1))
run "GET /xray/health (prefixed)" t_xray_health_prefixed || failed=$((failed+1))
run "POST /xray/add_user (prefixed)" t_xray_add_user_prefixed || failed=$((failed+1))
run "POST /xray/restore (prefixed)" t_xray_restore_prefixed_minimal || failed=$((failed+1))
run "DELETE /clients/{email} (sync)" t_remove_client_sync || failed=$((failed+1))
run "DELETE /clients/{email} (async)" t_remove_client_async || failed=$((failed+1))
run "POST /clients/issue?async=true + poll" t_issue_client_async_enqueue_and_poll || failed=$((failed+1))

if (( failed > 0 )); then
  err "SMOKE TEST FAILED: ${failed} test(s) failed"
  exit 1
fi

ok "SMOKE TEST OK ✅"
