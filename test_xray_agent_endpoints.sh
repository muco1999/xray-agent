#!/usr/bin/env bash
set -euo pipefail

# ==========================================================
# Xray Agent Load Test (bash-only)
# ==========================================================
# Requirements: bash, curl, awk, date
# Optional: jq (for parsing JSON)
#
# ENV:
#   BASE_URL="http://194.135.33.76:18000"
#   TOKEN="..."
#   TAG="vless-in"
#
# Load params:
#   CONCURRENCY=200        # параллельность (xargs -P)
#   REQUESTS=2000          # общее число запросов на endpoint
#   TIMEOUT=10             # curl --max-time
#   WARMUP=5               # прогрев (GET /health/full)
#
# Scenario toggles:
#   DO_GETS=1              # нагрузка на GET endpoints
#   DO_ISSUE=0             # нагрузка на issue_client (ОПАСНО: создаёт клиентов)
#   ISSUE_REQUESTS=50
#   ISSUE_CONCURRENCY=10
#   POLL_TIMEOUT=40
#   POLL_INTERVAL=1
#
# Output:
#   - summary per endpoint
#   - global totals
#   - optional CSV logs (CSV=1)
#
# Example:
#   BASE_URL="http://194.135.33.76:18000" TOKEN="..." TAG="vless-in" \
#   CONCURRENCY=300 REQUESTS=5000 DO_GETS=1 ./load_test_xray_agent.sh
#
#   # issue test (careful):
#   DO_ISSUE=1 ISSUE_REQUESTS=30 ISSUE_CONCURRENCY=5 ./load_test_xray_agent.sh

BASE_URL="${BASE_URL:-http://127.0.0.1:18000}"
TOKEN="${TOKEN:-}"
TAG="${TAG:-vless-in}"

CONCURRENCY="${CONCURRENCY:-200}"
REQUESTS="${REQUESTS:-2000}"
TIMEOUT="${TIMEOUT:-10}"
WARMUP="${WARMUP:-5}"

DO_GETS="${DO_GETS:-1}"
DO_ISSUE="${DO_ISSUE:-0}"

ISSUE_REQUESTS="${ISSUE_REQUESTS:-20}"
ISSUE_CONCURRENCY="${ISSUE_CONCURRENCY:-5}"
POLL_TIMEOUT="${POLL_TIMEOUT:-40}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"

CSV="${CSV:-0}"

have_jq() { command -v jq >/dev/null 2>&1; }

hdr_auth=()
if [[ -n "${TOKEN}" ]]; then
  hdr_auth=(-H "Authorization: Bearer ${TOKEN}")
else
  echo "⚠️ TOKEN пустой. Если require_token включён — будут 401."
fi

ts_ms() { date +%s%3N; }

# curl_one METHOD URL [JSON_BODY]
# prints: "code dur_ms"
curl_one() {
  local method="$1"; shift
  local url="$1"; shift
  local body="${1:-}"

  local t0 t1 code
  t0="$(ts_ms)"
  if [[ -n "${body}" ]]; then
    code="$(
      curl -sS --max-time "${TIMEOUT}" -o /dev/null \
        -w "%{http_code}" \
        -X "${method}" "${url}" \
        "${hdr_auth[@]}" \
        -H "Content-Type: application/json" \
        --data "${body}" \
      || echo "000"
    )"
  else
    code="$(
      curl -sS --max-time "${TIMEOUT}" -o /dev/null \
        -w "%{http_code}" \
        -X "${method}" "${url}" \
        "${hdr_auth[@]}" \
      || echo "000"
    )"
  fi
  t1="$(ts_ms)"
  echo "${code} $((t1 - t0))"
}

percentile_p95() {
  # input: durations (ms), one per line
  # output: p95
  awk '
    {a[NR]=$1}
    END{
      if(NR==0){print 0; exit}
      n=asort(a)
      idx=int((n*95+99)/100)  # ceil
      if(idx<1) idx=1
      if(idx>n) idx=n
      print a[idx]
    }'
}

summarize() {
  local name="$1"
  local tmp="$2"

  local total ok err min max avg p95
  total="$(wc -l < "${tmp}" | tr -d ' ')"
  ok="$(awk '$1>=200 && $1<300{c++} END{print c+0}' "${tmp}")"
  err="$((total - ok))"
  min="$(awk 'NR==1{m=$2} {if($2<m)m=$2} END{print m+0}' "${tmp}")"
  max="$(awk 'NR==1{m=$2} {if($2>m)m=$2} END{print m+0}' "${tmp}")"
  avg="$(awk '{s+=$2} END{ if(NR==0) print 0; else printf "%.2f", s/NR }' "${tmp}")"
  p95="$(awk '{print $2}' "${tmp}" | sort -n | percentile_p95)"

  echo "----------------------------------------"
  echo "📌 ${name}"
  echo "  total=${total} ok=${ok} err=${err} err%=$(awk -v e="${err}" -v t="${total}" 'BEGIN{ if(t==0) print "0.00"; else printf "%.2f", (e*100.0)/t }')"
  echo "  latency_ms: min=${min} avg=${avg} p95=${p95} max=${max}"
}

run_load_get() {
  local name="$1"
  local url="$2"
  local tmp
  tmp="$(mktemp)"

  echo "🚀 Load GET ${name}  requests=${REQUESTS} concurrency=${CONCURRENCY}"
  # генерируем N пустых строк -> параллельно выполняем curl_one
  seq 1 "${REQUESTS}" | xargs -P "${CONCURRENCY}" -I{} bash -c \
    'curl_one "GET" "'"${url}"'"' \
    >> "${tmp}"

  summarize "${name}" "${tmp}"

  if [[ "${CSV}" == "1" ]]; then
    cp "${tmp}" "./${name//\//_}.csv"
    echo "  CSV saved: ./${name//\//_}.csv (format: code dur_ms)"
  fi

  rm -f "${tmp}"
}

# issue job: POST /clients/issue?async=true then poll job until done/error
issue_one() {
  local base="$1"
  local tag="$2"

  # уникальный telegram_id чтобы не было коллизий и чтобы не трогать реальных юзеров
  local tid="load$(date +%s)${RANDOM}${RANDOM}"

  local body
  body="$(printf '{"telegram_id":"%s","inbound_tag":"%s","level":0,"flow":"xtls-rprx-vision"}' "${tid}" "${tag}")"

  # 1) POST issue
  local t0 t1 code resp job_id
  t0="$(ts_ms)"
  resp="$(
    curl -sS --max-time "${TIMEOUT}" \
      -X POST "${base}/clients/issue?async=true" \
      "${hdr_auth[@]}" \
      -H "Content-Type: application/json" \
      --data "${body}" \
    || true
  )"
  t1="$(ts_ms)"

  if have_jq; then
    job_id="$(echo "${resp}" | jq -r '.job_id // empty' 2>/dev/null || true)"
  else
    job_id="$(echo "${resp}" | sed -n 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  fi

  # если нет job_id — считаем ошибкой
  if [[ -z "${job_id}" ]]; then
    echo "202 $((t1 - t0)) ISSUE_NO_JOBID"
    return
  fi

  # 2) poll
  local start now state
  start="$(date +%s)"
  while true; do
    now="$(date +%s)"
    if (( now - start > POLL_TIMEOUT )); then
      echo "200 $((t1 - t0)) POLL_TIMEOUT"
      return
    fi

    st="$(
      curl -sS --max-time "${TIMEOUT}" \
        -X GET "${base}/jobs/${job_id}" \
        "${hdr_auth[@]}" \
      || true
    )"

    if have_jq; then
      state="$(echo "${st}" | jq -r '.state // empty' 2>/dev/null || true)"
    else
      state="$(echo "${st}" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
    fi

    if [[ "${state}" == "done" ]]; then
      echo "200 $((t1 - t0)) DONE"
      return
    fi
    if [[ "${state}" == "error" ]]; then
      echo "200 $((t1 - t0)) ERROR"
      return
    fi

    sleep "${POLL_INTERVAL}"
  done
}

run_issue_load() {
  local tmp
  tmp="$(mktemp)"

  echo "🚀 Load ISSUE (creates users!) requests=${ISSUE_REQUESTS} concurrency=${ISSUE_CONCURRENCY}"
  export -f issue_one have_jq ts_ms
  export BASE_URL TAG TIMEOUT POLL_TIMEOUT POLL_INTERVAL
  # передаём hdr_auth через env нельзя напрямую, поэтому делаем простой режим:
  # если TOKEN задан — используем в issue_one через глобальные hdr_auth (работает в bash -c только если объявить там)
  # => проще: не использовать bash -c here, а сделать xargs выполнять текущий shell? Не гарантировано.
  # Поэтому используем файл-обёртку: здесь сделаем inline bash -c с повтором hdr_auth.
  seq 1 "${ISSUE_REQUESTS}" | xargs -P "${ISSUE_CONCURRENCY}" -I{} bash -c '
    hdr_auth=()
    if [[ -n "'"${TOKEN}"'" ]]; then
      hdr_auth=(-H "Authorization: Bearer '"${TOKEN}"'")
    fi
    have_jq() { command -v jq >/dev/null 2>&1; }
    ts_ms() { date +%s%3N; }
    issue_one() '"$(declare -f issue_one)"'
    issue_one "'"${BASE_URL}"'" "'"${TAG}"'"
  ' >> "${tmp}"

  # summarize: for issue we treat only "DONE" as ok
  local total done err timeout
  total="$(wc -l < "${tmp}" | tr -d ' ')"
  done="$(awk '$3=="DONE"{c++} END{print c+0}' "${tmp}")"
  timeout="$(awk '$3=="POLL_TIMEOUT"{c++} END{print c+0}' "${tmp}")"
  err="$((total - done))"

  echo "----------------------------------------"
  echo "📌 ISSUE scenario"
  echo "  total=${total} done=${done} err=${err} poll_timeout=${timeout}"
  echo "  latency_ms: (POST only) min=$(awk 'NR==1{m=$2} {if($2<m)m=$2} END{print m+0}' "${tmp}") avg=$(awk '{s+=$2} END{ if(NR==0) print 0; else printf "%.2f", s/NR }' "${tmp}") p95=$(awk '{print $2}' "${tmp}" | sort -n | percentile_p95) max=$(awk 'NR==1{m=$2} {if($2>m)m=$2} END{print m+0}' "${tmp}")"

  if [[ "${CSV}" == "1" ]]; then
    cp "${tmp}" "./ISSUE.csv"
    echo "  CSV saved: ./ISSUE.csv (format: httpcode dur_ms status)"
  fi

  rm -f "${tmp}"
}

# ----------------- start -----------------
echo "=============================="
echo "🔥 Xray Agent Load Test"
echo "BASE_URL=${BASE_URL}"
echo "TAG=${TAG}"
echo "CONCURRENCY=${CONCURRENCY} REQUESTS=${REQUESTS} TIMEOUT=${TIMEOUT}s"
echo "DO_GETS=${DO_GETS} DO_ISSUE=${DO_ISSUE}"
echo "jq=$(have_jq && echo yes || echo no)"
echo "=============================="

echo "♨️ Warmup /health/full x${WARMUP}"
for _ in $(seq 1 "${WARMUP}"); do
  curl -sS --max-time "${TIMEOUT}" -o /dev/null \
    -X GET "${BASE_URL}/health/full" "${hdr_auth[@]}" || true
done

if [[ "${DO_GETS}" == "1" ]]; then
  run_load_get "/health/full" "${BASE_URL}/health/full"
  run_load_get "/xray/status" "${BASE_URL}/xray/status"
  run_load_get "/inbounds/${TAG}/users/count" "${BASE_URL}/inbounds/${TAG}/users/count"
  run_load_get "/inbounds/${TAG}/emails" "${BASE_URL}/inbounds/${TAG}/emails"
fi

if [[ "${DO_ISSUE}" == "1" ]]; then
  run_issue_load
fi

echo
echo "✅ Load test done."
