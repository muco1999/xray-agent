#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:18000}"
TOKEN="${TOKEN:-}"
TAG="${TAG:-vless-in}"

CONCURRENCY="${CONCURRENCY:-200}"
REQUESTS="${REQUESTS:-2000}"
TIMEOUT="${TIMEOUT:-10}"
WARMUP="${WARMUP:-5}"

DO_GETS="${DO_GETS:-1}"

CSV="${CSV:-0}"
have_jq() { command -v jq >/dev/null 2>&1; }

# auth header (string, so we can pass into subshell)
AUTH_HEADER=""
if [[ -n "${TOKEN}" ]]; then
  AUTH_HEADER="Authorization: Bearer ${TOKEN}"
else
  echo "⚠️ TOKEN пустой. Если require_token включён — будут 401."
fi

ts_ms() { date +%s%3N; }

# prints: "code dur_ms"
curl_one() {
  local method="$1"; shift
  local url="$1"; shift
  local body="${1:-}"

  local t0 t1 code
  t0="$(ts_ms)"

  if [[ -n "${AUTH_HEADER}" ]]; then
    if [[ -n "${body}" ]]; then
      code="$(
        curl -sS --max-time "${TIMEOUT}" -o /dev/null \
          -w "%{http_code}" \
          -X "${method}" "${url}" \
          -H "${AUTH_HEADER}" \
          -H "Content-Type: application/json" \
          --data "${body}" \
        || echo "000"
      )"
    else
      code="$(
        curl -sS --max-time "${TIMEOUT}" -o /dev/null \
          -w "%{http_code}" \
          -X "${method}" "${url}" \
          -H "${AUTH_HEADER}" \
        || echo "000"
      )"
    fi
  else
    if [[ -n "${body}" ]]; then
      code="$(
        curl -sS --max-time "${TIMEOUT}" -o /dev/null \
          -w "%{http_code}" \
          -X "${method}" "${url}" \
          -H "Content-Type: application/json" \
          --data "${body}" \
        || echo "000"
      )"
    else
      code="$(
        curl -sS --max-time "${TIMEOUT}" -o /dev/null \
          -w "%{http_code}" \
          -X "${method}" "${url}" \
        || echo "000"
      )"
    fi
  fi

  t1="$(ts_ms)"
  echo "${code} $((t1 - t0))"
}

percentile_p95() {
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

  # экспортируем функции/переменные, чтобы xargs->bash их видел
  export -f curl_one ts_ms
  export TIMEOUT AUTH_HEADER

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

echo "=============================="
echo "🔥 Xray Agent Load Test"
echo "BASE_URL=${BASE_URL}"
echo "TAG=${TAG}"
echo "CONCURRENCY=${CONCURRENCY} REQUESTS=${REQUESTS} TIMEOUT=${TIMEOUT}s"
echo "DO_GETS=${DO_GETS}"
echo "jq=$(have_jq && echo yes || echo no)"
echo "=============================="

echo "♨️ Warmup /health/full x${WARMUP}"
for _ in $(seq 1 "${WARMUP}"); do
  if [[ -n "${AUTH_HEADER}" ]]; then
    curl -sS --max-time "${TIMEOUT}" -o /dev/null -H "${AUTH_HEADER}" "${BASE_URL}/health/full" || true
  else
    curl -sS --max-time "${TIMEOUT}" -o /dev/null "${BASE_URL}/health/full" || true
  fi
done

if [[ "${DO_GETS}" == "1" ]]; then
  run_load_get "/health/full" "${BASE_URL}/health/full"
  run_load_get "/xray/status" "${BASE_URL}/xray/status"
  run_load_get "/inbounds/${TAG}/users/count" "${BASE_URL}/inbounds/${TAG}/users/count"
  run_load_get "/inbounds/${TAG}/emails" "${BASE_URL}/inbounds/${TAG}/emails"
fi

echo
echo "✅ Load test done."
