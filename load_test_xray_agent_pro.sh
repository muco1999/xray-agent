#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:18000}"
TOKEN="${TOKEN:-}"
TAG="${TAG:-vless-in}"

# concurrency = сколько одновременных curl процессов
CONCURRENCY="${CONCURRENCY:-300}"
# total requests across ALL endpoints
REQUESTS="${REQUESTS:-20000}"
# max time per request
TIMEOUT="${TIMEOUT:-30}"
# warmup /health/full count
WARMUP="${WARMUP:-5}"

# Traffic mix (percent, sum must be 100)
P_STATUS="${P_STATUS:-70}"     # /xray/status
P_COUNT="${P_COUNT:-25}"       # /inbounds/.../count
P_EMAILS="${P_EMAILS:-5}"      # /inbounds/.../emails (тяжёлый)

# Reduce logs noise
QUIET_CURL="${QUIET_CURL:-1}"  # 1=не печатать curl errors в консоль (считаем в статистике)

have_jq() { command -v jq >/dev/null 2>&1; }

AUTH_HEADER=""
if [[ -n "${TOKEN}" ]]; then
  AUTH_HEADER="Authorization: Bearer ${TOKEN}"
else
  echo "⚠️ TOKEN пустой. Если require_token включён — будут 401."
fi

ts_ms() { date +%s%3N; }

curl_one() {
  # prints: "endpoint code dur_ms"
  local endpoint="$1"; shift
  local url="$1"; shift

  local t0 t1 code
  t0="$(ts_ms)"

  if [[ "${QUIET_CURL}" == "1" ]]; then
    if [[ -n "${AUTH_HEADER}" ]]; then
      code="$(curl -sS --max-time "${TIMEOUT}" -o /dev/null -w "%{http_code}" -H "${AUTH_HEADER}" "${url}" 2>/dev/null || echo "000")"
    else
      code="$(curl -sS --max-time "${TIMEOUT}" -o /dev/null -w "%{http_code}" "${url}" 2>/dev/null || echo "000")"
    fi
  else
    if [[ -n "${AUTH_HEADER}" ]]; then
      code="$(curl -sS --max-time "${TIMEOUT}" -o /dev/null -w "%{http_code}" -H "${AUTH_HEADER}" "${url}" || echo "000")"
    else
      code="$(curl -sS --max-time "${TIMEOUT}" -o /dev/null -w "%{http_code}" "${url}" || echo "000")"
    fi
  fi

  t1="$(ts_ms)"
  echo "${endpoint} ${code} $((t1 - t0))"
}

percentile_p95() {
  awk '
    {a[NR]=$1}
    END{
      if(NR==0){print 0; exit}
      n=asort(a)
      idx=int((n*95+99)/100)
      if(idx<1) idx=1
      if(idx>n) idx=n
      print a[idx]
    }'
}

summarize_endpoint() {
  local name="$1"
  local tmp="$2"

  local total ok err min max avg p95
  total="$(awk -v n="${name}" '$1==n{c++} END{print c+0}' "${tmp}")"
  ok="$(awk -v n="${name}" '$1==n && $2>=200 && $2<300{c++} END{print c+0}' "${tmp}")"
  err="$((total - ok))"

  min="$(awk -v n="${name}" '$1==n{print $3}' "${tmp}" | awk 'NR==1{m=$1} {if($1<m)m=$1} END{print m+0}')"
  max="$(awk -v n="${name}" '$1==n{print $3}' "${tmp}" | awk 'NR==1{m=$1} {if($1>m)m=$1} END{print m+0}')"
  avg="$(awk -v n="${name}" '$1==n{s+=$3;c++} END{ if(c==0) print 0; else printf "%.2f", s/c }' "${tmp}")"
  p95="$(awk -v n="${name}" '$1==n{print $3}' "${tmp}" | sort -n | percentile_p95)"

  echo "----------------------------------------"
  echo "📌 ${name}"
  echo "  total=${total} ok=${ok} err=${err} err%=$(awk -v e="${err}" -v t="${total}" 'BEGIN{ if(t==0) print "0.00"; else printf "%.2f", (e*100.0)/t }')"
  echo "  latency_ms: min=${min} avg=${avg} p95=${p95} max=${max}"
}

validate_mix() {
  local sum=$((P_STATUS + P_COUNT + P_EMAILS))
  if (( sum != 100 )); then
    echo "⛔ Traffic mix must sum to 100 (now=${sum})" >&2
    exit 2
  fi
}

echo "=============================="
echo "🔥 Xray Agent Load Test PRO"
echo "BASE_URL=${BASE_URL}"
echo "TAG=${TAG}"
echo "CONCURRENCY=${CONCURRENCY} REQUESTS=${REQUESTS} TIMEOUT=${TIMEOUT}s"
echo "MIX: status=${P_STATUS}% count=${P_COUNT}% emails=${P_EMAILS}%"
echo "jq=$(have_jq && echo yes || echo no)"
echo "=============================="

validate_mix

echo "♨️ Warmup /health/full x${WARMUP}"
for _ in $(seq 1 "${WARMUP}"); do
  if [[ -n "${AUTH_HEADER}" ]]; then
    curl -sS --max-time "${TIMEOUT}" -o /dev/null -H "${AUTH_HEADER}" "${BASE_URL}/health/full" || true
  else
    curl -sS --max-time "${TIMEOUT}" -o /dev/null "${BASE_URL}/health/full" || true
  fi
done

STATUS_URL="${BASE_URL}/xray/status"
COUNT_URL="${BASE_URL}/inbounds/${TAG}/users/count"
EMAILS_URL="${BASE_URL}/inbounds/${TAG}/emails"

tmp="$(mktemp)"
t0="$(date +%s)"

export -f curl_one ts_ms percentile_p95
export BASE_URL TAG TIMEOUT AUTH_HEADER STATUS_URL COUNT_URL EMAILS_URL QUIET_CURL

echo "🚀 Running mixed load..."
seq 1 "${REQUESTS}" | xargs -P "${CONCURRENCY}" -I{} bash -c '
  r=$((RANDOM % 100))
  if (( r < '"${P_STATUS}"' )); then
    curl_one "status" "'"${STATUS_URL}"'"
  elif (( r < '"$((P_STATUS + P_COUNT))"' )); then
    curl_one "count" "'"${COUNT_URL}"'"
  else
    curl_one "emails" "'"${EMAILS_URL}"'"
  fi
' >> "${tmp}"

t1="$(date +%s)"
dur="$((t1 - t0))"

echo
echo "✅ Finished in ${dur}s"
if (( dur > 0 )); then
  echo "📈 Approx RPS: $(awk -v r="${REQUESTS}" -v d="${dur}" 'BEGIN{printf "%.2f", r/d}')"
fi

summarize_endpoint "status" "${tmp}"
summarize_endpoint "count" "${tmp}"
summarize_endpoint "emails" "${tmp}"

rm -f "${tmp}"

echo
echo "✅ Load test done."
