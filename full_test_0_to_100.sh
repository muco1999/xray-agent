#!/usr/bin/env bash
set -euo pipefail

# =========================================
# Full test: from 0 to 100 issued clients
# =========================================
# Requirements: bash, curl
# Optional: jq
#
# ENV:
#   BASE_URL="http://194.135.33.76:18000"
#   TOKEN="..."
#   TAG="vless-in"
#
# Params:
#   N=100                 # сколько клиентов выдать
#   ISSUE_CONCURRENCY=10  # параллельные issue
#   POLL_CONCURRENCY=25   # параллельные poll
#   TIMEOUT=20            # curl max-time
#   POLL_TIMEOUT=60       # max seconds per job
#   POLL_INTERVAL=1       # seconds
#   CLEANUP=0|1           # удалить созданных клиентов в конце
#
# Example:
#   BASE_URL="http://194.135.33.76:18000" TOKEN="..." TAG="vless-in" \
#   N=100 ISSUE_CONCURRENCY=10 POLL_CONCURRENCY=25 CLEANUP=0 \
#   ./full_test_0_to_100.sh

BASE_URL="${BASE_URL:-http://127.0.0.1:18000}"
TOKEN="${TOKEN:-}"
TAG="${TAG:-vless-in}"

N="${N:-100}"
ISSUE_CONCURRENCY="${ISSUE_CONCURRENCY:-10}"
POLL_CONCURRENCY="${POLL_CONCURRENCY:-25}"

TIMEOUT="${TIMEOUT:-20}"
POLL_TIMEOUT="${POLL_TIMEOUT:-60}"
POLL_INTERVAL="${POLL_INTERVAL:-1}"

CLEANUP="${CLEANUP:-0}"

have_jq() { command -v jq >/dev/null 2>&1; }

AUTH=()
if [[ -n "${TOKEN}" ]]; then
  AUTH=(-H "Authorization: Bearer ${TOKEN}")
else
  echo "⚠️ TOKEN пустой. Если require_token включён — будут 401."
fi

ts_ms() { date +%s%3N; }

json_pretty() {
  if have_jq; then jq .; else cat; fi
}

health_check() {
  echo "➡️  GET /health/full"
  curl -sS --max-time "${TIMEOUT}" "${AUTH[@]}" "${BASE_URL}/health/full" | json_pretty
}

status_check() {
  echo "➡️  GET /xray/status"
  curl -sS --max-time "${TIMEOUT}" "${AUTH[@]}" "${BASE_URL}/xray/status" | json_pretty
}

count_check() {
  echo "➡️  GET /inbounds/${TAG}/users/count"
  curl -sS --max-time "${TIMEOUT}" "${AUTH[@]}" "${BASE_URL}/inbounds/${TAG}/users/count" | json_pretty
}

issue_one() {
  # prints: "email job_id issue_ms"
  local i="$1"
  local t0 t1 email resp job_id

  email="loadtest_$(date +%s)_${i}"

  t0="$(ts_ms)"
  resp="$(
    curl -sS --max-time "${TIMEOUT}" \
      -X POST "${BASE_URL}/clients/issue?async=true" \
      "${AUTH[@]}" \
      -H "Content-Type: application/json" \
      --data "{\"telegram_id\":\"${email}\",\"inbound_tag\":\"${TAG}\",\"level\":0,\"flow\":\"xtls-rprx-vision\"}"
  )"
  t1="$(ts_ms)"

  if have_jq; then
    job_id="$(echo "${resp}" | jq -r '.job_id // empty' 2>/dev/null || true)"
  else
    job_id="$(echo "${resp}" | sed -n 's/.*"job_id"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
  fi

  if [[ -z "${job_id}" ]]; then
    # no job_id -> mark as failed
    echo "${email} - $((t1 - t0))"
    return 0
  fi

  echo "${email} ${job_id} $((t1 - t0))"
}

poll_one() {
  # input: "email job_id issue_ms"
  # prints: "email job_id state total_ms"
  local email job_id issue_ms
  read -r email job_id issue_ms

  if [[ "${job_id}" == "-" || -z "${job_id}" ]]; then
    echo "${email} - error $((issue_ms))"
    return 0
  fi

  local start now st state
  start="$(date +%s)"
  while true; do
    now="$(date +%s)"
    if (( now - start > POLL_TIMEOUT )); then
      echo "${email} ${job_id} timeout $((issue_ms))"
      return 0
    fi

    st="$(
      curl -sS --max-time "${TIMEOUT}" \
        -X GET "${BASE_URL}/jobs/${job_id}" \
        "${AUTH[@]}" \
      || true
    )"

    if have_jq; then
      state="$(echo "${st}" | jq -r '.state // empty' 2>/dev/null || true)"
    else
      state="$(echo "${st}" | sed -n 's/.*"state"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n1)"
    fi

    if [[ "${state}" == "done" ]]; then
      echo "${email} ${job_id} done $((issue_ms))"
      return 0
    fi
    if [[ "${state}" == "error" ]]; then
      echo "${email} ${job_id} error $((issue_ms))"
      return 0
    fi

    sleep "${POLL_INTERVAL}"
  done
}

cleanup_one() {
  # input: email
  local email="$1"
  curl -sS --max-time "${TIMEOUT}" -o /dev/null \
    -X DELETE "${BASE_URL}/clients/${email}?inbound_tag=${TAG}" \
    "${AUTH[@]}" \
    || true
}

p95() {
  # read numbers from stdin, output p95
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

echo "=============================="
echo "🧪 FULL TEST 0→${N}"
echo "BASE_URL=${BASE_URL}"
echo "TAG=${TAG}"
echo "ISSUE_CONCURRENCY=${ISSUE_CONCURRENCY} POLL_CONCURRENCY=${POLL_CONCURRENCY}"
echo "TIMEOUT=${TIMEOUT}s POLL_TIMEOUT=${POLL_TIMEOUT}s"
echo "CLEANUP=${CLEANUP}"
echo "jq=$(have_jq && echo yes || echo no)"
echo "=============================="

echo "✅ Step 1: health/status/count"
health_check
status_check
count_check

echo
echo "✅ Step 2: issue ${N} jobs (enqueue)"
tmp_jobs="$(mktemp)"
t0="$(date +%s)"

export -f issue_one ts_ms have_jq
export BASE_URL TAG TIMEOUT
# AUTH array can't export directly; embed via TOKEN in subshell by rebuilding AUTH
export TOKEN

seq 1 "${N}" | xargs -P "${ISSUE_CONCURRENCY}" -I{} bash -c '
  AUTH=()
  if [[ -n "'"${TOKEN}"'" ]]; then AUTH=(-H "Authorization: Bearer '"${TOKEN}"'"); fi
  issue_one "$1"
' _ {} >> "${tmp_jobs}"

echo "📌 Jobs created: $(wc -l < "${tmp_jobs}")"
echo "Sample:"
head -n 5 "${tmp_jobs}" || true

echo
echo "✅ Step 3: poll all jobs until done/error"
tmp_res="$(mktemp)"

export -f poll_one have_jq
export BASE_URL TIMEOUT POLL_TIMEOUT POLL_INTERVAL TOKEN

cat "${tmp_jobs}" | xargs -P "${POLL_CONCURRENCY}" -I{} bash -c '
  AUTH=()
  if [[ -n "'"${TOKEN}"'" ]]; then AUTH=(-H "Authorization: Bearer '"${TOKEN}"'"); fi
  echo "{}" | poll_one
' >> "${tmp_res}"

t1="$(date +%s)"
dur="$((t1 - t0))"

done_cnt="$(awk '$3=="done"{c++} END{print c+0}' "${tmp_res}")"
err_cnt="$(awk '$3=="error"{c++} END{print c+0}' "${tmp_res}")"
to_cnt="$(awk '$3=="timeout"{c++} END{print c+0}' "${tmp_res}")"

echo "----------------------------------------"
echo "✅ Summary"
echo "Total=${N} done=${done_cnt} error=${err_cnt} timeout=${to_cnt}"
echo "Duration=${dur}s"
if (( dur > 0 )); then
  echo "Approx throughput: $(awk -v n="${N}" -v d="${dur}" 'BEGIN{printf "%.2f jobs/s", n/d}')"
fi

echo "p95(issue_ms) = $(awk '{print $4}' "${tmp_res}" | sort -n | p95) ms"
echo "max(issue_ms) = $(awk 'NR==1{m=$4} {if($4>m)m=$4} END{print m+0}' "${tmp_res}") ms"
echo "----------------------------------------"

echo
echo "✅ Step 4: count check after issuing"
count_check

if [[ "${CLEANUP}" == "1" ]]; then
  echo
  echo "🧹 Step 5: cleanup (remove created clients)"
  # extract emails from tmp_res
  cut -d' ' -f1 "${tmp_res}" | xargs -P 20 -I{} bash -c '
    cleanup_one "$1"
  ' _ {}
  echo "✅ Cleanup done"
  echo "➡️  Count after cleanup:"
  count_check
fi

rm -f "${tmp_jobs}" "${tmp_res}"
echo
echo "✅ FULL TEST DONE."
