#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8081}"
KEY="${RATE_LIMIT_CLIENT_1_API_KEY:?set it via deploy/.env}"
LIMIT_PER_MINUTE="${LIMIT_PER_MINUTE:-6000}"
OVER_PARALLEL="${OVER_PARALLEL:-50}"

echo "actuator (no key): $(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/actuator/health")"

if [[ "${1:-nominal}" == "nominal" ]]; then
  echo "--- nominal: 20 GETs under the limit ---"
  for _ in $(seq 20); do
    curl -s -o /dev/null -w "%{http_code}\n" -H "X-API-Key: $KEY" "${BASE_URL}/v1/account/1"
  done | sort | uniq -c
elif [[ "$1" == "over" ]]; then
  n=$((LIMIT_PER_MINUTE + 50))
  echo "--- over: ${n} parallel GETs against a ${LIMIT_PER_MINUTE}/min window (expect a 429 tail) ---"
  seq "$n" | xargs -P "$OVER_PARALLEL" -I{} curl -s -o /dev/null -w "%{http_code}\n" \
    -H "X-API-Key: $KEY" "${BASE_URL}/v1/account/1" | sort | uniq -c
  echo "--- 401 without key must never consume the limit ---"
  echo "no-key status: $(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/v1/account/1")"
else
  echo "usage: $0 [nominal|over]" >&2
  exit 1
fi

echo "--- metrics probe ---"
curl -s "${BASE_URL}/actuator/prometheus" | grep -E '^rate_limit_(requests_total|window_usage_ratio|outage_total)' | head -10
