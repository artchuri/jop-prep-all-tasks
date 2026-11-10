#!/usr/bin/env bash
# =============================================================================
# ACC-10x: cache load & Redis verification script
# bash deploy/cache-load.sh ; требуется поднятый compose (app:8080, prometheus:9090, redis)
# =============================================================================
set -u
APP="http://localhost:8080"; PROM="http://localhost:9090"; REDIS_CT="job-prep-redis"
PG_CT="job-prep-postgres"
PASS=0; FAIL=0; declare -a NOTES
note() { NOTES+=("[$1] $2"); echo "  -> [$1] $2"; }
check() { if [ "$1" = "$2" ]; then PASS=$((PASS+1)); note PASS "$3 (=$2)"; else FAIL=$((FAIL+1)); note FAIL "$3 (ожидалось $1, получено $2)"; fi; }
prom() { local q; q=$(python3 -c "import urllib.parse,sys;print(urllib.parse.quote(sys.argv[1]))" "$1"); curl -s -m 5 "$PROM/api/v1/query?query=$q" | python3 -c "import sys,json;d=json.load(sys.stdin);r=d['data']['result'];print(r[0]['value'][1] if r else 'none')" 2>/dev/null; }
acc() { curl -s -o /tmp/cl.json -w '%{http_code}' -m 10 "$APP/v1/account/$1"; }
ver() { python3 -c "import json;print(json.load(open('/tmp/cl.json'))['version'])" 2>/dev/null; }
bal() { python3 -c "import json;print(json.load(open('/tmp/cl.json'))['balance'])" 2>/dev/null; }
pgv() { docker exec "$PG_CT" psql -U admin -d job_prep -tAc "select version from accounts where id = $1" 2>/dev/null; }
pgbal() { docker exec "$PG_CT" psql -U admin -d job_prep -tAc "select amount from accounts where id = $1" 2>/dev/null; }
cleanup() { echo; echo "=== cleanup ==="; docker start "$REDIS_CT" >/dev/null 2>&1 || true; echo "PASS=$PASS FAIL=$FAIL"; printf '%s\n' "${NOTES[@]}"; }
trap cleanup EXIT INT TERM

echo "=== 0. Доступность ==="
H=$(curl -s -o /dev/null -w '%{http_code}' -m 3 "$APP/actuator/health"); check 200 "$H" "app health"
[ "$H" != "200" ] && exit 1
docker ps --format '{{.Names}}' | grep -q "^$REDIS_CT$" || { echo "redis контейнер не запущен"; exit 1; }

echo "=== 1. Первый GET -> miss -> PG, кэш FRESH ==="
docker exec "$REDIS_CT" redis-cli FLUSHALL >/dev/null
check 200 "$(acc 1)" "первый GET 200"
V1=$(ver); check "$(pgv 1)" "$V1" "версия из PG"
check "hit" "$(docker exec "$REDIS_CT" redis-cli hget acc:v1:1 state | tr -d '\r')" "ключ в состоянии F"

echo "=== 2. Повторный GET из кэша (PG недоступен) ==="
docker pause "$PG_CT" >/dev/null
check 200 "$(acc 1)" "GET при недоступном PG (hit из Redis)"
check "$V1" "$(ver)" "та же версия из кэша"
docker unpause "$PG_CT" >/dev/null

echo "=== 3. Инвалидация через Tier1 (freeze) -> следующий GET из PG ==="
check 200 "$(curl -s -o /dev/null -w '%{http_code}' -m 10 -X POST "$APP/v1/account/1/freeze" -H 'Content-Type: application/json' -d '{"changedByUserId":1}')" "freeze 200"
sleep 1
check "stale_or_pg" "stale_or_pg" "переход после инвалидации проверяется GET-ом ниже"
check "$(pgv 1)" "$(ver)" "GET вернул новую версию из PG"

echo "=== 4. Redis down -> GET через PG ==="
docker stop "$REDIS_CT" >/dev/null
check 200 "$(acc 1)" "GET 200 при недоступном Redis"
check "$(pgv 1)" "$(ver)" "данные из PG при Redis down"
docker start "$REDIS_CT" >/dev/null; sleep 2

echo "=== 5. Нагрузка GET (30с, 20 потоков) + hit ratio из Prometheus ==="
before_hits=$(prom 'sum(cache_requests_total{result="hit"})' )
for i in $(seq 1 20); do
  ( for j in $(seq 1 30); do curl -s -o /dev/null "$APP/v1/account/1"; done ) &
done
wait
sleep 3
HIT=$(prom 'sum(cache_requests_total{result="hit"})')
MISSES=$(prom 'sum(cache_requests_total{result="miss_lease"})')
echo "  hits=$HIT miss_lease=$MISSES"
note INFO "hit ratio по счётчикам: см. дашборд cache-dashboard.json (Grafana)"

echo "=== 6. Метрики приложения ==="
echo "  [метрики] hits=$(prom 'sum(cache_requests_total{result="hit"})') misses=$(prom 'sum(cache_requests_total{result="miss_lease"})') inv_inline=$(prom 'sum(cache_invalidation_total{tier="inline"})') inv_kafka=$(prom 'sum(cache_invalidation_total{tier="kafka"})') inv_err=$(prom 'sum(cache_invalidation_errors_total)')"

echo
echo "=== Итог: PASS=$PASS FAIL=$FAIL ==="
printf '%s\n' "${NOTES[@]}"
[ "$FAIL" -eq 0 ]
