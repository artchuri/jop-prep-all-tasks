# Наблюдаемость

Prometheus скрейпит оба инстанса (`app-a:8080`, `app-b:8080` в сети compose), Grafana — два дашборда: **Account Cache (ACC-10x)** и **Outbox: доставка событий в Kafka**. Все счётчики регистрируются eagerly при старте: свежеподнятый инстанс отдаёт нули, а не «no data».

## Кэш (cache-dashboard.json)

| Метрика | Лейблы | Что показывает |
|---|---|---|
| `cache_requests_total` | `result` = hit / stale / miss_lease / wait | Исход каждого GET по кэшу |
| `cache_redis_latency_seconds` | `op` = read/invalidate, `outcome` = success/error | Латентность Redis-вызовов (гистограмма; p50/p99 через `histogram_quantile`) |
| `cache_errors_total` | `operation` | Ошибки Redis, пойманные `SuppressingCacheErrorHandler` |
| `cache_evict_total` | `result` = success/error | Инвалидации Tier 1 (inline, after-commit) |
| `cache_invalidation_total` | `tier` = kafka, `result` | Инвалидации Tier 2 (consumer) |
| `cache_invalidation_errors_total` | `tier`, `reason` | Битые payload'ы (ack без ретрая) |

Панели: hit ratio, p50/p99 latency (read/invalidate), invalidation errors, invalidations inline vs kafka.

## Outbox (outbox-dashboard.json)

| Метрика | Лейблы | Что показывает |
|---|---|---|
| `outbox_events_new_total` | `event_type` | Созданные события (в транзакции) |
| `outbox_events_published_total` | `event_type` | Доставленные в Kafka |
| `outbox_events_rejected_total` | `event_type` | REJECTED после 5 попыток |
| `outbox_pending` | `status` | Текущие NEW/PROCESSING/REJECTED |
| `outbox_delivery_seconds` | `event_type` | Латентность: событие → Kafka (histogram) |
| `outbox_poll_skipped_breaker_open` | — | Пропуски poll при OPEN breaker |
| `outbox_breaker_transitions_total` | `from`, `to` | Переходы состояний breaker |
| `outbox_recovered` | — | Строки, возвращённые reclaimer'ом |
| `kafka_producer_record_send_total` / `record_error_total` | client-метрики | Kafka producer (появляются после первого send) |

Панели: lost-event check (`increase(new[30m]) - increase(published[30m])` — главное «нет ли потери»), throughput created vs published (1m), Kafka producer.

## Health

`GET /actuator/health` — агрегат по реальному стеку (pg, redis, kafka); `/readiness`, `/liveness` — для роутинга. Индикаторы отсутствующих сервисов (mongo/es/jobrunr) отключены — см. [configuration.md](configuration.md#health-и-авто-конфигурации).

## Нагрузочные/проверочные скрипты

| Скрипт | Что делает |
|---|---|
| `deploy/cache-load.sh` | Нагрузка GET по кэшу + проверка счётчиков/Redis |
| `deploy/cache-cross-instance.sh` | Доказывает кросс-инстансную инвалидацию: запись на A, проверка stale на B (PTTL ≤ 69 с) |
| `deploy/outbox-load.sh` | Генерация событий, контроль delivered/lost |

## Как проверить руками

```bash
cd deploy && docker compose up -d
# трафик + событие
for i in 1 2 3; do curl -s http://localhost:8081/v1/account/1 >/dev/null; done
curl -X POST localhost:8081/v1/account/1/freeze -H 'Content-Type: application/json' -d '{"changedByUserId":1}'
# все таргеты Prometheus должны быть up
curl -s localhost:9090/api/v1/targets | jq '.data.activeTargets[] | {job: .labels.job, health}'
```

Grafana: http://localhost:3000 (admin/admin).
