# Конфигурация

Единственный активный конфиг: `src/main/resources/application.yaml`. Для деплоя часть значений переопределяется переменными окружения в `deploy/compose.yaml`.

## Порты и сервисы (deploy/compose.yaml)

| Сервис | Порт хоста | Назначение |
|---|---|---|
| app-a / app-b | 8081 / 8082 | Инстансы приложения (внутри — 8080) |
| postgres | 5432 | PostgreSQL 17, БД `job_prep` (admin/admin) |
| redis | 6379 | Redis 7, общий кэш обоих инстансов |
| kafka | 9092 | Apache Kafka 3.9 (внутри 29092) |
| prometheus | 9090 | Скрейп: `app-a:8080`, `app-b:8080`, `postgres-exporter:9187`, `redis-exporter:9121` |
| grafana | 3000 | Дашборды (admin/admin): Account Cache (ACC-10x), Outbox |
| redis-exporter | 9121 | Метрики Redis для дашборда |
| postgres-exporter | 9187 | Метрики PostgreSQL |

Локальный запуск одного инстанса из IDE — по кнопке Spring Boot run config; ему нужен поднятый compose (pg/redis/kafka). Если IDE-запуск жалуется «docker не видит» — проверь, что Docker Desktop запущен и `docker context` указывает на него (`docker context ls`).

## account-cache.* (кэш аккаунтов)

| Ключ | Дефолт | Смысл |
|---|---|---|
| `ttl-ms` | 60000 | Базовый TTL записи — жёсткая верхняя граница устаревания |
| `ttl-jitter-ms` | 9000 | Двусторонний разброс: фактический TTL = ttl ± jitter (51–69 с), против lockstep-истечения |
| `lease-ms` | 3000 | Lease читателя, взявшегося грузить из PG при miss |
| `margin-ms` | 1000 | Время сверх lease, за которое filler обязан успеть записать |
| `stale-ms` | 5000 | Окно, в котором инвалидированная запись ещё может отдаваться (stale-read) |
| `wait-attempts` | 3 | Сколько раз WAIT-читатель перепроверяет кэш |
| `wait-pause-ms` | 20 | Пауза между перепроверками |

В compose задаётся переменными `ACCOUNT_CACHE_*` для каждого инстанса: граница устаревания — решение деплоя, а не константа кода. Оба инстанса обязаны иметь одинаковые значения.

## resilience4j.circuitbreaker.instances.outbox-kafka.*

Circuit breaker доставки outbox в Kafka: open после 5 последовательных фейлов отправки, 30 с до half-open, 1 пробная отправка. При OPEN `OutboxPublisher.poll()` пропускает claim — строки остаются NEW, попытки не сгорают. Пороги можно крутить без пересборки.

## Kafka (spring.kafka.*)

Producer: acks=all, idempotence on, max.in.flight=2, delivery.timeout 120 с. Consumer: group `account-cache-invalidation`, auto-offset-reset=latest, ручной ack. Топик: `account-events`, ключ = userId (порядок событий по пользователю).

## Outbox (константы, GlobalValues)

| Константа | Значение |
|---|---|
| Размер батча claim | 10 |
| Lease publisher'а | 10 с |
| MAX_ATTEMPTS | 5 (дальше REJECTED) |
| Backoff | 2^attempt секунд, потолок 300 с |
| Reclaimer | раз в 60 с, REJECTED старше 5 мин → NEW |
| Cleanup cron | `0 0 3 * * *`, ретенция 2 месяца |
| DDL lock | advisory lock `outbox_partition_ddl` |

## Health и авто-конфигурации

Стартеры, требующие отсутствующей инфры (JobRunr/MongoDB, Spring AI S3, Eureka), исключены через `spring.autoconfigure.exclude` — без этого оба инстанса не стартуют. Health-индикаторы `mongodb`/`elasticsearch`/`jobrunr` выключены: агрегатный health отражает только реальный стек (pg, redis, kafka).

## Метрики

Expose: `/actuator/prometheus`. Полный список и панель-за-панелью — [observability.md](observability.md).
