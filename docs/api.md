# HTTP API

Base URL: `http://localhost:8081` (app-a) / `http://localhost:8082` (app-b). Оба инстанса идентичны и работают с одним Redis/PostgreSQL.

## Rate limiting (ACC-107)

Все бизнес-эндпоинты ниже защищены распределённым лимитером (дизайн-док ACC-107, лимиты в `rate-limit.*` конфиге):

- **`X-API-Key` обязателен** на каждый запрос к `/v1/**`. Нет или неизвестный ключ → `401` `UNAUTHORIZED` (ProblemDetail), лимит не расходуется. Ключи — allowlist `rate-limit.clients`, значение приходит из `RATE_LIMIT_CLIENT_1_API_KEY` (`deploy/.env`).
- **Лимиты на клиента в минуту** (окно 60 с, sliding window counter в Redis, **глобальный** для обоих инстансов): `GET /v1/account/{id}` — 6000, `POST /v1/account/{id}/freeze` — 600, `POST /v1/transfer` — 1200, незапротоколированный путь под `/v1` — default 600.
- Превышение → `429` `RATE_LIMITED` + заголовок `Retry-After` (секунды до расчётного освобождения слота).
- Успешные (2xx/3xx) запросы расходуют лимит. `4xx`/`5xx` **не** расходуют (слот возвращается, refund), **кроме `404`/`405`**: скан путей и методов считается намеренно (правка НФТ, согласована 2026-11-21).
- Redis лимитера недоступен → `503` `RATE_LIMITER_UNAVAILABLE` + `Retry-After` (fail-closed; аварийный fail-open — `rate-limit.outage-strategy: allow`).
- `/actuator/**` вне лимитера. Метрики и алерты — [observability.md](observability.md#rate-limiting-acc-107).

## GET /v1/account/{id}

Читает аккаунт через cache-aside (Redis; при miss — PostgreSQL). Ответ содержит `Cache-Control: no-store` (ответ может быть stale, кэшировать его выше нельзя).

```bash
curl -i -H "X-API-Key: $RATE_LIMIT_CLIENT_1_API_KEY" http://localhost:8081/v1/account/1
```

```json
{ "id": 1, "balance": "100.00", "status": "ACTIVE", "version": 0, "createdAt": "..." }
```

Ошибки: `404` — аккаунта нет. При недоступности Redis — fallback в PostgreSQL (ошибки кэша счётчиком, не 500).

## POST /v1/account/{id}/freeze

Заморозка аккаунта. Идемпотентна: повторный freeze уже-FROZEN аккаунта возвращает текущее состояние без второй инвалидации.

```bash
curl -X POST http://localhost:8081/v1/account/1/freeze \
  -H "X-API-Key: $RATE_LIMIT_CLIENT_1_API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"changedByUserId": 2}'
```

Эффекты в одной транзакции PG: статус → FROZEN, `version += 1`, запись в историю статусов, событие `ACCOUNT_FROZEN` в outbox. После commit: inline evict (Tier 1), затем Tier 2 через Kafka.

Ошибки: `409` — аккаунт CLOSED (перевод/заморозка недопустимы), `404`.

## POST /v1/transfer

Перевод между счетами. Требует заголовок `Idempotency-Key` (уникальный на операцию; повтор с тем же ключом возвращает результат первой, не исполняя повторно).

```bash
curl -X POST http://localhost:8082/v1/transfer \
  -H "X-API-Key: $RATE_LIMIT_CLIENT_1_API_KEY" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: 7b8c1f2e-...' \
  -d '{"recipientAccountId": 2, "userId": 1, "fromAccountId": 1, "amount": "25.00"}'
```

Порядок полей: `recipientAccountId, userId, fromAccountId, amount`.

Эффекты: обе записи под row lock (`SELECT FOR UPDATE`), статус-проверки (FROZEN/CLOSED отклоняют), достаточность средств, `version` обеих сторон инкрементируется, история переводов, событие `TRANSFER_COMPLETED` в outbox. Кэш при этом не читается — решения принимаются только по актуальному состоянию PG.

Ошибки: `409` — статус аккаунта не позволяет (FROZEN/CLOSED), `403` — перевод запрещён политикой (TransferForbidden), `422` — недостаточно средств, `400` — валидация (сумма > 0, поля заполнены), `409` `IdempotencyConflict` — тот же ключ с другим телом запроса. Все ошибки — ProblemDetail (RFC 7807).

## Известное ограничение доверия

`userId` в теле перевода и `changedByUserId` в freeze — доверенные поля: аутентификации/авторизации в проекте нет (учебный фокус — кэш и outbox, не безопасность). Любой клиент может перевести средства от имени любого пользователя. `X-API-Key` — не аутентификация, а идентификация клиента для лимитера.