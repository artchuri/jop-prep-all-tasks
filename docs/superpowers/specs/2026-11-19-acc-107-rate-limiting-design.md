# ACC-107 — Distributed Rate Limiting: дизайн-док

**Дата:** 2026-11-19
**Статус:** DRAFT (ожидает аппрува)
**Смежные доки:** [architecture.md](../../architecture.md), [observability.md](../../observability.md), [2026-11-13-spring-cache-adapter-design.md](2026-11-13-spring-cache-adapter-design.md)

## Intent / почему

Ограничить частоту запросов к API управления аккаунтами: защитить сервис от чрезмерной
нагрузки и неконтролируемого числа операций со стороны отдельных клиентов. Сервис живёт
в нескольких инстансах (app-a/app-b), поэтому лимит **глобальный** — счётчики в Redis,
общем для обоих инстансов.

## Goals / Non-goals / Anti-goals

**Goals**
- Глобальный per-client лимит на все бизнес-эндпоинты (GET /v1/account/{id},
  POST /v1/account/{id}/freeze, POST /v1/transfer).
- Слава 429 при превышении с Retry-After; метрики по каждому клиенту.
- Кластер-готовые ключи Redis (переезд на Redis Cluster = только конфиг).

**Non-goals**
- Лимит на aggregate-трафик всего сервиса («всего не ограничиваем»).
- Идемпотентность и replay-логика — вне лимитера.
- Миграция кеша аккаунтов на Redis Cluster — отдельный тикет.
- Автоподъём лимитов при пиках (см. Q9: только предсказуемые пики по расписанию).

**Anti-goals (что считаем провалом даже при работающем коде)**
- Петля «нагрузка → выше лимит → больше принято»: автоматическая эластичность лимитов.
- Лимитер, который делает успешный GET успешным из-за падения Redis (fail-closed выбран
  осознанно — см. Q7; НЕ «чинить» по аналогии с fail-open кеша).
- Overshoot: реальное число пропущенных 2XX за окно выше лимита.

## Constraints

- Стек проекта: Java 21, Spring Boot 4.1.x, Jedis, Lua, Micrometer/Prometheus/Grafana,
  Testcontainers + toxiproxy. Никаких новых инфраструктурных зависимостей.
- Redis сейчас одиночный (localhost:6379); кластер в перспективе.
- Аутентификации в проекте нет: идентификация клиента = X-API-Key allowlist.
- Латентный бюджет reserve/refund: p50 ≤ 1ms, p95 ≤ 3ms, p99 ≤ 15ms; детекция отказа ≤ 25ms на операцию (borrow max-wait, connect timeout, read timeout — каждый по 25ms; худший случай «borrow + connect» ≈ 50ms).

## Рассмотренные подходы

| Вариант | Суть | Плюсы сейчас | Плюсы потом | Минусы сейчас | Минусы потом | Вердикт |
|---|---|---|---|---|---|---|
| **A. Sliding window counter (2 ключа prev/curr, reserve/refund, Lua)** — ВЫБРАН | Взвешенная сумма prev+curr против лимита | каноничен проекту (Lua-файлы в resources/redis), атомарен одним EVAL, изоляция выделенного пула | cluster-ready ключи, профили пиков надстраиваются без переделки | 2 EVAL на refund-пути; reserve-then-refund нетривиален | hot-slot на кластере при перекосе трафика одного клиента (решение потом: sha-разбивка) | принят |
| B. GCRA / leaky bucket (redis-cell-style) | один ключ, burst+rate по timestamp | 1 EVAL, точный пейсинг | нет hot-slot | ломает тест-требование 59/61 (окна нет), «среднее/макс за окно» не построить | метрики окна невозможны | отклонён |
| C. Fixed window counter | INCR+EXPIRE | тривиален | — | 2× burst на границе окна; тест 59/61 провалит | мнимая простота | отклонён |
| D. Sliding window log (ZSET) | ZSET по timestamp на каждый запрос | точнее счётчика | — | O(N) память (6000 req/min/client), ZREMRANGEBYSCORE на каждый вызов | дороже всех при наших rps | отклонён |
| E. Spring Cloud Gateway RequestRateLimiter | вынести в шлюз | готовый фильтр | — | шлюза в проекте нет — новая инфраструктура ради одного тикета | двусторонняя связь шлюз↔сервис | отклонён |

Ключевое отклонение A от «учебникового» sliding counter: **reserve-then-refund**.
Счётчик включает зарезервированные слоты, поэтому классический overshoot
(~n×rps оценок при параллельных запросах) исключён в безопасную сторону: пропущено 2XX
≤ лимита; плата — ложные 429 при параллельных 4XX, которые refund'ятся.

## Зафиксированные решения (grill-сессия, Q1–Q11)

### Q1. Хранилище — Redis
Одиночный сейчас; ключи сразу cluster-ready; миграция кеша на кластер — отдельный тикет.
Плюсы сейчас: Redis уже развёрнут и для кеша, одна новая зависимость = ноль.
Плюсы потом: переезд = только конфиг. Минусы сейчас: общая судьба с кешем (один Redis).
Минусы потом: hot-slot при перекосе (см. A).

### Q2. Ключи — hash-теги
Ключи вида `{api-key-hash}:rate:{endpoint}` — фигурные скобки = hash-тег, все ключи
клиента в одном слоте кластера, EVAL атомарен и там же.
Плюсы сейчас: работает и на single, и на кластере без изменения кода.
Плюсы потом: миграция без рефакторинга. Минусы: hash-тег обязателен — забыт = сломанный кластер (покрывается константой-билдером ключей).

### Q3. Окно 60s, sliding window counter, лимиты ×60
freeze < 600/min, transfer < 1200/min, GET /v1/account/{id} < 6000/min — на каждого клиента.
«10 rps / 20 qps / 100 rps» из тикета нормализованы к окну.
Плюсы: соответствует тест-требованию 59/61 и метрикам «за окно».
Минусы потом: при смене окна — правка конфига + перезаход (статические свойства, Q9).

### Q4. Идентификация клиента — X-API-Key + allowlist в properties
Ключи клиентов через .env (${API_KEY_CLIENT_A} и т.п.), в properties — имена-ссылки.
Нет/неизвестный ключ → 401, лимитер не вызывается, запрос **не считается**.
Плюсы сейчас: честно отражает отсутствие auth в проекте, минимальный код.
Минусы потом: при появлении настоящего auth фильтр заменяется на auth-механизм (точка врезки одна).

### Q5. Таблица эндпоинтов
| Эндпоинт | Лимит/мин/клиент |
|---|---|
| GET /v1/account/{id} | 6000 |
| POST /v1/account/{id}/freeze | 600 |
| POST /v1/transfer | 1200 |
Actuator (/actuator/**) вне лимитера. Незапротоколированный путь → default-бакет.
404 по несуществующему accountId считается по бакету эндпоинта (роут существует).
Идемпотентность вне лимитера: лимитер не знает про replay.

### Q6. Reserve-then-refund + правило 404/405
Порядок: reserve до хендлера (атомарно, лимит включает резерв) → после ответа refund,
если статус 400/409/422/4XX-валидационный/5XX (не считаются). **Исключение: 404/405 —
без refund** (путь не существует / метод не поддерживается: лимит — защита от скана).
Превышение при reserve → 429 + Retry-After, инкремента нет, rejected-метрика.
Плюсы: overshoot исключён инвариантом, а не оценкой.
Минусы: 2 EVAL на refund-пути; правило 404/405 требует комментария в коде (неочевидно).

### Q7. Отказ Redis — fail-closed
Redis недоступен → 503 + Retry-After, `rate_limit_outage_total`; переключатель
`rate-limit.outage-strategy: reject|allow` (allow = аварийный fail-open вручную).
Это **третья** fail-семантика проекта (кеш — fail-open на PG; outbox — ретраи) —
зафиксирована в доке, чтобы её не «починили» по аналогии с кешем.
Плюсы: защита не отключается сама при аварии; клиенты получают явный сигнал ретрая.
Минусы: Redis становится жёсткой зависимостью API (mitigated: toggle allow + алерт).

### Q8. Метрики
База: `rate_limit_requests_total{client,endpoint,result=accepted|rejected}`,
`rate_limit_refunds_total{client,endpoint}`, `rate_limit_eval_time{op=reserve|refund,outcome}` (гистограмма), `rate_limit_outage_total`.
Сверх базы: `rate_limit_window_usage_ratio{client,endpoint}` (gauge 0..1 — база алертов),
`rate_limit_in_flight_reservations`, `rate_limit_redis_errors_total{op}`.
«Среднее + макс кол-во запросов» считаются в Prometheus из сырых счётчиков
(avg/max_over_time) — Redis хранит только счётчик окна. Регистрация eager при старте
(конвенция observability.md). Алерты: usage_ratio > 0.9 за 5м, p99 eval > 50ms, outage > 0.


**Формат (итоговая реализация, ноль дефолтов):**

```yaml
# НОЛЬ дефолтов: каждое значение задано явно; отсутствие значения или env-переменной
# роняет старт приложения (fail-fast). Api-ключ приходит только из окружения (.env).
rate-limit:
  enabled: true                      # kill-switch, false только для отката
  outage-strategy: reject            # reject|allow (Q7)
  path-prefix: /v1
  window-ms: 60000
  default-limit-per-minute: 600
  outage-retry-after-seconds: 30
  pool-max-active: 16
  pool-max-idle: 8
  pool-min-idle: 2
  pool-command-timeout-ms: 25
  clients:
    - name: local
      api-key: ${RATE_LIMIT_CLIENT_1_API_KEY}   # без fallback (Q4)
  endpoints:                         # rps -> x60 при окне 60s (Q3)
    - id: get-account
      method: GET
      pattern: "/v1/account/*"
      limit-per-minute: 6000
    - id: freeze
      method: POST
      pattern: "/v1/account/*/freeze"
      limit-per-minute: 600
    - id: transfer
      method: POST
      pattern: "/v1/transfer"
      limit-per-minute: 1200
  peak-profiles: []                  # cron + duration + multiplier <= 3 (Q9)
```

Правило отсутствия дефолтов добавлено по ревью: скрытые дефолты в @DefaultValue и
fallback-значения в ${VAR:default} маскируют пропущенную конфигурацию; теперь пропущенное
значение = падение на старте с точным сообщением, какое поле задать.
### Q9. Конфигурация — статик + профили пиков по расписанию
`RateLimitProperties` (@ConfigurationProperties, образец — AccountCacheProperties):
enabled, outage-strategy, default-limit-per-minute, clients{key→name}, endpoints{path→limit},
peak-profiles[{cron, duration, multiplier ≤ 3}]. Хот-релоада нет (рестарт = применение).
Пики — только предсказуемые по расписанию (решение A): множитель к базовому лимиту.
Лимит передаётся в Lua параметром (не зашит в скрипт) — профиль меняется без redeploy скрипта.
Отказ от автоэластичности (C) — anti-goal: петля обратной связи против назначения тикета.
Тест: «пик кончился в середине окна» → 429 до дренирования окна.

### Q10. Латентность и пул
Выделенный Jedis-пул лимитера: 16 коннектов, max-wait 25ms (не делит пул с кешем —
шторм кеша не душит лимитер и наоборот). Пороги: p50 ≤ 1ms, p95 ≤ 3ms, p99 ≤ 15ms,
детекция отказа ≤ 50ms → 503 (borrow 25ms + connect/read 25ms каждый). Алерт p99 > 50ms.
Плюсы: изоляция отказов и латентности двух Redis-потребителей.
Минусы: +1 пул на инстанс (16 коннектов — приемлемо).

### Q11. Тесты — матрица
Unit: U1 fail-fast валидации properties (пустой/дубль api-key, лимит ≤ 0, multiplier > 3);
U2 профиль пиков (cron вкл/выкл, множитель, потолок); U3 фильтр: нет/неверный X-API-Key →
401 без вызова лимитера; U4 refund-правило (400/409/422/500 → refund; 404/405 → без refund).
Integration (Testcontainers Redis + toxiproxy, прецеденты RedisOutageGetIT, CrossInstanceLeaseWaitIT):
I1 N < limit → все 2XX; I2 N > limit → 429 + Retry-After + rejected-метрика;
I3 комбинация эндпоинтов (исчерпал freeze → GET жив); I4 59/61сек через **управляемое время**;
I5 два инстанса → общий лимит; I6 refund возвращает слот; I7 toxiproxy рвёт Redis →
503 + Retry-After + outage-метрика, восстановление, enabled=false обходит лимитер;
I8 пик вкл/выкл в середине окна. Вне CI: deploy/rate-limit-load.sh (конвенция deploy/*.sh).

### Q12 (финал). Время в Lua — параметром
reserve/refund принимают `now` аргументом (мс) — управляемое время для I4.
Это осознанное отклонение от конвенции кеша (`redis.call('TIME')`), фиксируется
комментарием в шапке обоих скриптов. Плата: приложение отвечает за корректность часов
(дрейф между инстансами допустим — окно 60s, дрейф NTP ≪ порогов).

## Что осталось билдеру (не решено дизайн-доком)

- Точная структура пакетов: `ratelimit` (filter, scripts, RateLimiter, properties, metrics) —
  по образцу `utils.cache` / `outbox`; имя пакета на усмотрение ревью.
- Формат ответа 429/503 (body) — по конвенции GlobalExceptionHandler.
- Значения hash-тега: sha256(api-key) — длина префикса.

## Разбивка на подзадачи (порядок = зависимости)

| # | Подзадача | Закрывает | Содержание |
|---|---|---|---|
| 1 | ACC-107a: Properties + allowlist + 401-фильтр | Q4, Q9, U1, U3 | RateLimitProperties, ApiKeyFilter, fail-fast валидация |
| 2 | ACC-107b: Lua reserve/refund + RateLimiter + выделенный пул | Q2, Q3, Q6, Q10, Q12, I4 | rate_limit_reserve.lua / rate_limit_refund.lua (now параметром), JedisConnectionFactory отдельный, метрика eval_time |
| 3 | ACC-107c: Интеграция в фильтр (reserve-then-refund) | Q5, Q6, U4 | порядок reserve→handler→refund, 404/405-правило, Retry-After, 429/503 через GlobalExceptionHandler |
| 4 | ACC-107d: Метрики + дашборд + алерты | Q8 | eager-регистрация, rate-limit-dashboard.json, alerts.yaml |
| 5 | ACC-107e: Профили пиков | Q9, I8, U2 | cron-профили, множитель, тест «пик кончился в середине окна» |
| 6 | ACC-107f: Fail-closed 503 + toggle | Q7, I7 | outage-strategy, toxiproxy IT |
| 7 | ACC-107g: Матрица тестов + нагрузочный скрипт | U1–U4, I1–I8, deploy | добить матрицу, rate-limit-load.sh |

Зависимости: 1 → 2 → 3 → (4, 5, 6) → 7.


## Отклонения реализации от текста (зафиксировано 2026-11-21)


1. **404/405 считаются без refund** (уточнение Q5/Q6): скан путей и методов намеренно расходует лимит; прочие 4xx/5xx рефандятся. Согласовано как правка НФТ тикета.

2. **Счётчики outage регистрируются на старте** (не лениво на первой ошибке): конвенция observability.md — zero вместо «no data» в дашборде.

3. **NFT «avg/max per client» считается в Prometheus** из `rate_limit_requests_total` (Redis хранит только счётчик окна, истории per-client в нём нет, Q8): панели `avg_over_time`/`max_over_time` в дашборде.

4. **Алерт-база «клиент упёрся в лимит»**: `usage_ratio` — gauge, замораживается при молчащем клиенте; дополнен активностным `ClientRejectedSpike` по `rate_limit_requests_total{result="rejected"}`.

5. **Boot 4**: HTTP-клиент rate-limit IT — plain `java.net.http.HttpClient` с фиксированными портами (18083/18084) и `webEnvironment = DEFINED_PORT`: у Boot 4 бин `TestRestTemplate` инстанцируется до старта встроенного сервера и не разрешает случайный порт в нашей конфигурации; аннотация подкласса перебивает базовую `RANDOM_PORT`.

6. **Binder не мержит сложные списки между источниками** — переопределение `rate-limit.endpoints[0].*` в тестах повторяет запись целиком, а не только лимит.
7. **Тела 429/503 пишет фильтр напрямую** (ProblemDetail, problem+json, свойство `code`): `@RestControllerAdvice` не покрывает ответы, сгенерированные до диспетчеризации; формат совместим с конвенцией GlobalExceptionHandler.
8. **Метрика латентности EVAL называется `rate_limit_eval_seconds`** (в Q8 — `rate_limit_eval_time`): гистограмма с базовой единицей seconds, имя согласовано во всех доках/алертах/дашборде.
9. **Бюджет детекции отказа уточнён**: 25 мс — это max-wait borrow **и отдельно** connect/read timeout; худший случай «borrow + connect» ≈ 50 мс, а не ≤25 мс суммарно, как читался исходный пункт ограничений.

