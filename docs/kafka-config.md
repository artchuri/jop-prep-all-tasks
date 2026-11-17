# Kafka: полная карта конфигурации

Дата: 2026-11-13. Источники: `application.yaml`, `CacheKafkaConfig.java`,
`AccountCacheInvalidationConsumer.java`, `OutboxPublisher.java`, `TestKafka.java` (тесты).

## 1. application.yaml — spring.kafka (единственный источник свойств)

### Общее
- `bootstrap-servers: localhost:9092` — точка входа в кластер; в IT переопределяется инициализатором Testcontainers.

### Producer (пишет только OutboxPublisher)
- `key-serializer: StringSerializer` — ключ = userId: порядок партиционирования по пользователю.
- `value-serializer: StringSerializer` — значение: JSON-конверт outbox-события.
- `acks: all` — подтверждение всех ISR-реплик: событие не теряется при падении лидера.
- `enable.idempotence: true` — ретраи продюсера не дублируют события (producer id + seq).
- `max.in.flight.requests.per.connection: 2` — пайплайнинг при сохранении порядка (с idempotence допускается до 5).
- `delivery.timeout.ms: 120000` — верхняя граница попыток отправки (включая ретраи): 2 мин.
- `request.timeout.ms: 30000` — таймаут ожидания ack от брокера на запрос.

### Consumer (group account-cache-invalidation, Tier 2 инвалидации кэша)
- `group-id: account-cache-invalidation` — группа инвалидации; свои оффсеты на группу.
- `auto-offset-reset: latest` — без закоммиченного оффсета начать с новых событий; пропуски покрыты TTL кэша (51–69 с).
- `key/value-deserializer: StringDeserializer` — JSON-конверт парсится Jackson'ом в consumer'е.

### Listener
- `ack-mode: manual` — оффсет коммитится только вызовом ack.acknowledge() ПОСЛЕ успешной инвалидации (at-least-once, но не раньше результата).

## 2. Java-конфигурация

### CacheKafkaConfig (outbox/delivery)
- CommonErrorHandler = DefaultErrorHandler(FixedBackOff(1000ms, MAX_VALUE)) — ошибка инвалидации: ретри с паузой 1 с бесконечно, никогда не DLT, оффсет не коммитится, поток блокируется (соответствует README).

### AccountCacheInvalidationConsumer (аннотация слушателя)
- @KafkaListener.topics = "account-events" — топик захардкожен строкой (дублирует GlobalValues.KAFKA_TOPIC).
- @KafkaListener.groupId = "account-cache-invalidation" — дублирует spring.kafka.consumer.group-id; явность важнее ссылки.
- Битый payload: лог + метрика + ack — не блокирует партицию.
- Неизвестный тип события: метрика skipped_type + ack, без ретрая.
- После успешной инвалидации: ack.acknowledge() — ручной коммит оффсета.

### OutboxPublisher (producer-сторона)
- Топик: GlobalValues.KAFKA_TOPIC ("account-events") — единственная константа топика в main-коде.
- Ключ сообщения: row.userId() — порядок по пользователю + gate hasOlderActiveForUser.
- Ожидание: send(...).get(SEND_TIMEOUT=10s) — синхронная отправка с бюджетом из GlobalValues.SEND_TIMEOUT.

### TestKafka (тестовая инфраструктура)
Продюсер с подмножеством продакшн-настроек: bootstrap, String-сериализаторы, acks=all, idempotence=true. Таймауты не заданы — тесты не конкурируют за брокер.

## 3. Зазоры и их статус (2026-11-13)
1. ~~Топик "account-events" захардкожен в @KafkaListener~~ — ЗАКРЫТО: @KafkaListener ссылается на GlobalValues.KAFKA_TOPIC через SpEL; тесты тоже используют константу.
2. groupId в аннотации дублирует yaml — оставлено сознательно (явность у слушателя).
3. concurrency слушателя не задана (по умолчанию 1) — для инвалидации достаточно.
4. enable.auto.commit не переопределён явно: при ack-mode: manual Spring сам выставляет false.

## 4. Circuit breaker доставки
- `resilience4j.circuitbreaker.instances.outbox-kafka` в `application.yaml`: open после 5 последовательных фейлов отправки, 30 с до half-open, 1 пробная отправка.
- `OutboxPublisher.poll()` при OPEN пропускает claim: строки остаются NEW, attempt_count не растёт, premature REJECTED невозможен при простое брокера.
- Гранулярность — отдельная отправка (вручную через breaker.onError/onSuccess), а не батч целиком.

## 5. Итог консолидации
- Java-сторона Kafka сведена в один класс: config/KafkaConfig (CommonErrorHandler с политикой "ретри 1 с, бесконечно, без DLT").
- Старый outbox/delivery/CacheKafkaConfig.java удалён.
- Все имена топиков — только в GlobalValues.KAFKA_TOPIC: main (@KafkaListener через SpEL, OutboxPublisher) и тесты.
- Свойства подключения/сериализации/таймаутов — только в application.yaml (spring.kafka.*).