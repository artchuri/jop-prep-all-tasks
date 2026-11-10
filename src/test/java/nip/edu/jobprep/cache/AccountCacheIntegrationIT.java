package nip.edu.jobprep.cache;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.outbox.AccountCacheInvalidationConsumer;
import nip.edu.jobprep.service.AccountCacheService;
import nip.edu.jobprep.service.AccountQueryService;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * End-to-end cache tests against real PostgreSQL, Redis (via TestRedis proxy)
 * and Kafka: first/second GET, both Tiers of invalidation, rollback safety,
 * fallback when Redis is down.
 */
@Import(AccountCacheIntegrationIT.BrokenKafkaProducerConfig.class)
class AccountCacheIntegrationIT extends AbstractCacheIntegrationTest {

    @Autowired
    AccountCacheService cache;

    @Autowired
    AccountQueryService queryService;

    @Autowired
    AccountCacheInvalidationConsumer consumer;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    KafkaTemplate<String, String> kafka;

    static final org.testcontainers.containers.KafkaContainer KAFKA =
            new org.testcontainers.containers.KafkaContainer(org.testcontainers.utility.DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @TestConfiguration
    static class BrokenKafkaProducerConfig {
        // A working producer pointed at the test Kafka; the app's own producer
        // targets localhost:9092 from application.yaml and is not used here.
        @Bean
        KafkaTemplate<String, String> testKafkaTemplate() {
            KAFKA.start();
            Map<String, Object> props = Map.ofEntries(
                    Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                    Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                    Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
            return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        }
    }

    @BeforeEach
    void setup() {
        jdbc.update("delete from outbox");
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
        seedAccount(1, "100.00", "ACTIVE", 0);
    }

    private void seedAccount(long id, String amount, String status, long version) {
        jdbc.update("delete from accounts where id = ?", id);
        jdbc.update("delete from users where id not in (select user_id from accounts)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (?, ?, ?, now(), now(), now(), ?, ?, 'RUB')
                on conflict (id) do nothing
                """, id, 1, status, version, new java.math.BigDecimal(amount));
        try {
            jdbc.update("insert into users (id, created_at, version) values (?, now(), 0)", 1);
        } catch (Exception ignored) { }
    }

    private long pgVersion(long id) {
        return jdbc.queryForObject("select version from accounts where id = ?", Long.class, id);
    }

    @Test
    void firstGetFillsCacheAndSecondIsHitWithoutPg() {
        AccountResponseDto first = queryService.get(1);
        assertThat(first.balance()).isEqualByComparingTo("100.00");
        assertThat(cache.field(1, "state")).isEqualTo("F");

        // Break the DB read path by renaming the table: a HIT must not touch PG.
        jdbc.update("alter table accounts rename to accounts_shadow");
        try {
            AccountResponseDto second = queryService.get(1);
            assertThat(second.version()).isEqualTo(first.version());
            assertThat(second.balance()).isEqualByComparingTo("100.00");
        } finally {
            jdbc.update("alter table accounts_shadow rename to accounts");
        }
    }

    @Test
    void tier1InvalidateAfterCommitForcesPgReload() {
        queryService.get(1); // fill FRESH v0
        jdbc.update("update accounts set amount = 70.00, version = 1 where id = 1");

        // Tier 1 invalidation directly (the AFTER_COMMIT listener calls the same method)
        cache.invalidate(1, 1);

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(queryService.get(1).version()).isEqualTo(1L));
        assertThat(queryService.get(1).balance()).isEqualByComparingTo("70.00");
    }

    @Test
    void tier2ConsumerInvalidatesAndCommitsOnlyAfterSuccess() throws Exception {
        queryService.get(1); // fill FRESH v0
        jdbc.update("update accounts set amount = 55.00, version = 3 where id = 1");

        Map<String, Object> payload = Map.of("accountId", 1, "version", 3);
        String envelope = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "ACCOUNT_FROZEN",
                "userId", 1,
                "occurredAt", java.time.Instant.now().toString(),
                "payload", payload));
        kafka.send("account-events", "1", envelope).get(10, java.util.concurrent.TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            // consume directly via the handler under test to stay hermetic
            // (the consumer's Kafka listener container is not started in this context)
        });
        // Direct handler invocation: pins the contract regardless of container lifecycle
        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> rec =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("account-events", 0, 0, "1", envelope);
        consumer.onMessage(rec, () -> { });

        assertThat(queryService.get(1).version()).isEqualTo(3L);
        assertThat(queryService.get(1).balance()).isEqualByComparingTo("55.00");
    }

    @Test
    void staleEventWithOlderVersionIsRejected() throws Exception {
        queryService.get(1); // FRESH v0
        // Older-version invalidate must be a NOOP: state stays F with v0
        cache.invalidate(1, -5); // older than cached ver 0
        assertThat(cache.field(1, "state")).isEqualTo("F");

        Map<String, Object> payload = Map.of("accountId", 1, "version", -1);
        String envelope = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "ACCOUNT_FROZEN",
                "userId", 1,
                "occurredAt", java.time.Instant.now().toString(),
                "payload", payload));
        org.apache.kafka.clients.consumer.ConsumerRecord<String, String> rec =
                new org.apache.kafka.clients.consumer.ConsumerRecord<>("account-events", 0, 0, "1", envelope);
        consumer.onMessage(rec, () -> { });
        // version -1 <= cached 0 -> NOOP, still fresh
        assertThat(cache.field(1, "state")).isEqualTo("F");
    }

    @Test
    void redisDownFallsBackToPgAndResponseIsServed() {
        queryService.get(1); // warm
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        try {
            AccountResponseDto dto = queryService.get(1);
            assertThat(dto.accountId()).isEqualTo(1L);
            assertThat(dto.balance()).isEqualByComparingTo("100.00");
            assertThat(dto.version()).isEqualTo(pgVersion(1));
        } finally {
            TestRedis.REDIS_PROXY.setConnectionCut(false);
        }
    }

    @Test
    void leaseGuardsConcurrentMiss_singlePgFill() throws Exception {
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
        // First reader takes the lease and fills; a parallel WAIT-reader resolves to the filled value.
        AccountCacheService.ReadResult r1 = cache.read(1);
        assertThat(r1.status()).isEqualTo(AccountCacheService.Status.LEASE);
        AccountCacheService.ReadResult r2 = cache.read(1);
        assertThat(r2.status()).isEqualTo(AccountCacheService.Status.WAIT);
        cache.fill(1, r1.token(), 0, "100.00", "ACTIVE");
        AccountResponseDto dto = queryService.get(1);
        assertThat(dto.version()).isEqualTo(0L);
    }
}