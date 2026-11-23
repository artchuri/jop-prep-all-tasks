package nip.edu.jobprep.outbox.delivery;

import org.springframework.data.redis.core.RedisCallback;
import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.cache.AbstractCacheIntegrationTest;
import nip.edu.jobprep.cache.TestRedis;
import nip.edu.jobprep.cache.TestKafkaShim;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListConsumerGroupOffsetsOptions;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ContextConfiguration;
import tools.jackson.databind.ObjectMapper;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Tier-2 delivery contract under a Redis outage, exercised through the real
 * listener container (not a direct onMessage call): while Redis is down the
 * valid event is retried with the configured backoff and its offset is NOT
 * committed (invalidation is never lost); a broken payload is acked
 * immediately (poison events do not block the partition, independent of
 * Redis); once Redis returns, the retry succeeds, the invalidation lands and
 * the offset commits.
 */
@Tag("integration")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@ContextConfiguration(initializers = CacheInvalidationConsumerOutageIT.KafkaAndEarliest.class)
class CacheInvalidationConsumerOutageIT extends AbstractCacheIntegrationTest {

    /** Point the real consumer container at the test Kafka and start from the earliest offset. */
    public static class KafkaAndEarliest extends Initializer {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            super.initialize(context);
            TestKafkaShim.KAFKA.start();
            TestPropertyValues.of(
                    "spring.kafka.bootstrap-servers=" + TestKafkaShim.KAFKA.getBootstrapServers(),
                    "spring.kafka.consumer.auto-offset-reset=earliest",
                    // The test-classpath application.yaml shadows the main one, which
                    // drops spring.kafka.listener.ack-mode=manual; without it the
                    // container runs in BATCH mode and never supplies the Acknowledgment,
                    // so every delivery fails before the handler runs.
                    "spring.kafka.listener.ack-mode=manual",
                    "logging.level.org.springframework.kafka=DEBUG"
            ).applyTo(context.getEnvironment());
        }
    }

    private static final String GROUP = "account-cache-invalidation";

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    MeterRegistry meters;

    @Autowired
    CacheManager cacheManager;

    Cache cache;

    KafkaTemplate<String, String> kafka;

    @BeforeEach
    void setUp() {
        cache = cacheManager.getCache(LuaCache.NAME);
        kafka = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                TestKafkaShim.producerProps(TestKafkaShim.KAFKA.getBootstrapServers())));
        kafka.setDefaultTopic(GlobalValues.KAFKA_TOPIC);

        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (1, now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (1, 1, 'ACTIVE', now(), now(), now(), 0, 100.00, 'RUB')
                """);
        redis.execute((RedisCallback<Void>) conn -> {
            conn.serverCommands().flushAll();
            return null;
        });
        TestRedis.REDIS_PROXY.setConnectionCut(false);
    }

    @AfterEach
    void restore() {
        TestRedis.REDIS_PROXY.setConnectionCut(false);
    }

    /**
     * Runs SECOND (after the broken-payload test has fully settled): while
     * Redis is cut only Kafka-side facts are asserted (the offset must not
     * advance); Redis state is asserted only after recovery, when the cache
     * layer is reachable again.
     */
    @Test
    @Order(2)
    void validEventHoldsOffsetWhileRedisIsDownThenDeliversAfterRecovery() throws Exception {
        warmFreshV0();
        long committedBefore = committedOffsetOr(0);
        double invalidationsBefore = meters.counter("cache_invalidation_total", "tier", "kafka").count();

        // Cut BEFORE the send: the consumer cannot succeed on this record.
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        kafka.send(GlobalValues.KAFKA_TOPIC, "1", envelope("ACCOUNT_FROZEN", Map.of("accountId", 1, "version", 1)))
                .get(10, TimeUnit.SECONDS);
        Thread.sleep(3500); // >= 3 backoff cycles of the 1 s FixedBackOff

        // Kafka-side only: the retry holds the offset, nothing commits.
        assertThat(committedOffsetOr(0)).as("offset not committed while Redis is down").isEqualTo(committedBefore);

        // Recovery: the next retry succeeds, invalidation lands, offset commits.
        // Tombstone semantics: tver advances to 1, the displayed ver stays
        // coherent with the stale bal/st (BUG-A fix) - the next GET reloads from PG.
        TestRedis.REDIS_PROXY.setConnectionCut(false);
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(field("tver")).as("invalidation applied").isEqualTo("1"));
        await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                assertThat(committedOffsetOr(0)).as("offset committed after success").isEqualTo(committedBefore + 1));
        assertThat(meters.counter("cache_invalidation_total", "tier", "kafka").count())
                .as("exactly one successful invalidation").isEqualTo(invalidationsBefore + 1);
    }

    /**
     * Runs FIRST on a virgin topic: the broken payload is record 0, so with
     * Redis cut the only way the committed offset can advance is the
     * broken_payload branch acking without touching Redis.
     */
    @Test
    @Order(1)
    void brokenPayloadIsAckedEvenWhileRedisIsDown() throws Exception {
        long committedBefore = committedOffsetOr(0);

        TestRedis.REDIS_PROXY.setConnectionCut(true);
        try {
            kafka.send(GlobalValues.KAFKA_TOPIC, "1", "{not-json").get(10, TimeUnit.SECONDS);
            // The broken_payload branch acks without touching Redis: the offset
            // commits even though the cache layer is unreachable.
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() ->
                    assertThat(committedOffsetOr(0)).as("broken payload acked while Redis down").isEqualTo(committedBefore + 1));
        } finally {
            TestRedis.REDIS_PROXY.setConnectionCut(false);
        }
    }

    private void warmFreshV0() {
        AccountResponseDto dto = cache.get(1L, () ->
                new AccountResponseDto(1L, new BigDecimal("100.00"), "ACTIVE", 0L));
        assertThat(dto.balance()).isEqualByComparingTo("100.00");
        assertThat(field("state")).isEqualTo("F");
        assertThat(field("ver")).isEqualTo("0");
    }

    private String envelope(String type, Map<String, Object> payload) throws Exception {
        return objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", type,
                "userId", 1,
                "occurredAt", Instant.now().toString(),
                "payload", payload));
    }

    private Object field(String name) {
        return redis.opsForHash().get("acc:v1:1", name);
    }

    /** Committed offset of the group on partition 0, or 0 when nothing is committed yet. */
    private long committedOffsetOr(long fallback) throws Exception {
        var tp = new TopicPartition(GlobalValues.KAFKA_TOPIC, 0);
        try (AdminClient admin = AdminClient.create(Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, TestKafkaShim.KAFKA.getBootstrapServers()))) {
            var committed = admin.listConsumerGroupOffsets(GROUP,
                            new ListConsumerGroupOffsetsOptions().timeoutMs(5000))
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            var held = committed.get(tp);
            return held == null ? fallback : held.offset();
        }
    }
}
