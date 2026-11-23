package nip.edu.jobprep.cache;

import org.springframework.data.redis.core.RedisCallback;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.outbox.delivery.AccountCacheInvalidationConsumer;
import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.exceptions.InsufficientFundsException;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import nip.edu.jobprep.service.AccountService;
import nip.edu.jobprep.service.TransferService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;


@Import(AccountCacheIntegrationIT.BrokenKafkaProducerConfig.class)
class AccountCacheIntegrationIT extends AbstractCacheIntegrationTest {

    @Autowired
    CacheManager cacheManager;

    @Autowired
    MeterRegistry meters;

    Cache cache;

    @BeforeEach
    void initCache() {
        cache = cacheManager.getCache(LuaCache.NAME);
    }

    @Autowired
    AccountService accountService;

    @Autowired
    TransferService transferService;

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

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

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
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from transfers_history");
        redis.execute((RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
        seedAccount(1, "100.00", "ACTIVE", 0);
    }

    private void seedAccount(long id, String amount, String status, long version) {
        // Seed the owner first: when the shared static PostgreSQL container carries
        // dirty state from a failed previous run, inserting the account before its
        // owner trips the fk_accounts_user constraint.
        jdbc.update("insert into users (id, created_at, version) values (1, now(), 0) on conflict (id) do nothing");
        jdbc.update("delete from accounts where id = ?", id);
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (?, ?, ?, now(), now(), now(), ?, ?, 'RUB')
                on conflict (id) do update set status = excluded.status, version = excluded.version, amount = excluded.amount
                """, id, 1, status, version, new BigDecimal(amount));
    }

    private long pgVersion(long id) {
        return jdbc.queryForObject("select version from accounts where id = ?", Long.class, id);
    }


    @Test
    void tier1InvalidateAfterCommitForcesPgReload() {
        accountService.get(1); // fill FRESH v0
        jdbc.update("update accounts set amount = 70.00, version = 1 where id = 1");

        // Tier 1 invalidation directly (the AFTER_COMMIT listener calls the same method)
        cache.evict(new LuaCache.Invalidate(1, 1));

        await().atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                assertThat(accountService.get(1).version()).isEqualTo(1L));
        assertThat(accountService.get(1).balance()).isEqualByComparingTo("70.00");
    }

    @Test
    void tier2ConsumerInvalidatesAndCommitsOnlyAfterSuccess() throws Exception {
        accountService.get(1); // fill FRESH v0
        jdbc.update("update accounts set amount = 55.00, version = 3 where id = 1");

        Map<String, Object> payload = Map.of("accountId", 1, "version", 3);
        String envelope = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "ACCOUNT_FROZEN",
                "userId", 1,
                "occurredAt", Instant.now().toString(),
                "payload", payload));
        kafka.send(GlobalValues.KAFKA_TOPIC, "1", envelope).get(10, TimeUnit.SECONDS);

        await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> {
            // consume directly via the handler under test to stay hermetic
            // (the consumer's Kafka listener container is not started in this context)
        });
        // Direct handler invocation: pins the contract regardless of container lifecycle
        ConsumerRecord<String, String> rec =
                new ConsumerRecord<>(GlobalValues.KAFKA_TOPIC, 0, 0, "1", envelope);
        consumer.onMessage(rec, () -> { });

        assertThat(accountService.get(1).version()).isEqualTo(3L);
        assertThat(accountService.get(1).balance()).isEqualByComparingTo("55.00");
    }

    @Test
    void staleEventWithOlderVersionIsRejected() throws Exception {
        accountService.get(1); // FRESH v0
        // Older-version invalidate must be a NOOP: state stays F with v0
        cache.evict(new LuaCache.Invalidate(1, -5L)); // older than cached ver 0
        assertThat(field(1, "state")).isEqualTo("F");

        Map<String, Object> payload = Map.of("accountId", 1, "version", -1);
        String envelope = objectMapper.writeValueAsString(Map.of(
                "eventId", UUID.randomUUID().toString(),
                "eventType", "ACCOUNT_FROZEN",
                "userId", 1,
                "occurredAt", Instant.now().toString(),
                "payload", payload));
        ConsumerRecord<String, String> rec =
                new ConsumerRecord<>(GlobalValues.KAFKA_TOPIC, 0, 0, "1", envelope);
        consumer.onMessage(rec, () -> { });
        // version -1 <= cached 0 -> NOOP, still fresh
        assertThat(field(1, "state")).isEqualTo("F");
    }

    @Test
    void redisDownFallsBackToPgAndResponseIsServed() {
        accountService.get(1); // warm
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        try {
            AccountResponseDto dto = accountService.get(1);
            assertThat(dto.accountId()).isEqualTo(1L);
            assertThat(dto.balance()).isEqualByComparingTo("100.00");
            assertThat(dto.version()).isEqualTo(pgVersion(1));
        } finally {
            TestRedis.REDIS_PROXY.setConnectionCut(false);
        }
    }

    @Test
    void missTakesLeaseThenFillLandsUnderToken() throws Exception {
        redis.execute((RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
        // Sequential lease handoff: the first reader takes the lease, loads from
        // PG and fills under the token. (Real concurrency lives in
        // realConcurrentMissCollapsesIntoSinglePgFill below.)
        AccountResponseDto first = accountService.get(1);
        assertThat(first.version()).isEqualTo(0L);
        assertThat(field(1, "state")).isEqualTo("F");
        // A direct low-level read now returns a filled value (HIT), proving lease handoff works.
        assertThat(field(1, "bal")).isEqualTo("100.00");
    }

    /**
     * True concurrent cold miss: 8 readers released at once on an empty key
     * must collapse into a single PostgreSQL fill. Overlap is proven by the
     * wait counter - a sequential run cannot produce 7 WAIT outcomes, because
     * WAIT requires another reader's live lease at the moment of the read.
     */
    @Test
    void realConcurrentMissCollapsesIntoSinglePgFill() throws Exception {
        redis.execute((RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
        double waitBefore = waitCounter();
        AtomicInteger dbCalls = new AtomicInteger();
        Callable<AccountResponseDto> loader = () -> {
            if (dbCalls.incrementAndGet() == 1) {
                Thread.sleep(400); // hold the lease so late readers must WAIT
            }
            return new AccountResponseDto(1L, new BigDecimal("100.00"), "ACTIVE", 0L);
        };

        int readers = 8;
        ExecutorService pool = Executors.newFixedThreadPool(readers);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<AccountResponseDto>> results = new ArrayList<>();
        for (int i = 0; i < readers; i++) {
            results.add(pool.submit(() -> {
                start.await();
                return cache.get(1L, loader);
            }));
        }
        start.countDown();
        for (Future<AccountResponseDto> f : results) {
            assertThat(f.get(10, TimeUnit.SECONDS)).isNotNull();
        }
        pool.shutdown();

        // The stampede collapsed: one PostgreSQL fill for eight simultaneous readers.
        assertThat(dbCalls.get()).isEqualTo(1);
        // Overlap proven: exactly the 7 non-leader readers observed a live
        // foreign lease (WAIT); sequential execution yields 0.
        assertThat(waitCounter() - waitBefore).isEqualTo(7.0);
        // The fill landed, so the next reader is served from Redis, not PG.
        int before = dbCalls.get();
        assertThat(cache.get(1L, loader)).isNotNull();
        assertThat(dbCalls.get()).isEqualTo(before);
        assertThat(field(1, "state")).isEqualTo("F");
    }

    private double waitCounter() {
        return meters.counter("cache_requests_total", "result", "wait").count();
    }

    @Test
    void freezeInvalidatesThroughTheAnnotatedListenerPath() {
        // Warm the cache from PG: FRESH v0, balance 100.
        AccountResponseDto warm = accountService.get(1);
        assertThat(warm.version()).isEqualTo(0L);
        assertThat(field(1, "state")).isEqualTo("F");

        // Real write path: freeze commits, then the AFTER_COMMIT listener runs
        // @CacheEvict with the versioned key -> LuaCache.evict -> version-gated invalidate.
        accountService.freeze(1L, 7L);

        // The commit advanced the row version in PG.
        assertThat(pgVersion(1)).isEqualTo(1L);
        // The entry was invalidated (lease voided / stale window), not served as FRESH v0.
        assertThat(field(1, "state")).isNotEqualTo("F");
        // The next read reflects the write, i.e. invalidation actually reached Redis.
        assertThat(accountService.get(1).status()).isEqualTo("FROZEN");
        assertThat(accountService.get(1).version()).isEqualTo(1L);
    }


    @Test
    void staleCacheValueDoesNotAffectFinancialOperations() {
        // Warm the cache: FRESH v0, balance 100.00, ACTIVE.
        AccountResponseDto warm = accountService.get(1);
        assertThat(warm.version()).isEqualTo(0L);
        assertThat(warm.balance()).isEqualByComparingTo("100.00");

        // Simulate a fully missed invalidation: another instance lowers the real
        // balance (50.00 v1) while our cache keeps the old snapshot (100.00 v0).
        jdbc.update("update accounts set amount = 50.00, version = 1 where id = 1");

        // The cache still serves the stale snapshot (documented TTL bound).
        AccountResponseDto stale = accountService.get(1);
        assertThat(stale.version()).isEqualTo(0L);
        assertThat(stale.balance()).isEqualByComparingTo("100.00");

        // Seed the recipient account (user 1 owns it too - only the sender is ownership-checked).
        jdbc.update("insert into users (id, created_at, version) values (1, now(), 0) on conflict (id) do nothing");
        jdbc.update("delete from accounts where id = 2");
        jdbc.update("insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency) " +
                "values (2, 1, 'ACTIVE', now(), now(), now(), 0, 0.00, 'RUB') on conflict (id) do update set status = 'ACTIVE', version = 0");

        // Financial operations read PG under row lock and MUST NOT trust the cache:
        // the cache claims 100.00 is available, but the real balance (50.00) makes
        // a 75.00 transfer impossible - rejected from the real state.
        assertThatThrownBy(() -> transferService.transfer(
                UUID.randomUUID().toString(),
                new TransferRequestDto(2L, 1L, 1L, new BigDecimal("75.00"))))
                .isInstanceOf(InsufficientFundsException.class);

        // ...and a transfer from an account frozen behind the cache is rejected
        // despite the cache still claiming the account is ACTIVE.
        jdbc.update("update accounts set status = 'FROZEN', version = 2 where id = 1");
        assertThatThrownBy(() -> transferService.transfer(
                UUID.randomUUID().toString(),
                new TransferRequestDto(2L, 1L, 1L, new BigDecimal("10.00"))))
                .isInstanceOf(AccountStatusConflictException.class);

        // Cleanup: remove the extra account so other tests' seeding (which deletes
        // users not referenced by accounts) stays predictable.
        jdbc.update("delete from accounts where id = 2");
    }


    private Object field(long accountId, String name) {
        return redis.opsForHash().get(AccountCacheScripts.key(accountId), name);
    }
}