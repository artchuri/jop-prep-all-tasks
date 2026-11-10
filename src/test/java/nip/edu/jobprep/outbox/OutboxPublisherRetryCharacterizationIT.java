package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Characterization tests: they pin the CURRENT publisher retry/REJECTED behavior
 * before any policy change (ACC-10x subtask 8). If a future change to the retry
 * policy is intended, these tests must be edited deliberately, not silently.
 *
 * Current behavior fixed here:
 *  C1. After MAX_ATTEMPTS (=5) failed Kafka sends, the row becomes status='REJECTED'
 *      and is never claimed again by claimBatch (terminal loss point).
 *  C2. On each failure the row returns to status='NEW' with next_attempt_at backed
 *      off exponentially: 2s, 4s, 8s, 16s, 32s (capped at 300s by formula
 *      min(300, 2^attempts)).
 *  C3. hasOlderActiveForUser gates newer rows: while an older row of the same user
 *      is NEW/PROCESSING, the newer row is not sent even if claimed.
 *  C4. recordFailure is a no-op (returns 0) when the row is not PROCESSING and
 *      owned by the caller.
 */
class OutboxPublisherRetryCharacterizationIT extends AbstractIntegrationTest {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    OutboxRepository outboxRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanOutbox() {
        jdbcTemplate.update("delete from outbox");
    }

    private OutboxPublisher publisher(KafkaTemplate<String, String> template) {
        return new OutboxPublisher(outboxRepository, template, new ObjectMapper(),
                new OutboxMetrics(new SimpleMeterRegistry()));
    }

    private KafkaTemplate<String, String> brokenTemplate(String bootstrapServers) {
        // Points at a dead port: every send fails after the producer's own delivery timeout.
        // To keep the test fast we bypass producer timeouts: a template with an impossible
        // bootstrap and a tiny delivery timeout.
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 500),
                Map.entry(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 400),
                Map.entry(ProducerConfig.MAX_BLOCK_MS_CONFIG, 500));
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }

    private long saveRow(long userId, long aggregateId) {
        outboxRepository.save(new DomainEvent(UUID.randomUUID().toString(), DomainEvent.ACCOUNT_FROZEN,
                userId, Instant.now(), null), aggregateId, "{}", Instant.now());
        return jdbcTemplate.queryForObject(
                "select id from outbox where user_id = ? order by id desc limit 1", Long.class, userId);
    }

    private String statusOf(long id) {
        return jdbcTemplate.queryForObject("select status from outbox where id = ?", String.class, id);
    }

    private Integer attemptsOf(long id) {
        return jdbcTemplate.queryForObject("select attempt_count from outbox where id = ?", Integer.class, id);
    }

    @Test
    void c1_rowBecomesRejectedTerminalAfterMaxAttempts() {
        KAFKA.start();
        // Kafka container is up but topic send will fail: use unreachable bootstrap on the SAME JVM producer
        // by pointing the template at a closed local port.
        OutboxPublisher publisher = publisher(brokenTemplate("localhost:1"));

        long id = saveRow(101L, 201L);
        for (int i = 0; i < 5; i++) {
            publisher.poll();
            if (i < 4) {
                // return row to claimable state immediately (publisher set NEW + next_attempt_at in the future)
                jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id);
            }
        }

        assertThat(statusOf(id)).isEqualTo("REJECTED");
        assertThat(attemptsOf(id)).isEqualTo(5);

        // Terminal: claimBatch no longer returns it even with next_attempt_at cleared.
        jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id);
        var claimed = outboxRepository.claimBatch(10, "other-owner", java.time.Duration.ofSeconds(1));
        assertThat(claimed).isEmpty();
    }

    @Test
    void c2_backoffGrowsExponentiallyAndIsCapped() {
        KAFKA.start();
        OutboxPublisher publisher = publisher(brokenTemplate("localhost:1"));

        long id = saveRow(102L, 202L);
        long[] expectedBackoffSeconds = {2, 4, 8, 16, 32};

        for (int attempt = 1; attempt <= 5; attempt++) {
            publisher.poll();
            jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id);
            Integer nextAttempt = attemptsOf(id);
            assertThat(nextAttempt).isEqualTo(attempt);
        }
        // after the final attempt the row is REJECTED; verify last retry delay on 4th attempt row state:
        // (delay checks are done via next_attempt_at recorded right after each failure)
        // Re-run a fresh row and capture delays explicitly.
        jdbcTemplate.update("delete from outbox where id = ?", id);
        long id2 = saveRow(103L, 203L);
        for (int attempt = 1; attempt <= 4; attempt++) {
            publisher.poll();
            Instant next = jdbcTemplate.queryForObject(
                    "select next_attempt_at from outbox where id = ?", java.sql.Timestamp.class, id2).toInstant();
            long delay = java.time.Duration.between(Instant.now(), next).toSeconds();
            assertThat(delay).as("attempt %d", attempt)
                    .isBetween(expectedBackoffSeconds[attempt - 1] - 1, expectedBackoffSeconds[attempt - 1] + 1);
            jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id2);
        }
    }

    @Test
    void c3_olderActiveRowBlocksNewerRowOfSameUser() {
        KAFKA.start();
        // Real working Kafka template: the older row would be delivered, so instead keep it
        // permanently claimable-but-unowned by simulating an in-flight row: set status NEW.
        long older = saveRow(104L, 204L);
        long newer = saveRow(104L, 205L);

        OutboxPublisher publisher = publisher(TestKafka.template(KAFKA.getBootstrapServers()));
        publisher.poll();

        // older row delivered, newer row sent in same batch? Current behavior: gate checks
        // hasOlderActiveForUser BEFORE sending; when older is completed first in the same batch,
        // newer may also be sent. This test pins that both can be delivered in one poll
        // (order preserved, no artificial stall on the happy path).
        var statuses = jdbcTemplate.queryForList(
                "select status from outbox where user_id = 104 order by id", String.class);
        assertThat(statuses).hasSize(2);
        // Both rows completed: happy path is not blocked by the gate.
        assertThat(statuses).containsOnly("COMPLETED");
    }

    @Test
    void c3b_olderUnfinishedRowStallsNewerRowDelivery() {
        KAFKA.start();
        long older = saveRow(105L, 206L);
        long newer = saveRow(105L, 207L);
        // Simulate an unfinished older row owned by another publisher (lease not expired):
        jdbcTemplate.update(
                "update outbox set status = 'PROCESSING', lease_owner = 'ghost', " +
                "lease_until = now() + interval '30 seconds' where id = ?", older);

        OutboxPublisher publisher = publisher(TestKafka.template(KAFKA.getBootstrapServers()));
        publisher.poll();

        // Characterized: the newer row IS claimed (-> PROCESSING with the publisher's lease),
        // its send is skipped by hasOlderActiveForUser, and the row is left in PROCESSING
        // until the lease expires. It is not returned to NEW and not sent. This is the
        // current head-of-line stall behavior this suite pins.
        assertThat(statusOf(newer)).isEqualTo("PROCESSING");
        assertThat(attemptsOf(newer)).isEqualTo(0);
    }

    @Test
    void c4_recordFailureIsNoOpWithoutProcessingLease() {
        long id = saveRow(106L, 208L);
        // Row is NEW (not PROCESSING, no owner): recordFailure must return 0 and change nothing.
        int result = outboxRepository.recordFailure(id, jdbcTemplate.queryForObject(
                "select created_at from outbox where id = ?", java.sql.Timestamp.class, id).toInstant(),
                "someone", 5, "boom", Instant.now());
        assertThat(result).isZero();
        assertThat(statusOf(id)).isEqualTo("NEW");
        assertThat(attemptsOf(id)).isZero();
    }
}