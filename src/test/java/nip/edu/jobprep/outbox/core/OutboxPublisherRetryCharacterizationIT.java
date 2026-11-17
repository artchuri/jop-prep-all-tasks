package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.outbox.event.DomainEvent;

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

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;


class OutboxPublisherRetryCharacterizationIT extends AbstractIntegrationTest {

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Autowired
    OutboxDispatchRepository outboxRepository;

    @Autowired
    OutboxEventRepository outboxEventRepository;

    @Autowired
    JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanOutbox() {
        jdbcTemplate.update("delete from outbox");
    }

    private OutboxPublisher publisher(KafkaTemplate<String, String> template) {
        return new OutboxPublisher(outboxRepository, template, new ObjectMapper(),
                new OutboxMetrics(new SimpleMeterRegistry()), new OutboxRetryPolicy(), new SimpleMeterRegistry(),
                io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry.ofDefaults());
    }

    private KafkaTemplate<String, String> brokenTemplate(String bootstrapServers) {
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
        outboxEventRepository.save(new DomainEvent(UUID.randomUUID().toString(), DomainEvent.ACCOUNT_FROZEN,
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
        OutboxPublisher publisher = publisher(brokenTemplate("localhost:1"));

        long id = saveRow(101L, 201L);
        for (int i = 0; i < 5; i++) {
            publisher.poll();
            if (i < 4) {
                jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id);
            }
        }

        assertThat(statusOf(id)).isEqualTo("REJECTED");
        assertThat(attemptsOf(id)).isEqualTo(5);

        jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id);
        var claimed = outboxRepository.claimBatch(10, "other-owner", Duration.ofSeconds(1));
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

        jdbcTemplate.update("delete from outbox where id = ?", id);
        long id2 = saveRow(103L, 203L);

        OutboxPublisher freshPublisher = publisher(brokenTemplate("localhost:1"));
        for (int attempt = 1; attempt <= 4; attempt++) {
            freshPublisher.poll();
            Instant next = jdbcTemplate.queryForObject(
                    "select next_attempt_at from outbox where id = ?", Timestamp.class, id2).toInstant();
            long delay = Duration.between(Instant.now(), next).toSeconds();
            assertThat(delay).as("attempt %d", attempt)
                    .isBetween(expectedBackoffSeconds[attempt - 1] - 1, expectedBackoffSeconds[attempt - 1] + 1);
            jdbcTemplate.update("update outbox set next_attempt_at = null where id = ?", id2);
        }
    }

    @Test
    void c3_olderActiveRowBlocksNewerRowOfSameUser() {
        KAFKA.start();

        long older = saveRow(104L, 204L);
        long newer = saveRow(104L, 205L);

        OutboxPublisher publisher = publisher(TestKafka.template(KAFKA.getBootstrapServers()));
        publisher.poll();


        var statuses = jdbcTemplate.queryForList(
                "select status from outbox where user_id = 104 order by id", String.class);
        assertThat(statuses).hasSize(2);
        assertThat(statuses).containsOnly("COMPLETED");
    }

    @Test
    void c3b_olderUnfinishedRowStallsNewerRowDelivery() {
        KAFKA.start();
        long older = saveRow(105L, 206L);
        long newer = saveRow(105L, 207L);
        jdbcTemplate.update(
                "update outbox set status = 'PROCESSING', lease_owner = 'ghost', " +
                "lease_until = now() + interval '30 seconds' where id = ?", older);

        OutboxPublisher publisher = publisher(TestKafka.template(KAFKA.getBootstrapServers()));
        publisher.poll();

        assertThat(statusOf(newer)).isEqualTo("PROCESSING");
        assertThat(attemptsOf(newer)).isEqualTo(0);
    }

    @Test
    void c4_recordFailureIsNoOpWithoutProcessingLease() {
        long id = saveRow(106L, 208L);
        int result = outboxRepository.recordFailure(id, jdbcTemplate.queryForObject(
                "select created_at from outbox where id = ?", Timestamp.class, id).toInstant(),
                "someone", 5, "boom", new OutboxRetryPolicy());
        assertThat(result).isZero();
        assertThat(statusOf(id)).isEqualTo("NEW");
        assertThat(attemptsOf(id)).isZero();
    }
}