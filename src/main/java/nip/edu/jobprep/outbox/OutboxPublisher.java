package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    static final String TOPIC = "account-events";
    private static final int BATCH_SIZE = 100;
    private static final int MAX_ATTEMPTS = 5;
    private static final Duration LEASE = Duration.ofSeconds(30);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics metrics;
    private final String owner = UUID.randomUUID().toString();

    public OutboxPublisher(OutboxRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper, OutboxMetrics metrics) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "500")
    public void poll() {
        metrics.poll(() -> {
            List<OutboxRow> batch = outboxRepository.claimBatch(BATCH_SIZE, owner, LEASE);
            metrics.batchClaimed(batch.size());
            if (batch.isEmpty()) {
                return;
            }
            int sent = 0;
            for (OutboxRow row : batch) {
                if (!outboxRepository.renewLease(row.id(), row.createdAt(), owner, LEASE)) {
                    log.debug("outbox row {} lease was lost, skipping", row.id());
                    metrics.leaseLost();
                    continue;
                }
                if (outboxRepository.hasOlderActiveForUser(row.userId(), row.id())) {
                    continue;
                }
                try {
                    String message = buildMessage(row);
                    //NOTE в какой конкретно топик, в партицию по айди пользователя(чтобы у нас были все сообщения по порядку), событие
                    kafkaTemplate.send(TOPIC, String.valueOf(row.userId()), message).get(SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    metrics.sendFailure(row.eventType(), e);
                    int attempts = outboxRepository.recordFailure(row.id(), row.createdAt(), owner, MAX_ATTEMPTS, e.getMessage(), java.time.Instant.now());
                    metrics.retry();
                    if (attempts >= MAX_ATTEMPTS) {
                        metrics.eventRejected(row.eventType());
                        log.error("outbox row {} rejected after {} attempts", row.id(), attempts, e);
                    } else {
                        log.warn("outbox row {} delivery failed, attempt {}: {}", row.id(), attempts, e.getMessage());
                    }
                    break;
                }
                outboxRepository.markAsCompleted(row.id(), row.createdAt(), owner);
                metrics.delivery(Duration.between(row.createdAt(), java.time.Instant.now()), row.eventType());
                metrics.eventPublished(row.eventType());
                sent++;
            }
            if (sent > 0) {
                log.info("outbox batch: {} of {} sent", sent, batch.size());
            }
        });
    }

    private String buildMessage(OutboxRow row) {
        ObjectNode envelope = objectMapper.createObjectNode();
        envelope.put("eventId", row.eventId());
        envelope.put("eventType", row.eventType());
        envelope.put("userId", row.userId());
        envelope.put("aggregateId", row.aggregateId());
        envelope.put("occurredAt", row.createdAt().toString());
        envelope.set("payload", objectMapper.readTree(row.payloadJson()));
        return objectMapper.writeValueAsString(envelope);
    }
}
