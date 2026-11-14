package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
@ConditionalOnProperty(name = "outbox.publisher.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private static final Logger log = LoggerFactory.getLogger(OutboxPublisher.class);

    /** Breaker instance name; thresholds are configured in application.yaml (resilience4j.*). */
    static final String BREAKER_NAME = "outbox-kafka";

    private final CircuitBreaker breaker;
    private final OutboxDispatchRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics metrics;
    private final OutboxRetryPolicy retryPolicy;
    private final MeterRegistry meters;
    private final String owner = UUID.randomUUID().toString();

    @Autowired
    public OutboxPublisher(OutboxDispatchRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper, OutboxMetrics metrics, OutboxRetryPolicy retryPolicy,
                           MeterRegistry meters, CircuitBreakerRegistry registry) {
        this(outboxRepository, kafkaTemplate, objectMapper, metrics, retryPolicy, meters,
                registry.circuitBreaker(BREAKER_NAME));
    }

    OutboxPublisher(OutboxDispatchRepository outboxRepository, KafkaTemplate<String, String> kafkaTemplate,
                    ObjectMapper objectMapper, OutboxMetrics metrics, OutboxRetryPolicy retryPolicy,
                    MeterRegistry meters, CircuitBreaker breaker) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
        this.retryPolicy = retryPolicy;
        this.meters = meters;
        this.breaker = breaker;
        breaker.getEventPublisher()
                .onStateTransition(e -> {
                    meters.counter("outbox_breaker_transitions_total", "from", e.getStateTransition().getFromState().name(), "to", e.getStateTransition().getToState().name()).increment();
                    log.warn("outbox kafka circuit breaker: {} -> {}", e.getStateTransition().getFromState(), e.getStateTransition().getToState());
                });
    }

    @Scheduled(fixedDelayString = "500")
    public void poll() {
        metrics.poll(() -> {
            if (breaker.getState() == CircuitBreaker.State.OPEN) {
                meters.counter("outbox_poll_skipped_breaker_open").increment();
                return;
            }
            List<OutboxRow> batch = outboxRepository.claimBatch(GlobalValues.BATCH_SIZE, owner, GlobalValues.LEASE);
            metrics.batchClaimed(batch.size());
            if (batch.isEmpty()) {
                return;
            }
            int sent = 0;
            for (OutboxRow row : batch) {
                if (!outboxRepository.renewLease(row.id(), row.createdAt(), owner, GlobalValues.LEASE)) {
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
                    long started = System.nanoTime();
                    try {
                        kafkaTemplate.send(GlobalValues.KAFKA_TOPIC, String.valueOf(row.userId()), message)
                                .get(GlobalValues.SEND_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
                    } catch (Exception sendError) {
                        breaker.onError(started, TimeUnit.NANOSECONDS, sendError);
                        throw sendError;
                    }
                    breaker.onSuccess(started, TimeUnit.NANOSECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    metrics.sendFailure(row.eventType(), e);
                    int attempts = outboxRepository.recordFailure(row.id(), row.createdAt(), owner, GlobalValues.MAX_ATTEMPTS, e.getMessage(), retryPolicy);
                    metrics.retry();
                    if (attempts >= GlobalValues.MAX_ATTEMPTS) {
                        metrics.eventRejected(row.eventType());
                        log.error("outbox row {} rejected after {} attempts", row.id(), attempts, e);
                    } else {
                        log.warn("outbox row {} delivery failed, attempt {}: {}", row.id(), attempts, e.getMessage());
                    }
                    break;
                }
                outboxRepository.markAsCompleted(row.id(), row.createdAt(), owner);
                metrics.delivery(Duration.between(row.createdAt(), Instant.now()), row.eventType());
                metrics.eventPublished(row.eventType());
                sent++;
            }
            if (sent > 0) {
                log.info("outbox batch: {} of {} sent", sent, batch.size());
            }
        });
    }

    /** Test wiring: an explicit breaker with the previously hardcoded thresholds. */
    static CircuitBreaker defaultBreaker() {
        return CircuitBreaker.of(BREAKER_NAME, CircuitBreakerConfig.custom()
                .failureRateThreshold(50)
                .slidingWindowSize(10)
                .minimumNumberOfCalls(5)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .permittedNumberOfCallsInHalfOpenState(1)
                .automaticTransitionFromOpenToHalfOpenEnabled(true)
                .build());
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