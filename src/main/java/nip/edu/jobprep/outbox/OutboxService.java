package nip.edu.jobprep.outbox;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.UUID;


@Service
public class OutboxService {
    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final OutboxMetrics metrics;

    public OutboxService(OutboxRepository outboxRepository, ObjectMapper objectMapper, OutboxMetrics metrics) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.metrics = metrics;
    }


    public void publish(String eventType, long userId, long aggregateId, Object payload, Instant occurredAt) {
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JacksonException e) {
            throw new IllegalStateException("failed to serialize outbox payload for event " + eventType, e);
        }

        DomainEvent envelope = new DomainEvent(UUID.randomUUID().toString(), eventType, userId, occurredAt, null);
        outboxRepository.save(envelope, aggregateId, payloadJson, envelope.occurredAt());
        metrics.eventCreated(eventType);
    }
}
