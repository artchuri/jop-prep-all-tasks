package nip.edu.jobprep.outbox.core;

import java.time.Instant;

public record OutboxRow(
        long id,
        String eventId,
        String eventType,
        long aggregateId,
        long userId,
        String payloadJson,
        Instant createdAt
) {
}
