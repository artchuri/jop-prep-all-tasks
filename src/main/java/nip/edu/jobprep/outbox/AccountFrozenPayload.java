package nip.edu.jobprep.outbox;

import java.time.Instant;

public record AccountFrozenPayload(
        long accountId,
        String previousStatus,
        String newStatus,
        long changedByUserId,
        Instant occurredAt
) {
}
