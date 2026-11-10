package nip.edu.jobprep.outbox;

import java.math.BigDecimal;
import java.time.Instant;

public record TransferCompletedPayload(
        long transferId,
        String idempotencyKey,
        long fromAccountId,
        long recipientAccountId,
        BigDecimal amount,
        String currency,
        long fromAccountVersion,
        long toAccountVersion,
        Instant occurredAt
) {
}