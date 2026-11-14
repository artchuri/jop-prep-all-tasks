package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class OutboxRetryPolicy {

    public RetryDecision nextFailure(int currentAttempts, int maxAttempts, Instant now) {
        int attempts = currentAttempts + 1;
        boolean rejected = attempts >= maxAttempts;

        long backoffSeconds = Math.min(GlobalValues.MAX_BACKOFF.toSeconds(), 1L << Math.min(attempts, 8));

        return new RetryDecision(
                attempts,
                rejected,
                rejected ? null : now.plusSeconds(backoffSeconds),
                rejected ? now : null
        );
    }

    public record RetryDecision(
            int attempts,
            boolean rejected,
            Instant nextAttemptAt,
            Instant rejectedAt
    ) {
    }
}
