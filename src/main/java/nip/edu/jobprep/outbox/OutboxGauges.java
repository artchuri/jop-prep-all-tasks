package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;


@Component
public class OutboxGauges {

    public OutboxGauges(OutboxRepository outboxRepository, MeterRegistry meterRegistry) {
        for (String status : new String[]{"NEW", "PROCESSING", "REJECTED"}) {
            Gauge.builder("outbox_pending", () -> outboxRepository.countByStatus(status))
                    .tag("status", status)
                    .description("Outbox rows currently in status")
                    .register(meterRegistry);
        }

        Gauge.builder("outbox_oldest_new_age_seconds",
                        () -> outboxRepository.findOldestNewCreatedAt()
                                .map(ts -> (double) Duration.between(ts, Instant.now()).getSeconds())
                                .orElse(0.0))
                .description("Age of the oldest NEW outbox row")
                .register(meterRegistry);

        Gauge.builder("outbox_deferred_retries", outboxRepository::countScheduledRetries)
                .description("Outbox rows waiting for a scheduled retry (next_attempt_at in the future)")
                .register(meterRegistry);
    }
}
