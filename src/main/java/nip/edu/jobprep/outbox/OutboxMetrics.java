package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicReference;

@Component
public class OutboxMetrics {

    private static final String UNKNOWN = "unknown";

    private final MeterRegistry registry;
    private final AtomicReference<Number> batchSize = new AtomicReference<>(0);

    public OutboxMetrics(MeterRegistry registry) {
        this.registry = registry;
        Gauge.builder("outbox_publisher_batch_size", batchSize, ref -> ref.get().doubleValue())
                .description("Number of rows claimed in the last poll batch")
                .register(registry);


        Counter.builder("outbox_send_failures")
                .description("Outbox Kafka send failures")
                .tag("event_type", UNKNOWN)
                .tag("error", UNKNOWN)
                .register(registry);
        Counter.builder("outbox_events_rejected")
                .description("Outbox events rejected after max attempts")
                .tag("event_type", UNKNOWN)
                .register(registry);
        Counter.builder("outbox_retries")
                .description("Outbox delivery retries")
                .register(registry);
        Counter.builder("outbox_lease_lost")
                .description("Lease renewals lost to another owner")
                .register(registry);

        Timer.builder("outbox_delivery_seconds")
                .description("Outbox event delivery duration")
                .tag("event_type", UNKNOWN)
                .publishPercentileHistogram()
                .register(registry);
    }



    public void eventCreated(String eventType) {
        Counter.builder("outbox_events_new")
                .tag("event_type", tag(eventType))
                .register(registry)
                .increment();
    }

    public void eventPublished(String eventType) {
        Counter.builder("outbox_events_published")
                .tag("event_type", tag(eventType))
                .register(registry)
                .increment();
    }

    public void delivery(Duration createdToPublished, String eventType) {
        Timer.builder("outbox_delivery_seconds")
                .tag("event_type", tag(eventType))
                .publishPercentileHistogram()
                .register(registry)
                .record(createdToPublished);
    }



    public void sendFailure(String eventType, Throwable error) {
        Counter.builder("outbox_send_failures")
                .tag("event_type", tag(eventType))
                .tag("error", errorTag(error))
                .register(registry)
                .increment();
    }

    public void eventRejected(String eventType) {
        Counter.builder("outbox_events_rejected")
                .tag("event_type", tag(eventType))
                .register(registry)
                .increment();
    }

    public void retry() {
        Counter.builder("outbox_retries")
                .description("Outbox delivery retries")
                .register(registry)
                .increment();
    }

    public void leaseLost() {
        Counter.builder("outbox_lease_lost")
                .description("Lease renewals lost to another owner")
                .register(registry)
                .increment();
    }

    public void batchClaimed(int size) {
        batchSize.set(size);
    }

    public void poll(Runnable pollBody) {
        Timer.builder("outbox_publisher_poll_seconds")
                .description("Outbox publisher poll duration")
                .register(registry)
                .record(pollBody);
    }

    private static String tag(String eventType) {
        return eventType == null || eventType.isBlank() ? UNKNOWN : eventType;
    }

    private static String errorTag(Throwable error) {
        if (error == null) {
            return UNKNOWN;
        }
        String name = error.getClass().getSimpleName();
        return name.isBlank() ? UNKNOWN : name;
    }
}
