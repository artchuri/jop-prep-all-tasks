package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.service.AccountCacheService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.Map;


@Component
public class AccountCacheInvalidationConsumer {

    private static final Logger log = LoggerFactory.getLogger(AccountCacheInvalidationConsumer.class);
    private static final TypeReference<Map<String, Object>> ENVELOPE = new TypeReference<>() {};

    private final AccountCacheService cache;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meters;

    public AccountCacheInvalidationConsumer(AccountCacheService cache, ObjectMapper objectMapper, MeterRegistry meters) {
        this.cache = cache;
        this.objectMapper = objectMapper;
        this.meters = meters;
    }

    @KafkaListener(topics = "account-events", groupId = "account-cache-invalidation")
    public void onMessage(ConsumerRecord<String, String> record, Acknowledgment ack) {
        Map<String, Object> envelope;
        try {
            envelope = objectMapper.readValue(record.value(), ENVELOPE);
        } catch (JacksonException brokenPayload) {
            meters.counter("cache_invalidation_errors_total", "tier", "kafka", "reason", "broken_payload").increment();
            log.error("unparseable outbox event at {}-{}: {}", record.partition(), record.offset(), record.value());
            ack.acknowledge();
            return;
        }
        String type = String.valueOf(envelope.get("eventType"));

        Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
        if (DomainEvent.ACCOUNT_FROZEN.equals(type)) {
            cache.invalidate(num(payload.get("accountId")), num(payload.get("version")));
        } else if (DomainEvent.TRANSFER_COMPLETED.equals(type)) {
            cache.invalidate(num(payload.get("fromAccountId")), num(payload.get("fromAccountVersion")));
            cache.invalidate(num(payload.get("recipientAccountId")), num(payload.get("toAccountVersion")));
        } else {
            meters.counter("cache_invalidation_total", "tier", "kafka", "result", "skipped_type").increment();
            ack.acknowledge();
            return;
        }
        meters.counter("cache_invalidation_total", "tier", "kafka").increment();
        ack.acknowledge();
    }

    private static long num(Object value) {
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(value));
    }
}