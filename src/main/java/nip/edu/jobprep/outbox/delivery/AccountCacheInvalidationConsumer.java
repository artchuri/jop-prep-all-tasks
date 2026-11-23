package nip.edu.jobprep.outbox.delivery;
import lombok.extern.slf4j.Slf4j;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.outbox.event.DomainEvent;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;


@Component
@Slf4j
public class AccountCacheInvalidationConsumer {
    private static final TypeReference<Map<String, Object>> ENVELOPE = new TypeReference<>() {};

    private final CacheManager cacheManager;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meters;

    public AccountCacheInvalidationConsumer(CacheManager cacheManager, ObjectMapper objectMapper, MeterRegistry meters) {
        this.cacheManager = cacheManager;
        this.objectMapper = objectMapper;
        this.meters = meters;
        // Eager registration: the Kafka-tier panels must render zeros before the
        // first invalidation event ever arrives.
        meters.counter("cache_invalidation_total", "tier", "kafka");
        for (String reason : List.of("broken_payload", "malformed_payload", "no_cache")) {
            meters.counter("cache_invalidation_errors_total", "tier", "kafka", "reason", reason);
        }
    }

    @KafkaListener(topics = "#{T(nip.edu.jobprep.model.GlobalValues).KAFKA_TOPIC}", groupId = "account-cache-invalidation")
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


        long[][] targets;
        try {
            Map<String, Object> payload = (Map<String, Object>) envelope.get("payload");
            if (payload == null) {
                throw new IllegalArgumentException("payload is missing");
            }
            if (DomainEvent.ACCOUNT_FROZEN.equals(type)) {
                targets = new long[][]{{num(payload.get("accountId")), num(payload.get("version"))}};
            } else if (DomainEvent.TRANSFER_COMPLETED.equals(type)) {
                targets = new long[][]{
                        {num(payload.get("fromAccountId")), num(payload.get("fromAccountVersion"))},
                        {num(payload.get("recipientAccountId")), num(payload.get("toAccountVersion"))}};
            } else {
                meters.counter("cache_invalidation_total", "tier", "kafka", "result", "skipped_type").increment();
                ack.acknowledge();
                return;
            }
        } catch (RuntimeException malformed) {
            meters.counter("cache_invalidation_errors_total", "tier", "kafka", "reason", "malformed_payload").increment();
            log.error("malformed cache invalidation payload at {}-{}: {}", record.partition(), record.offset(), record.value());
            ack.acknowledge();
            return;
        }
        for (long[] target : targets) {
            invalidate(target[0], target[1]);
        }
        meters.counter("cache_invalidation_total", "tier", "kafka").increment();
        ack.acknowledge();
    }

    private void invalidate(long accountId, long version) {
        Cache cache = cacheManager.getCache(LuaCache.NAME);
        if (cache == null) {
            meters.counter("cache_invalidation_errors_total", "tier", "kafka", "reason", "no_cache").increment();
            return;
        }
        cache.evict(new LuaCache.Invalidate(accountId, version));
    }

    private static long num(Object value) {
        if (value instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(value));
    }
}