package nip.edu.jobprep.exceptions;
import lombok.extern.slf4j.Slf4j;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.CacheErrorHandler;

import java.util.List;


@Slf4j
public class SuppressingCacheErrorHandler implements CacheErrorHandler {

    /** Operations we pre-register so the dashboard renders zeros, not "no data". */
    private static final List<String> OPERATIONS = List.of("get", "put", "evict", "clear");

    private final MeterRegistry meters;

    public SuppressingCacheErrorHandler(MeterRegistry meters) {
        this.meters = meters;
        // Eager registration keeps the "Invalidation errors" panel at zero
        // instead of "no data" until the first Redis hiccup.
        for (String operation : OPERATIONS) {
            meters.counter("cache_errors_total", "operation", operation);
        }
    }

    @Override
    public void handleCacheGetError(RuntimeException e, Cache cache, Object key) {
        count("get");
        log.warn("cache get failed for {}: {}", key, e.getMessage());
    }

    @Override
    public void handleCachePutError(RuntimeException e, Cache cache, Object key, Object value) {
        count("put");
        log.warn("cache put failed for {}: {}", key, e.getMessage());
    }

    @Override
    public void handleCacheEvictError(RuntimeException e, Cache cache, Object key) {
        count("evict");
        log.warn("cache evict failed for {}: {}", key, e.getMessage());
    }

    @Override
    public void handleCacheClearError(RuntimeException e, Cache cache) {
        count("clear");
        log.warn("cache clear failed: {}", e.getMessage());
    }

    private void count(String operation) {
        meters.counter("cache_errors_total", "operation", operation).increment();
    }
}