package nip.edu.jobprep.exceptions;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.interceptor.CacheErrorHandler;


public class SuppressingCacheErrorHandler implements CacheErrorHandler {

    private static final Logger log = LoggerFactory.getLogger(SuppressingCacheErrorHandler.class);

    private final MeterRegistry meters;

    public SuppressingCacheErrorHandler(MeterRegistry meters) {
        this.meters = meters;
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
