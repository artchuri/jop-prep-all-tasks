package nip.edu.jobprep.cache;

import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;


@Component
public class CacheInvalidationAfterCommitListener {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationAfterCommitListener.class);

    private final AccountCacheService cache;
    private final MeterRegistry meters;

    public CacheInvalidationAfterCommitListener(AccountCacheService cache, MeterRegistry meters) {
        this.cache = cache;
        this.meters = meters;
    }

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT, fallbackExecution = false)
    public void on(AccountCacheInvalidationRequested event) {
        try {
            cache.invalidate(event.accountId(), event.newVersion());
            meters.counter("cache_invalidation_total", "tier", "inline").increment();
        } catch (Exception e) {
            meters.counter("cache_invalidation_errors_total", "tier", "inline").increment();
            log.warn("tier1 invalidation failed for account {} v{}: {}", event.accountId(), event.newVersion(), e.getMessage());
        }
    }
}