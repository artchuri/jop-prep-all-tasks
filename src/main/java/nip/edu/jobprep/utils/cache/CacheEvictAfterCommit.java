package nip.edu.jobprep.utils.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;


@Component
public class CacheEvictAfterCommit {

    private static final Logger log = LoggerFactory.getLogger(CacheEvictAfterCommit.class);

    private final CacheManager cacheManager;

    public CacheEvictAfterCommit(CacheManager cacheManager) {
        this.cacheManager = cacheManager;
    }


    public void evict(long accountId, long version) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            evictNow(accountId, version);
            return;
        }
        TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
            @Override
            public void afterCommit() {
                evictNow(accountId, version);
            }
        });
    }

    private void evictNow(long accountId, long version) {
        try {
            Cache cache = cacheManager.getCache(LuaCache.NAME);
            if (cache == null) {
                log.warn("no cache named {} - skipping eviction for account {}", LuaCache.NAME, accountId);
                return;
            }
            cache.evict(new LuaCache.Invalidate(accountId, version));
        } catch (RuntimeException e) {
            log.warn("post-commit eviction failed for account {} v{}: {}", accountId, version, e.getMessage());
        }
    }
}
