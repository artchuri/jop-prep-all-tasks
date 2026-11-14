package nip.edu.jobprep.utils.cache;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CacheEvictAfterCommitTest {

    private final Cache cache = mock(Cache.class);
    private final CacheManager cacheManager = mock(CacheManager.class);
    private final CacheEvictAfterCommit evictor = new CacheEvictAfterCommit(cacheManager);

    @AfterEach
    void clearSynchronization() {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.clearSynchronization();
        }
    }

    @Test
    void evictsImmediatelyWithoutTransaction() {
        when(cacheManager.getCache(LuaCache.NAME)).thenReturn(cache);

        evictor.evict(42L, 7L);

        verify(cache).evict(new LuaCache.Invalidate(42L, 7L));
    }

    @Test
    void defersEvictionUntilCommitWhenTransactionIsActive() {
        when(cacheManager.getCache(LuaCache.NAME)).thenReturn(cache);
        TransactionSynchronizationManager.initSynchronization();

        evictor.evict(42L, 7L);

        // Nothing evicted yet: the transaction has not committed.
        verify(cache, never()).evict(any());

        TransactionSynchronizationManager.getSynchronizations().forEach(TransactionSynchronization::afterCommit);

        ArgumentCaptor<Object> key = ArgumentCaptor.forClass(Object.class);
        verify(cache).evict(key.capture());
        assertThat(key.getValue()).isEqualTo(new LuaCache.Invalidate(42L, 7L));
    }

    @Test
    void neverEvictsOnRollback() {
        when(cacheManager.getCache(LuaCache.NAME)).thenReturn(cache);
        TransactionSynchronizationManager.initSynchronization();

        evictor.evict(42L, 7L);

        // Simulate rollback: afterCompletion is called, afterCommit is not.
        var syncs = TransactionSynchronizationManager.getSynchronizations();
        syncs.forEach(s -> s.afterCompletion(TransactionSynchronization.STATUS_ROLLED_BACK));

        verify(cache, never()).evict(any());
    }

    @Test
    void cacheFailureDoesNotBreakTheWrite() {
        when(cacheManager.getCache(LuaCache.NAME)).thenThrow(new RuntimeException("redis down"));

        // must not propagate
        evictor.evict(42L, 7L);

        verify(cache, never()).evict(any());
    }
}
