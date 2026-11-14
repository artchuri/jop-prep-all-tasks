package nip.edu.jobprep.utils.cache;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.config.AccountCacheProperties;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;

import java.util.Collection;
import java.util.List;


public class LuaCacheManager implements CacheManager {

    private final LuaCache cache;

    public LuaCacheManager(AccountCacheScripts scripts, MeterRegistry meters, AccountCacheProperties properties) {
        this.cache = new LuaCache(scripts, meters, properties);
    }

    @Override
    public Cache getCache(String name) {
        return LuaCache.NAME.equals(name) ? cache : null;
    }

    @Override
    public Collection<String> getCacheNames() {
        return List.of(LuaCache.NAME);
    }
}