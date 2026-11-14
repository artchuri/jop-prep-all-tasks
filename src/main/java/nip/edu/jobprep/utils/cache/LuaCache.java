package nip.edu.jobprep.utils.cache;

import nip.edu.jobprep.dto.AccountResponseDto;
import java.math.BigDecimal;
import nip.edu.jobprep.config.AccountCacheProperties;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.cache.Cache;
import org.springframework.cache.support.SimpleValueWrapper;
import org.springframework.lang.Nullable;

import java.util.concurrent.Callable;


public class LuaCache implements Cache {

    public static final String NAME = "accounts";

    private final AccountCacheScripts scripts;
    private final MeterRegistry meters;
    private final AccountCacheProperties properties;

    public LuaCache(AccountCacheScripts scripts, MeterRegistry meters, AccountCacheProperties properties) {
        this.scripts = scripts;
        this.meters = meters;
        this.properties = properties;
    }

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public Object getNativeCache() {
        return scripts;
    }

    @Nullable
    @Override
    public ValueWrapper get(Object key) {
        ReadResult r = read(key);
        return r.value() == null ? null : new SimpleValueWrapper(toDto(r.value()));
    }

    @SuppressWarnings("unchecked")
    @Nullable
    @Override
    public <T> T get(Object key, @Nullable Class<T> type) {
        ReadResult r = read(key);
        if (r.value() == null) {
            return null;
        }
        T dto = (T) toDto(r.value());
        if (type != null && !type.isInstance(dto)) {
            throw new IllegalStateException("Cached value is not of required type [" + type.getName() + "]: " + dto);
        }
        return dto;
    }


    @Nullable
    @Override
    public <T> T get(Object key, Callable<T> valueLoader) {
        ReadResult r = read(key);
        if (r.value() != null) {
            return (T) toDto(r.value());
        }
        if (r.leaseToken() != null) {
            T loaded = loadUnderLease(key, r.leaseToken(), valueLoader);
            if (loaded != null) {
                return loaded;
            }
            r = read(key);
            if (r.value() != null) {
                return (T) toDto(r.value());
            }
        }
        try {
            return valueLoader.call();
        } catch (Exception e) {
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    private <T> T loadUnderLease(Object key, String token, Callable<T> valueLoader) {
        try {
            T loaded = valueLoader.call();
            if (loaded instanceof AccountResponseDto dto) {
                scripts.fill(dto.accountId(), token, dto.version(), dto.balance().toPlainString(), dto.status());
            }
            return loaded;
        } catch (Exception e) {
            scripts.release(asLong(key), token);
            throw new ValueRetrievalException(key, valueLoader, e);
        }
    }

    @Override
    public void put(Object key, @Nullable Object value) {
    }

    @Override
    public ValueWrapper putIfAbsent(Object key, @Nullable Object value) {
        return null;
    }


    @Override
    public void evict(Object key) {
        try {
            if (key instanceof Invalidate i) {
                scripts.invalidate(i.accountId(), i.version());
            } else {
                scripts.invalidate(asLong(key), Long.MAX_VALUE);
            }
            meters.counter("cache_evict_total", "result", "success").increment();
        } catch (RuntimeException e) {
            meters.counter("cache_evict_total", "result", "error").increment();
            throw e;
        }
    }

    @Override
    public boolean evictIfPresent(Object key) {
        evict(key);
        return true;
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException("clear() is not supported for version-gated Lua cache");
    }

    private ReadResult read(Object key) {
        long accountId = asLong(key);
        AccountCacheScripts.ReadResult r = scripts.read(accountId);
        return switch (r.status()) {
            case HIT, STALE -> new ReadResult(new AccountSnapshot(accountId, r.balance(), r.statusName(), r.version()), null);
            case LEASE -> new ReadResult(null, r.token());
            case WAIT -> new ReadResult(waitThenRead(accountId), null);
        };
    }

    private AccountSnapshot waitThenRead(long accountId) {
        for (int i = 0; i < properties.waitAttempts(); i++) {
            try {
                Thread.sleep(properties.waitPauseMs());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
            AccountCacheScripts.ReadResult r = scripts.read(accountId);
            if (r.status() == AccountCacheScripts.Status.HIT || r.status() == AccountCacheScripts.Status.STALE) {
                return new AccountSnapshot(accountId, r.balance(), r.statusName(), r.version());
            }
            if (r.status() == AccountCacheScripts.Status.LEASE) {
                return null;
            }
        }
        return null;
    }

    private static AccountResponseDto toDto(AccountSnapshot s) {
        return new AccountResponseDto(s.accountId(), new BigDecimal(s.balance()), s.status(), s.version());
    }

    private static long asLong(Object key) {
        if (key instanceof Number n) return n.longValue();
        return Long.parseLong(String.valueOf(key));
    }

    public record AccountSnapshot(long accountId, String balance, String status, long version) {
    }

    public record Invalidate(long accountId, long version) {
    }

    private record ReadResult(AccountSnapshot value, String leaseToken) {
    }
}