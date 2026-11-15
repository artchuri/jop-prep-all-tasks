package nip.edu.jobprep.config;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.exceptions.SuppressingCacheErrorHandler;
import nip.edu.jobprep.utils.cache.LuaCacheManager;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.CachingConfigurer;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.interceptor.CacheErrorHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;


@Configuration
@EnableCaching
@EnableConfigurationProperties(AccountCacheProperties.class)
public class CacheConfig implements CachingConfigurer {

    private final MeterRegistry meters;

    public CacheConfig(MeterRegistry meters) {
        this.meters = meters;
        // The CacheErrorHandler is only instantiated on the first cache failure,
        // so its counters never exist on a healthy instance and the dashboard
        // shows "no data". Register them here, at context startup, instead.
        for (String operation : List.of("get", "put", "evict", "clear")) {
            meters.counter("cache_errors_total", "operation", operation);
        }
    }

    @Bean
    public LuaCacheManager luaCacheManager(AccountCacheScripts scripts, MeterRegistry meters, AccountCacheProperties properties) {
        return new LuaCacheManager(scripts, meters, properties);
    }

    @Override
    public CacheErrorHandler errorHandler() {
        return new SuppressingCacheErrorHandler(meters);
    }
}