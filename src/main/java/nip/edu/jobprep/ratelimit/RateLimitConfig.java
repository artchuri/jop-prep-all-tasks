package nip.edu.jobprep.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.jedis.JedisClientConfiguration;
import org.springframework.data.redis.connection.jedis.JedisConnectionFactory;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(RateLimitProperties.class)
public class RateLimitConfig {
    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public RateLimitMetrics rateLimitMetrics(MeterRegistry meters, RateLimitProperties properties) {
        return new RateLimitMetrics(meters, properties);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public RateLimitScripts rateLimitScripts(
            @Value("${spring.data.redis.host}") String host,
            @Value("${spring.data.redis.port}") int port,
            RateLimitProperties properties,
            RateLimitMetrics metrics) {
        RedisStandaloneConfiguration standalone = new RedisStandaloneConfiguration(host, port);
        JedisPoolConfig pool = new JedisPoolConfig();
        pool.setMaxTotal(properties.poolMaxActive());
        pool.setMaxIdle(properties.poolMaxIdle());
        pool.setMinIdle(properties.poolMinIdle());
        pool.setMaxWait(Duration.ofMillis(properties.poolCommandTimeoutMs()));
        pool.setTestOnBorrow(true);
        JedisClientConfiguration client = JedisClientConfiguration.builder()
                .usePooling().poolConfig(pool).and()
                .connectTimeout(Duration.ofMillis(properties.poolCommandTimeoutMs()))
                .readTimeout(Duration.ofMillis(properties.poolCommandTimeoutMs()))
                .build();
        JedisConnectionFactory factory = new JedisConnectionFactory(standalone, client);
        factory.afterPropertiesSet();
        return new RateLimitScripts(factory, metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public FilterRegistrationBean<RateLimitFilter> rateLimitFilter(
            RateLimitProperties properties,
            ApiKeyResolver apiKeyResolver,
            EndpointClassifier endpointClassifier,
            RateLimiter rateLimiter,
            RateLimitMetrics metrics,
            PeakProfileEvaluator peakProfiles,
            Clock rateLimitClock,
            tools.jackson.databind.ObjectMapper objectMapper) {
        RateLimitFilter filter = new RateLimitFilter(properties, apiKeyResolver, endpointClassifier,
                rateLimiter, metrics, peakProfiles, rateLimitClock, objectMapper);
        FilterRegistrationBean<RateLimitFilter> reg = new FilterRegistrationBean<>(filter);
        reg.setOrder(10);
        reg.addUrlPatterns("/*");
        return reg;
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public ApiKeyResolver apiKeyResolver(RateLimitProperties properties) {
        return new ApiKeyResolver(properties);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public EndpointClassifier endpointClassifier(RateLimitProperties properties) {
        return new EndpointClassifier(properties);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public RateLimiter rateLimiter(RateLimitScripts scripts, RateLimitProperties properties, RateLimitMetrics metrics) {
        return new RateLimiter(scripts, properties, metrics);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public PeakProfileEvaluator peakProfileEvaluator(RateLimitProperties properties) {
        return new PeakProfileEvaluator(properties);
    }

    @Bean
    @ConditionalOnProperty(name = "rate-limit.enabled", havingValue = "true")
    public Clock rateLimitClock() {
        return Clock.systemUTC();
    }
}
