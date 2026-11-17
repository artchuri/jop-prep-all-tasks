package nip.edu.jobprep.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "account-cache")
public record AccountCacheProperties(
        @DefaultValue("60000") long ttlMs,
        @DefaultValue("9000") long ttlJitterMs,
        @DefaultValue("3000") long leaseMs,
        @DefaultValue("1000") long marginMs,
        @DefaultValue("5000") long staleMs
) {
}