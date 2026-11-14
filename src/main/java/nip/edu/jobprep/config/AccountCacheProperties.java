package nip.edu.jobprep.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Tuning knobs of the account cache-aside layer.
 *
 * <p>In a deployment these values are set explicitly per instance in
 * {@code deploy/compose.yaml} ({@code ACCOUNT_CACHE_*} env vars): the deployment
 * descriptor is the single place where the staleness bound of an environment is
 * decided. The defaults below only keep local runs and tests working.
 *
 * @param ttlMs        entry lifetime (last-resort staleness bound) when invalidation is lost
 * @param ttlJitterMs  random spread added to {@code ttlMs} to avoid synchronized expiry
 * @param leaseMs      lease a cache filler holds while loading from PostgreSQL
 * @param marginMs     extra time kept beyond the lease so a filler can still write
 * @param staleMs      window during which an invalidated entry may still be served
 * @param waitAttempts how many times a WAIT-ing reader re-checks the cache
 * @param waitPauseMs  pause between WAIT re-checks
 */
@ConfigurationProperties(prefix = "account-cache")
public record AccountCacheProperties(
        @DefaultValue("60000") long ttlMs,
        @DefaultValue("9000") long ttlJitterMs,
        @DefaultValue("3000") long leaseMs,
        @DefaultValue("1000") long marginMs,
        @DefaultValue("5000") long staleMs,
        @DefaultValue("3") int waitAttempts,
        @DefaultValue("20") long waitPauseMs
) {
}