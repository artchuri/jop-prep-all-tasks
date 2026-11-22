package nip.edu.jobprep.ratelimit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

@ConfigurationProperties(prefix = "rate-limit")
public record RateLimitProperties(

        Boolean enabled,
        String outageStrategy,
        String pathPrefix,
        Long windowMs,
        Integer defaultLimitPerMinute,
        Integer outageRetryAfterSeconds,
        List<ClientSpec> clients,
        List<EndpointLimit> endpoints,
        Integer poolMaxActive,
        Integer poolMaxIdle,
        Integer poolMinIdle,
        Integer poolCommandTimeoutMs,
        List<PeakProfile> peakProfiles
) {
    public RateLimitProperties {
        if (enabled == null) throw new IllegalArgumentException("rate-limit.enabled must be set explicitly (true/false), no defaults");
        if (outageStrategy == null || outageStrategy.isBlank())
            throw new IllegalArgumentException("rate-limit.outage-strategy must be set explicitly: reject or allow");
        if (!"reject".equals(outageStrategy) && !"allow".equals(outageStrategy))
            throw new IllegalArgumentException("rate-limit.outage-strategy must be reject or allow");
        if (pathPrefix == null || !pathPrefix.startsWith("/"))
            throw new IllegalArgumentException("rate-limit.path-prefix must be set explicitly and start with a slash");
        if (windowMs == null || windowMs <= 0) throw new IllegalArgumentException("rate-limit.window-ms must be set explicitly and positive");
        if (defaultLimitPerMinute == null || defaultLimitPerMinute <= 0)
            throw new IllegalArgumentException("rate-limit.default-limit-per-minute must be set explicitly and positive");
        if (outageRetryAfterSeconds == null || outageRetryAfterSeconds <= 0)
            throw new IllegalArgumentException("rate-limit.outage-retry-after-seconds must be set explicitly and positive");
        if (poolMaxActive == null || poolMaxIdle == null || poolMinIdle == null || poolCommandTimeoutMs == null
                || poolMaxActive <= 0 || poolMaxIdle <= 0 || poolMinIdle < 0 || poolCommandTimeoutMs <= 0)
            throw new IllegalArgumentException("rate-limit.pool* values must all be set explicitly and valid");
        if (poolMaxIdle > poolMaxActive) throw new IllegalArgumentException("rate-limit.pool-max-idle must be <= pool-max-active");
        if (clients == null)
            throw new IllegalArgumentException("rate-limit.clients must be configured explicitly");
        clients = List.copyOf(clients);
        Set<String> names = new HashSet<>();
        Set<String> keys = new HashSet<>();
        for (ClientSpec c : clients) {
            if (c.name() == null || c.name().isBlank())
                throw new IllegalArgumentException("rate-limit.clients has a blank name");
            if (!names.add(c.name()))
                throw new IllegalArgumentException("duplicate client name: " + c.name());
            if (c.apiKey() == null || c.apiKey().isBlank())
                throw new IllegalArgumentException("rate-limit.clients." + c.name() + ".api-key is blank (env var missing?)");
            if (!keys.add(c.apiKey()))
                throw new IllegalArgumentException("duplicate api-key across clients: " + c.name());
        }
        if (enabled && clients.isEmpty())
            throw new IllegalArgumentException("rate-limit.enabled=true requires at least one client in rate-limit.clients");
        if (endpoints == null)
            throw new IllegalArgumentException("rate-limit.endpoints must be configured explicitly");
        endpoints = List.copyOf(endpoints);
        Set<String> ids = new HashSet<>();
        for (EndpointLimit e : endpoints) {
            if (e.id() == null || e.id().isBlank())
                throw new IllegalArgumentException("rate-limit.endpoints has a blank id");
            if (!ids.add(e.id()))
                throw new IllegalArgumentException("duplicate endpoint id: " + e.id());
            if (e.method() == null || !Set.of("GET", "POST", "PUT", "PATCH", "DELETE").contains(e.method().toUpperCase(Locale.ROOT)))
                throw new IllegalArgumentException("rate-limit.endpoints." + e.id() + ".method must be a HTTP verb");
            if (e.pattern() == null || !e.pattern().startsWith("/"))
                throw new IllegalArgumentException("rate-limit.endpoints." + e.id() + ".pattern must start with a slash");
            if (e.limitPerMinute() <= 0)
                throw new IllegalArgumentException("rate-limit.endpoints." + e.id() + ".limit-per-minute must be positive");
        }
        if (peakProfiles == null)
            throw new IllegalArgumentException("rate-limit.peak-profiles must be configured explicitly (use [] when none)");
        peakProfiles = List.copyOf(peakProfiles);
        for (PeakProfile p : peakProfiles) {
            if (p.cron() == null || p.cron().isBlank())
                throw new IllegalArgumentException("rate-limit.peak-profiles.cron is blank");
            try {
                CronExpression.parse(p.cron());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("rate-limit.peak-profiles.cron is invalid: " + p.cron());
            }
            if (p.duration() == null || p.duration().isZero() || p.duration().isNegative())
                throw new IllegalArgumentException("rate-limit.peak-profiles.duration must be positive");
            if (p.multiplier() <= 0 || p.multiplier() > 3)
                throw new IllegalArgumentException("rate-limit.peak-profiles.multiplier must be in (0; 3]");
        }
    }

    public record ClientSpec(String name, String apiKey) {
    }

    public record EndpointLimit(String id, String method, String pattern, int limitPerMinute) {
        public EndpointLimit {
            if (id == null || id.isBlank())
                throw new IllegalArgumentException("rate-limit.endpoints has a blank id");
            if (method == null || method.isBlank())
                throw new IllegalArgumentException("rate-limit.endpoints." + id + ".method must be set explicitly (GET/POST/...), no defaults");
            method = method.toUpperCase(Locale.ROOT);
        }
    }

    public record PeakProfile(String cron, Duration duration, double multiplier) {
    }
}
