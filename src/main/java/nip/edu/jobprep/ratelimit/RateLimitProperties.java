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
        require(enabled != null, "rate-limit.enabled must be set explicitly (true/false), no defaults");
        require(outageStrategy != null && !outageStrategy.isBlank(),
                "rate-limit.outage-strategy must be set explicitly: reject or allow");
        require("reject".equals(outageStrategy) || "allow".equals(outageStrategy),
                "rate-limit.outage-strategy must be reject or allow");
        require(pathPrefix != null && pathPrefix.startsWith("/"),
                "rate-limit.path-prefix must be set explicitly and start with a slash");
        require(windowMs != null && windowMs > 0, "rate-limit.window-ms must be set explicitly and positive");
        require(defaultLimitPerMinute != null && defaultLimitPerMinute > 0,
                "rate-limit.default-limit-per-minute must be set explicitly and positive");
        require(outageRetryAfterSeconds != null && outageRetryAfterSeconds > 0,
                "rate-limit.outage-retry-after-seconds must be set explicitly and positive");
        require(poolMaxActive != null && poolMaxIdle != null && poolMinIdle != null && poolCommandTimeoutMs != null
                        && poolMaxActive > 0 && poolMaxIdle > 0 && poolMinIdle >= 0 && poolCommandTimeoutMs > 0,
                "rate-limit.pool* values must all be set explicitly and valid");
        require(poolMaxIdle <= poolMaxActive, "rate-limit.pool-max-idle must be <= pool-max-active");

        require(clients != null, "rate-limit.clients must be configured explicitly");
        clients = List.copyOf(clients);
        Set<String> names = new HashSet<>();
        Set<String> keys = new HashSet<>();
        for (ClientSpec c : clients) {
            require(c.name() != null && !c.name().isBlank(), "rate-limit.clients has a blank name");
            require(names.add(c.name()), "duplicate client name: " + c.name());
            require(c.apiKey() != null && !c.apiKey().isBlank(),
                    "rate-limit.clients." + c.name() + ".api-key is blank (env var missing?)");
            require(keys.add(c.apiKey()), "duplicate api-key across clients: " + c.name());
        }
        require(!enabled || !clients.isEmpty(),
                "rate-limit.enabled=true requires at least one client in rate-limit.clients");

        require(endpoints != null, "rate-limit.endpoints must be configured explicitly");
        endpoints = List.copyOf(endpoints);
        Set<String> ids = new HashSet<>();
        for (EndpointLimit e : endpoints) {
            require(e.id() != null && !e.id().isBlank(), "rate-limit.endpoints has a blank id");
            require(ids.add(e.id()), "duplicate endpoint id: " + e.id());
            require(Set.of("GET", "POST", "PUT", "PATCH", "DELETE").contains(e.method().toUpperCase(Locale.ROOT)),
                    "rate-limit.endpoints." + e.id() + ".method must be a HTTP verb");
            require(e.pattern() != null && e.pattern().startsWith("/"),
                    "rate-limit.endpoints." + e.id() + ".pattern must start with a slash");
            require(e.limitPerMinute() > 0, "rate-limit.endpoints." + e.id() + ".limit-per-minute must be positive");
        }

        require(peakProfiles != null, "rate-limit.peak-profiles must be configured explicitly (use [] when none)");
        peakProfiles = List.copyOf(peakProfiles);
        for (PeakProfile p : peakProfiles) {
            require(p.cron() != null && !p.cron().isBlank(), "rate-limit.peak-profiles.cron is blank");
            try {
                CronExpression.parse(p.cron());
            } catch (IllegalArgumentException ex) {
                throw new IllegalArgumentException("rate-limit.peak-profiles.cron is invalid: " + p.cron());
            }
            require(p.duration() != null && !p.duration().isZero() && !p.duration().isNegative(),
                    "rate-limit.peak-profiles.duration must be positive");
            require(p.multiplier() > 0 && p.multiplier() <= 3,
                    "rate-limit.peak-profiles.multiplier must be in (0; 3]");
        }
    }

    /** fail-fast helper: каждое нарушение конфига роняет старт с точным сообщением (design Q4). */
    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalArgumentException(message);
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
