package nip.edu.jobprep.ratelimit;
import lombok.extern.slf4j.Slf4j;


import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;

@Slf4j
public final class RateLimiter {

    private final RateLimitScripts scripts;

    private final RateLimitProperties properties;

    private final RateLimitMetrics metrics;

    public RateLimiter(RateLimitScripts scripts, RateLimitProperties properties, RateLimitMetrics metrics) {
        this.scripts = scripts;
        this.properties = properties;
        this.metrics = metrics;
    }

    public Reservation reserve(RateLimitProperties.ClientSpec client,
                               EndpointClassifier.Endpoint endpoint,
                               double effectiveLimit,
                               long nowMs) {
        long window = properties.windowMs();
        String base = "{" + clientTag(client.apiKey()) + "}:rate:" + endpoint.id();
        long bucket = Math.floorDiv(nowMs, window);
        List<String> keys = List.of(base + ":b" + bucket, base + ":b" + (bucket - 1));
        try {
            RateLimitScripts.ReserveResult r = scripts.reserve(keys, nowMs, window, effectiveLimit);
            metrics.windowUsage(client.name(), endpoint.id(), r.used(), effectiveLimit);
            return new Reservation(client, endpoint, r.allowed(), r.retryAfterMs(), r.used(), keys.getFirst());
        } catch (RuntimeException e) {
            log.warn("rate limit reserve failed: {}: {}", e.getClass().getSimpleName(), e.getMessage());
            throw new RateLimitOutageException(e);
        }
    }

    public void refund(Reservation reservation) {
        if (!reservation.allowed()) return;
        scripts.refund(reservation.bucketKey());
    }

    public static String clientTag(String apiKey) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(apiKey.getBytes(StandardCharsets.UTF_8));
            return "h" + HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    public record Reservation(RateLimitProperties.ClientSpec client,
                              EndpointClassifier.Endpoint endpoint,
                              boolean allowed,
                              long retryAfterMs,
                              double used,
                              String bucketKey) {
    }
}
