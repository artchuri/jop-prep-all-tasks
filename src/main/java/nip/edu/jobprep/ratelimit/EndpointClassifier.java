package nip.edu.jobprep.ratelimit;

import org.springframework.http.server.PathContainer;
import org.springframework.web.util.pattern.PathPattern;
import org.springframework.web.util.pattern.PathPatternParser;

import java.util.List;
import java.util.Locale;

public final class EndpointClassifier {
    public record Endpoint(String id, int limitPerMinute) {
    }

    private record Rule(PathPattern pattern, String method, Endpoint endpoint) {
    }

    private final List<Rule> rules;

    private final Endpoint fallback;

    public EndpointClassifier(RateLimitProperties properties) {
        PathPatternParser parser = new PathPatternParser();
        this.rules = properties.endpoints().stream()
                .map(e -> new Rule(parser.parse(e.pattern()), e.method(),
                        new Endpoint(e.id(), e.limitPerMinute())))
                .toList();
        this.fallback = new Endpoint("default", properties.defaultLimitPerMinute());
    }

    public Endpoint classify(String method, String pathWithinApplication) {
        String m = method.toUpperCase(Locale.ROOT);
        PathContainer path = PathContainer.parsePath(pathWithinApplication);
        Endpoint methodMismatch = null;
        for (Rule r : rules) {
            if (!r.pattern().matches(path)) {
                continue;
            }
            if (r.method().equals(m)) {
                return r.endpoint();
            }
            methodMismatch = r.endpoint();
        }
        return methodMismatch != null ? methodMismatch : fallback;
    }
}
