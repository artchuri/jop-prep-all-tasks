package nip.edu.jobprep.ratelimit;

import java.util.HashMap;
import java.util.Map;

public final class ApiKeyResolver {
    private final Map<String, RateLimitProperties.ClientSpec> byKey;

    public ApiKeyResolver(RateLimitProperties properties) {
        Map<String, RateLimitProperties.ClientSpec> m = new HashMap<>();
        for (RateLimitProperties.ClientSpec c : properties.clients()) {
            m.put(c.apiKey(), c);
        }
        this.byKey = Map.copyOf(m);
    }

    public RateLimitProperties.ClientSpec resolve(String apiKey) {
        if (apiKey == null || apiKey.isBlank()) return null;
        return byKey.get(apiKey);
    }
}
