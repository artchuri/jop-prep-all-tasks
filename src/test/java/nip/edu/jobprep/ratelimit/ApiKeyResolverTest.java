package nip.edu.jobprep.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ApiKeyResolverTest {
    private final ApiKeyResolver resolver = new ApiKeyResolver(new RateLimitProperties(
            false, "reject", "/v1", 60000L, 600, 30,
            List.of(new RateLimitProperties.ClientSpec("mobile", "key-mobile"),
                    new RateLimitProperties.ClientSpec("partner", "key-partner")),
            List.of(), 16, 8, 2, 25, List.of()));

    @Test
    void knownKeyResolves() {
        assertThat(resolver.resolve("key-mobile").name()).isEqualTo("mobile");
        assertThat(resolver.resolve("key-partner").name()).isEqualTo("partner");
    }

    @Test
    void nullOrBlankOrUnknownResolveToNull() {
        assertThat(resolver.resolve(null)).isNull();
        assertThat(resolver.resolve("")).isNull();
        assertThat(resolver.resolve("nope")).isNull();
    }
}
