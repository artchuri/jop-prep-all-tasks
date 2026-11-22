package nip.edu.jobprep.ratelimit;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimiterKeyTest {
    @Test
    void sameApiKeySameTag() {
        assertThat(RateLimiter.clientTag("secret-key"))
                .isEqualTo(RateLimiter.clientTag("secret-key"))
                .startsWith("h")
                .hasSize(17);
    }

    @Test
    void differentApiKeysDifferentTags() {
        assertThat(RateLimiter.clientTag("key-a")).isNotEqualTo(RateLimiter.clientTag("key-b"));
    }

    @Test
    void tagNameIsHashTagSafe() {
        String tag = RateLimiter.clientTag("k");
        assertThat(tag).matches("h[0-9a-f]{16}");
    }
}
