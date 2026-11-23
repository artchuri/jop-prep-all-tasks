package nip.edu.jobprep.ratelimit;

import java.time.Clock;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockHttpServletRequest;

import static org.assertj.core.api.Assertions.assertThat;

class RateLimitFilterPathTest {
    private static RateLimitProperties props(boolean enabled) {
        return new RateLimitProperties(
                enabled, "reject", "/v1", 60000L, 600, 30,
                List.of(new RateLimitProperties.ClientSpec("c", "k")),
                List.of(), 16, 8, 2, 25, List.of());
    }

    private static RateLimitFilter filter(boolean enabled) {
        return new RateLimitFilter(props(enabled), null, null, null, null, null,
                Clock.systemUTC(), null);
    }

    private static MockHttpServletRequest request(String uri) {
        MockHttpServletRequest r = new MockHttpServletRequest();
        r.setRequestURI(uri);
        return r;
    }

    private static MockHttpServletRequest requestWithContext(String context, String uri) {
        MockHttpServletRequest r = request(uri);
        r.setContextPath(context);
        return r;
    }

    @Test
    void disabledLimiterFiltersNothing() {
        assertThat(filter(false).shouldNotFilter(request("/v1/account/1"))).isTrue();
    }

    @Test
    void businessPrefixIsNotFiltered() {
        assertThat(filter(true).shouldNotFilter(request("/v1/account/1"))).isFalse();
        assertThat(filter(true).shouldNotFilter(request("/v1/transfer"))).isFalse();
    }

    @Test
    void actuatorAndForeignPathsAreNotFiltered() {
        assertThat(filter(true).shouldNotFilter(request("/actuator/health"))).isTrue();
        assertThat(filter(true).shouldNotFilter(request("/error"))).isTrue();
    }

    @Test
    void siblingPrefixDoesNotMatch() {
        assertThat(filter(true).shouldNotFilter(request("/v10/account/1"))).isTrue();
    }

    @Test
    void contextPathIsStrippedBeforeMatching() {
        MockHttpServletRequest r = requestWithContext("/api", "/api/v1/account/1");
        assertThat(filter(true).shouldNotFilter(r)).isFalse();
        assertThat(RateLimitFilter.pathWithinApplication(r)).isEqualTo("/v1/account/1");
        MockHttpServletRequest foreign = requestWithContext("/api", "/api/actuator/health");
        assertThat(filter(true).shouldNotFilter(foreign)).isTrue();
    }
}
