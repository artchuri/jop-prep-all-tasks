package nip.edu.jobprep.ratelimit;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class RateLimitPropertiesValidationTest {
    private static RateLimitProperties.EndpointLimit getAccount(int limit) {
        return new RateLimitProperties.EndpointLimit("get-account", "GET", "/v1/account/*", limit);
    }

    private static RateLimitProperties.ClientSpec client(String name, String key) {
        return new RateLimitProperties.ClientSpec(name, key);
    }

    @Test
    void validConfigPasses() {
        assertThatCode(() -> new RateLimitProperties(
                true, "reject", "/v1", 60000L, 600, 30,
                List.of(client("client-a", "key-a")),
                List.of(getAccount(6000)),
                16, 8, 2, 25,
                List.of(new RateLimitProperties.PeakProfile("* * * * * *", Duration.ofSeconds(10), 2.0))
        )).doesNotThrowAnyException();
    }

    @Test
    void blankApiKeyFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(client("client-a", " ")), List.of(), 16, 8, 2, 25, List.of()));
    }

    @Test
    void duplicateApiKeyFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(client("client-a", "same"), client("client-b", "same")),
                        List.of(), 16, 8, 2, 25, List.of()));
    }

    @Test
    void duplicateClientNameFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(client("client-a", "k1"), client("client-a", "k2")),
                        List.of(), 16, 8, 2, 25, List.of()));
    }

    @Test
    void nonPositiveLimitFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 0, 30,
                        List.of(), List.of(), 16, 8, 2, 25, List.of()));
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(), List.of(getAccount(0)), 16, 8, 2, 25, List.of()));
    }

    @Test
    void badOutageStrategyFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "whatever", "/v1", 60000L, 600, 30,
                        List.of(), List.of(), 16, 8, 2, 25, List.of()));
    }

    @Test
    void multiplierAboveThreeFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(), List.of(), 16, 8, 2, 25,
                        List.of(new RateLimitProperties.PeakProfile("* * * * * *", Duration.ofSeconds(10), 3.5))));
    }

    @Test
    void invalidCronFails() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(), List.of(), 16, 8, 2, 25,
                        List.of(new RateLimitProperties.PeakProfile("not a cron", Duration.ofSeconds(10), 2.0))));
    }

    @Test
    void methodMustBeHttpVerb() {
        assertThatIllegalArgumentException().isThrownBy(() ->
                new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                        List.of(),
                        List.of(new RateLimitProperties.EndpointLimit("bad", "BREW", "/v1/x", 10)),
                        16, 8, 2, 25, List.of()));
    }
}
