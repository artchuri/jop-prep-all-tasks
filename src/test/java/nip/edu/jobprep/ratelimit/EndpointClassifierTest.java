package nip.edu.jobprep.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointClassifierTest {
    private final EndpointClassifier classifier = new EndpointClassifier(new RateLimitProperties(
            false, "reject", "/v1", 60000L, 600, 30,
            List.of(),
            List.of(
                    new RateLimitProperties.EndpointLimit("get-account", "GET", "/v1/account/*", 6000),
                    new RateLimitProperties.EndpointLimit("freeze", "POST", "/v1/account/*/freeze", 600),
                    new RateLimitProperties.EndpointLimit("transfer", "POST", "/v1/transfer", 1200)),
            16, 8, 2, 25, List.of()));

    @Test
    void getAccountMatches() {
        assertThat(classifier.classify("GET", "/v1/account/42").id()).isEqualTo("get-account");
    }

    @Test
    void freezeMatchesNestedPattern() {
        assertThat(classifier.classify("POST", "/v1/account/42/freeze").id()).isEqualTo("freeze");
    }

    @Test
    void transferMatchesExactPath() {
        assertThat(classifier.classify("POST", "/v1/transfer").id()).isEqualTo("transfer");
    }

    @Test
    void wrongMethodOnKnownPathStaysInItsBucket() {
        assertThat(classifier.classify("GET", "/v1/transfer").id()).isEqualTo("transfer");
    }

    @Test
    void unknownPathFallsBackToDefault() {
        EndpointClassifier.Endpoint e = classifier.classify("GET", "/v1/does-not-exist");
        assertThat(e.id()).isEqualTo("default");
        assertThat(e.limitPerMinute()).isEqualTo(600);
    }
}
