package nip.edu.jobprep.ratelimit;

import nip.edu.jobprep.controller.AccountController;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.service.AccountService;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyDouble;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = AccountController.class)
@TestPropertySource(properties = {
        "rate-limit.enabled=true",
        "rate-limit.outage-strategy=allow"
})

@Import(RateLimitFilterAllowStrategyTest.Mocks.class)
class RateLimitFilterAllowStrategyTest {
    @Autowired MockMvc mvc;

    @MockitoBean
    AccountService accountService;

    @MockitoBean RateLimiter limiter;

    @MockitoBean RateLimitMetrics metrics;
    @MockitoBean ApiKeyResolver resolver;
    @MockitoBean EndpointClassifier classifier;
    @MockitoBean PeakProfileEvaluator peaks;

    @TestConfiguration

    @EnableConfigurationProperties(RateLimitProperties.class)
    static class Mocks {
        @Bean
        Clock rateLimitClock() { return Clock.fixed(Instant.parse("2026-11-19T12:00:00Z"), ZoneOffset.UTC); }
        @Bean
        RateLimitFilter rateLimitFilter(RateLimitProperties properties,
                                        ApiKeyResolver resolver,
                                        EndpointClassifier classifier,
                                        RateLimiter limiter,
                                        RateLimitMetrics metrics,
                                        PeakProfileEvaluator peaks,
                                        Clock rateLimitClock,
                                        ObjectMapper objectMapper) {
            return new RateLimitFilter(properties, resolver, classifier, limiter, metrics, peaks, rateLimitClock, objectMapper);
        }
    }

    @BeforeEach
    void defaults() {
        when(resolver.resolve("key-ok")).thenReturn(new RateLimitProperties.ClientSpec("client-a", "key-ok"));
        when(classifier.classify(any(), any())).thenReturn(new EndpointClassifier.Endpoint("get-account", 6000));
        when(limiter.reserve(any(), any(), anyDouble(), anyLong()))
                .thenThrow(new RateLimitOutageException(new RuntimeException("redis down")));
    }

    @Test
    void outageBypassesInsteadOf503() throws Exception {
        when(accountService.get(999999L)).thenThrow(new nip.edu.jobprep.exceptions.AccountNotFoundException("account 999999 not found"));
        mvc.perform(get("/v1/account/999999").header("X-API-Key", "key-ok"))
                .andExpect(status().isNotFound());
    }
}
