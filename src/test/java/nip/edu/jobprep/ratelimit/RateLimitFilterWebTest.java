package nip.edu.jobprep.ratelimit;

import nip.edu.jobprep.controller.AccountController;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.service.AccountService;
import org.mockito.Mockito;
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
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(controllers = AccountController.class)
@TestPropertySource(properties = {
        "rate-limit.enabled=true",
        "rate-limit.outage-retry-after-seconds=30"
})
@Import(RateLimitFilterWebTest.Mocks.class)
class RateLimitFilterWebTest {
    static final Instant NOW = Instant.parse("2026-11-19T12:00:00Z");

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
        Clock rateLimitClock() { return Clock.fixed(NOW, ZoneOffset.UTC); }
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
        when(peaks.multiplierAt(anyLong())).thenReturn(1.0);
    }

    private RateLimiter.Reservation allowed() {
        return new RateLimiter.Reservation(
                new RateLimitProperties.ClientSpec("client-a", "key-ok"),
                new EndpointClassifier.Endpoint("get-account", 6000),
                true, 0, 1.0, "{" + RateLimiter.clientTag("key-ok") + "}:rate:get-account:b1");
    }

    private RateLimiter.Reservation rejected(long retryAfterMs) {
        return new RateLimiter.Reservation(
                new RateLimitProperties.ClientSpec("client-a", "key-ok"),
                new EndpointClassifier.Endpoint("get-account", 6000),
                false, retryAfterMs, 6000.0, "unused");
    }

    @Test
    void missingKeyIs401WithoutTouchingTheLimiter() throws Exception {
        mvc.perform(get("/v1/account/1"))
                .andExpect(status().isUnauthorized());
        Mockito.verify(limiter, Mockito.never()).reserve(any(), any(), anyDouble(), anyLong());
    }

    @Test
    void unknownKeyIs401() throws Exception {
        mvc.perform(get("/v1/account/1").header("X-API-Key", "nope"))
                .andExpect(status().isUnauthorized());
    }

    @Test
    void allowedRequestPasses() throws Exception {
        when(limiter.reserve(any(), any(), anyDouble(), anyLong())).thenReturn(allowed());
        mvc.perform(get("/v1/account/1").header("X-API-Key", "key-ok"))
                .andExpect(status().isOk());
    }

    @Test
    void rejectedIs429WithRetryAfter() throws Exception {
        when(limiter.reserve(any(), any(), anyDouble(), anyLong())).thenReturn(rejected(15000L));
        mvc.perform(get("/v1/account/1").header("X-API-Key", "key-ok"))
                .andExpect(status().isTooManyRequests())
                .andExpect(header().string("Retry-After", "15"));
    }

    @Test
    void outageRejects503() throws Exception {
        when(limiter.reserve(any(), any(), anyDouble(), anyLong()))
                .thenThrow(new RateLimitOutageException(new RuntimeException("redis down")));
        mvc.perform(get("/v1/account/1").header("X-API-Key", "key-ok"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "30"));
    }

    @Test
    void notFoundIsCountedNotRefunded() throws Exception {
        when(accountService.get(999999L)).thenThrow(new AccountNotFoundException("account 999999 not found"));
        when(limiter.reserve(any(), any(), anyDouble(), anyLong())).thenReturn(allowed());
        mvc.perform(get("/v1/account/999999").header("X-API-Key", "key-ok"))
                .andExpect(status().isNotFound());
        Mockito.verify(limiter, Mockito.never()).refund(any());
    }

    @Test
    void outageWithAllowStrategyPassesThrough() throws Exception {
        when(limiter.reserve(any(), any(), anyDouble(), anyLong()))
                .thenThrow(new RateLimitOutageException(new RuntimeException("redis down")));
        mvc.perform(get("/v1/account/1").header("X-API-Key", "key-ok"))
                .andExpect(status().isServiceUnavailable());
    }
}
