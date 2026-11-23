package nip.edu.jobprep.ratelimit;

import org.springframework.data.redis.core.RedisCallback;
import nip.edu.jobprep.cache.AbstractCacheIntegrationTest;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18083",
        "rate-limit.enabled=true",
        "rate-limit.clients[0].name=sat-client",
        "rate-limit.clients[0].api-key=sat-key",
        "rate-limit.endpoints[0].id=get-account",
        "rate-limit.endpoints[0].method=GET",
        "rate-limit.endpoints[0].pattern=/v1/account/*",
        "rate-limit.endpoints[0].limit-per-minute=3"
})
class RateLimitSaturationIT extends AbstractCacheIntegrationTest {
    private static final long ACCOUNT_ID = 78L;
    private static final String BASE = "http://localhost:18083";

    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Autowired JdbcTemplate jdbc;
    @Autowired StringRedisTemplate redis;
    @Autowired MeterRegistry meters;

    private record Reply(int status, String retryAfter, String body) {
    }

    private Reply send(String method, String path, String json) throws Exception {
        HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(BASE + path))
                .header("X-API-Key", "sat-key")
                .timeout(Duration.ofSeconds(10));
        if ("POST".equals(method)) {
            builder.header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(json));
        } else {
            builder.GET();
        }
        HttpResponse<String> response = HTTP.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(),
                response.headers().firstValue("Retry-After").orElse(null), response.body());
    }

    @BeforeEach
    void seedAndResetWindow() throws Exception {
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (" + ACCOUNT_ID + ", now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (%d, %d, 'ACTIVE', now(), now(), now(), 0, 100.00, 'RUB')
                """.formatted(ACCOUNT_ID, ACCOUNT_ID));
        redis.execute((RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        long intoWindow = System.currentTimeMillis() % 60000;
        if (intoWindow > 55000) {
            Thread.sleep(60000 - intoWindow + 200);
        }
    }

    @Test
    void underLimitAllTwoXX_overLimit429WithRetryAfter_otherBucketsAlive() throws Exception {
        for (int i = 0; i < 3; i++) {
            Reply ok = send("GET", "/v1/account/" + ACCOUNT_ID, null);
            assertThat(ok.status()).as("request %d", i + 1).isEqualTo(200);
        }
        assertThat(meters.get("rate_limit_requests_total")
                .tags("client", "sat-client", "endpoint", "get-account", "result", "accepted")
                .counter().count()).isEqualTo(3.0);
        Reply limited = send("GET", "/v1/account/" + ACCOUNT_ID, null);
        assertThat(limited.status()).isEqualTo(429);
        assertThat(limited.retryAfter()).isNotNull();
        assertThat(Integer.parseInt(limited.retryAfter())).isGreaterThanOrEqualTo(1);
        assertThat(limited.body()).contains("RATE_LIMITED");
        assertThat(meters.get("rate_limit_requests_total")
                .tags("client", "sat-client", "endpoint", "get-account", "result", "rejected")
                .counter().count()).isGreaterThanOrEqualTo(1.0);
        Reply freeze = send("POST", "/v1/account/" + ACCOUNT_ID + "/freeze", "{\"changedByUserId\": 1}");
        assertThat(freeze.status()).as("freeze body: %s", freeze.body()).isEqualTo(200);
    }
}
