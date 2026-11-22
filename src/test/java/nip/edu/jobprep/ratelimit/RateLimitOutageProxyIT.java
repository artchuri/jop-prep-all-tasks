package nip.edu.jobprep.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import nip.edu.jobprep.cache.AbstractCacheIntegrationTest;
import nip.edu.jobprep.cache.TestRedis;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
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
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT, properties = {
        "server.port=18084",
        "rate-limit.enabled=true",
        "rate-limit.clients[0].name=sat-client",
        "rate-limit.clients[0].api-key=sat-key"
})
class RateLimitOutageProxyIT extends AbstractCacheIntegrationTest {
    private static final long ACCOUNT_ID = 79L;
    private static final String BASE = "http://localhost:18084";

    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @Autowired JdbcTemplate jdbc;
    @Autowired StringRedisTemplate redis;
    @Autowired MeterRegistry meters;

    private record Reply(int status, String retryAfter, String body) {
    }

    private Reply getAccount() throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create(BASE + "/v1/account/" + ACCOUNT_ID))
                .header("X-API-Key", "sat-key")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(),
                response.headers().firstValue("Retry-After").orElse(null), response.body());
    }

    @BeforeEach
    void seedAndReset() {
        TestRedis.REDIS_PROXY.setConnectionCut(false);
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (" + ACCOUNT_ID + ", now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (%d, %d, 'ACTIVE', now(), now(), now(), 0, 100.00, 'RUB')
                """.formatted(ACCOUNT_ID, ACCOUNT_ID));
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
    }

    @Test
    @Order(1)
    void outageMetersExistAtStartup() {
        assertThat(meters.get("rate_limit_outage_total").tags("client", "sat-client").counter().count()).isEqualTo(0.0);
    }

    @Test
    @Order(2)
    void redisOutageFailsClosed503_thenRecovers() throws Exception {
        Reply warm = getAccount();
        assertThat(warm.status()).isEqualTo(200);
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        Reply outage = getAccount();
        assertThat(outage.status()).isEqualTo(503);
        assertThat(outage.retryAfter()).isEqualTo("30");
        assertThat(outage.body()).contains("RATE_LIMITER_UNAVAILABLE");
        assertThat(meters.get("rate_limit_outage_total").tags("client", "sat-client").counter().count())
                .isGreaterThanOrEqualTo(1.0);
        TestRedis.REDIS_PROXY.setConnectionCut(false);
        Reply recovered = getAccount();
        assertThat(recovered.status()).isEqualTo(200);
    }
}
