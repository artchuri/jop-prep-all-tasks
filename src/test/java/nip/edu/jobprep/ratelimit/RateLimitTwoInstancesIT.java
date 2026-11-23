package nip.edu.jobprep.ratelimit;

import org.springframework.data.redis.core.RedisCallback;
import nip.edu.jobprep.JobPrepApplication;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

@Tag("integration")
class RateLimitTwoInstancesIT {
    private static final long ACCOUNT_ID = 9001L;

    private static final int PORT_A = 18081;
    private static final int PORT_B = 18082;

    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    static ConfigurableApplicationContext instanceA;
    static ConfigurableApplicationContext instanceB;

    static final HttpClient HTTP = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build();

    @BeforeAll
    static void startTwoInstances() {
        REDIS.start();
        PostgreSQLContainer<?> pg = AbstractIntegrationTest.POSTGRES;
        instanceA = boot(pg, PORT_A);
        instanceB = boot(pg, PORT_B);
    }

    @AfterAll
    static void stopInstances() {
        if (instanceA != null) instanceA.close();
        if (instanceB != null) instanceB.close();
        REDIS.stop();
    }

    private static ConfigurableApplicationContext boot(PostgreSQLContainer<?> pg, int port) {
        return new SpringApplicationBuilder(JobPrepApplication.class)
                .run(
                        "--server.port=" + port,
                        "--spring.datasource.url=" + pg.getJdbcUrl(),
                        "--spring.datasource.username=" + pg.getUsername(),
                        "--spring.datasource.password=" + pg.getPassword(),
                        "--spring.data.redis.host=" + REDIS.getHost(),
                        "--spring.data.redis.port=" + REDIS.getMappedPort(6379),
                        "--rate-limit.enabled=true",
                        "--rate-limit.endpoints[0].id=get-account",
                        "--rate-limit.endpoints[0].method=GET",
                        "--rate-limit.endpoints[0].pattern=/v1/account/*",
                        "--rate-limit.endpoints[0].limit-per-minute=3",
                        "--outbox.publisher.enabled=false");
    }

    private record Reply(int status, String retryAfter) {
    }

    private static Reply getAccount(int port) throws Exception {
        HttpRequest request = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/v1/account/" + ACCOUNT_ID))
                .header("X-API-Key", "test-client-key")
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
        return new Reply(response.statusCode(), response.headers().firstValue("Retry-After").orElse(null));
    }

    @Test
    void limitIsGlobalAcrossInstances() throws Exception {
        JdbcTemplate jdbc = instanceA.getBean(JdbcTemplate.class);
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (" + ACCOUNT_ID + ", now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (%d, %d, 'ACTIVE', now(), now(), now(), 0, 100.00, 'RUB')
                """.formatted(ACCOUNT_ID, ACCOUNT_ID));
        StringRedisTemplate redis = instanceA.getBean(StringRedisTemplate.class);
        redis.execute((RedisCallback<Void>) c -> {
            c.serverCommands().flushAll();
            return null;
        });
        long intoWindow = System.currentTimeMillis() % 60000;
        if (intoWindow > 55000) {
            Thread.sleep(60000 - intoWindow + 200);
        }
        for (int i = 0; i < 3; i++) {
            assertThat(getAccount(PORT_A).status())
                    .as("instance A, request %d", i + 1)
                    .isEqualTo(200);
        }
        Reply fromB = getAccount(PORT_B);
        assertThat(fromB.status()).as("instance B first call").isEqualTo(429);
        assertThat(Integer.parseInt(fromB.retryAfter())).isGreaterThanOrEqualTo(1);
        assertThat(getAccount(PORT_B).status()).as("instance B second call").isEqualTo(429);
        assertThat(getAccount(PORT_A).status()).as("instance A after exhaustion").isEqualTo(429);
    }
}
