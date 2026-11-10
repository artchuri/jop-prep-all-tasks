package nip.edu.jobprep.cache;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.service.AccountCacheService;
import nip.edu.jobprep.service.AccountQueryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.TestPropertySource;

import java.math.BigDecimal;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Rollback AC: a failed transfer must not touch the cache and must not emit
 * invalidation for uncommitted state. PG remains unchanged, cache stays FRESH v0.
 */
@Import(CacheRollbackSafetyIT.KafkaConfig.class)
@TestPropertySource(properties = "spring.datasource.hikari.maximum-pool-size=10")
class CacheRollbackSafetyIT extends AbstractCacheIntegrationTest {

    @Autowired
    nip.edu.jobprep.service.TransferService transferService;

    @Autowired
    AccountCacheService cache;

    @Autowired
    AccountQueryService queryService;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JdbcTemplate jdbc;

    static final org.testcontainers.containers.KafkaContainer KAFKA =
            new org.testcontainers.containers.KafkaContainer(org.testcontainers.utility.DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @TestConfiguration
    static class KafkaConfig {
        @Bean
        KafkaTemplate<String, String> testKafkaTemplate() {
            KAFKA.start();
            Map<String, Object> props = Map.ofEntries(
                    Map.entry(org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                    Map.entry(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName()),
                    Map.entry(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class.getName()));
            return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
        }
    }

    @BeforeEach
    void setup() {
        jdbc.update("delete from outbox");
        jdbc.update("delete from transfers_history");
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (1, now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (1, 1, 'ACTIVE', now(), now(), now(), 0, 100.00, 'RUB'),
                       (2, 1, 'ACTIVE', now(), now(), now(), 0, 0.00, 'RUB')
                """);
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
    }

    @Test
    void rollbackLeavesCacheUntouched() {
        // Warm cache from both accounts
        queryService.get(1);
        queryService.get(2);
        assertThat(cache.field(1, "state")).isEqualTo("F");
        assertThat(cache.field(2, "state")).isEqualTo("F");

        // Overspend triggers InsufficientFundsException -> rollback (nothing changed)
        assertThatThrownBy(() -> transferService.transfer(
                java.util.UUID.randomUUID().toString(),
                new TransferRequestDto(2L, 1L, 1L, new BigDecimal("500"))))
                .isInstanceOf(nip.edu.jobprep.exceptions.InsufficientFundsException.class);

        // PG unchanged
        assertThat(jdbc.queryForObject("select amount from accounts where id = 1", BigDecimal.class))
                .isEqualByComparingTo("100.00");
        assertThat(jdbc.queryForObject("select count(*) from outbox", Long.class)).isZero();
        // Cache still FRESH with pre-rollback values and no invalidation happened
        assertThat(cache.field(1, "state")).isEqualTo("F");
        assertThat(cache.field(1, "ver")).isEqualTo("0");
        assertThat(cache.field(2, "state")).isEqualTo("F");
    }
}