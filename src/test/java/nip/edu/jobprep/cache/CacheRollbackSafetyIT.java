package nip.edu.jobprep.cache;

import nip.edu.jobprep.exceptions.InsufficientFundsException;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.data.redis.core.RedisCallback;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import nip.edu.jobprep.service.AccountService;
import nip.edu.jobprep.service.TransferService;

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
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;


@Import(CacheRollbackSafetyIT.KafkaConfig.class)
@TestPropertySource(properties = "spring.datasource.hikari.maximum-pool-size=10")
class CacheRollbackSafetyIT extends AbstractCacheIntegrationTest {

    @Autowired
    TransferService transferService;

    @Autowired
    CacheManager cacheManager;

    Cache cache;

    @BeforeEach
    void initCache() {
        cache = cacheManager.getCache(LuaCache.NAME);
    }

    @Autowired
    AccountService accountService;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JdbcTemplate jdbc;

    static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @TestConfiguration
    static class KafkaConfig {
        @Bean
        KafkaTemplate<String, String> testKafkaTemplate() {
            KAFKA.start();
            Map<String, Object> props = Map.ofEntries(
                    Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                    Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                    Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()));
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
        redis.execute((RedisCallback<Void>) conn -> { conn.serverCommands().flushAll(); return null; });
    }

    @Test
    void rollbackLeavesCacheUntouched() {
        // Warm cache from both accounts
        accountService.get(1);
        accountService.get(2);
        assertThat(field(1, "state")).isEqualTo("F");
        assertThat(field(2, "state")).isEqualTo("F");

        // Overspend triggers InsufficientFundsException -> rollback (nothing changed)
        assertThatThrownBy(() -> transferService.transfer(
                UUID.randomUUID().toString(),
                new TransferRequestDto(2L, 1L, 1L, new BigDecimal("500"))))
                .isInstanceOf(InsufficientFundsException.class);

        // PG unchanged
        assertThat(jdbc.queryForObject("select amount from accounts where id = 1", BigDecimal.class))
                .isEqualByComparingTo("100.00");
        assertThat(jdbc.queryForObject("select count(*) from outbox", Long.class)).isZero();
        // Cache still FRESH with pre-rollback values and no invalidation happened
        assertThat(field(1, "state")).isEqualTo("F");
        assertThat(field(1, "ver")).isEqualTo("0");
        assertThat(field(2, "state")).isEqualTo("F");
    }

    private Object field(long accountId, String name) {
        return redis.opsForHash().get(AccountCacheScripts.key(accountId), name);
    }
}