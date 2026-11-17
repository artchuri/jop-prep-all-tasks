package nip.edu.jobprep.cache;

import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.service.AccountService;
import nip.edu.jobprep.utils.cache.AccountCacheScripts;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Redis must stay an optimisation on the GET path: when it is unreachable,
 * reads degrade to PostgreSQL (controlled PG fallback, no cache write)
 * instead of failing - on a cold miss and on a warm entry alike - and the
 * cache resumes serving once Redis returns. Regression for the claim that a
 * Redis error could break a GET that the DB can serve.
 */
@Tag("integration")
class RedisOutageGetIT extends AbstractCacheIntegrationTest {

    @Autowired
    CacheManager cacheManager;

    @Autowired
    AccountService accountService;

    @Autowired
    AccountCacheScripts scripts;

    @Autowired
    StringRedisTemplate redis;

    @Autowired
    JdbcTemplate jdbc;

    Cache cache;

    @BeforeEach
    void init() {
        cache = cacheManager.getCache(LuaCache.NAME);
        jdbc.update("delete from account_status_history");
        jdbc.update("delete from accounts");
        jdbc.update("delete from users");
        jdbc.update("insert into users (id, created_at, version) values (7, now(), 0)");
        jdbc.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (7, 7, 'ACTIVE', now(), now(), now(), 0, 250.00, 'RUB')
                """);
        redis.execute((org.springframework.data.redis.core.RedisCallback<Void>) conn -> {
            conn.serverCommands().flushAll();
            return null;
        });
    }

    @AfterEach
    void restore() {
        TestRedis.REDIS_PROXY.setConnectionCut(false);
    }

    @Test
    void coldMissWithRedisDownServesFromPostgreSql() {
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        AccountResponseDto dto = accountService.get(7);
        assertThat(dto.balance()).isEqualByComparingTo("250.00");
        assertThat(dto.status()).isEqualTo("ACTIVE");
    }

    @Test
    void warmEntryWithRedisDownStillServesAfterFill() {
        assertThat(accountService.get(7).balance()).isEqualByComparingTo("250.00");
        assertThat(field("state")).isEqualTo("F");

        TestRedis.REDIS_PROXY.setConnectionCut(true);
        assertThat(accountService.get(7).balance()).isEqualByComparingTo("250.00");
    }

    @Test
    void redisComingBackServesFromCacheAgain() {
        TestRedis.REDIS_PROXY.setConnectionCut(true);
        assertThat(accountService.get(7).balance()).isEqualByComparingTo("250.00");
        TestRedis.REDIS_PROXY.setConnectionCut(false);

        assertThat(accountService.get(7).balance()).isEqualByComparingTo("250.00");
        assertThat(field("state")).isEqualTo("F");
    }

    private Object field(String name) {
        return redis.opsForHash().get(AccountCacheScripts.key(7), name);
    }
}
