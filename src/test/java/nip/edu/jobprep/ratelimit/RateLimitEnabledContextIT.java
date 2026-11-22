package nip.edu.jobprep.ratelimit;

import nip.edu.jobprep.cache.AbstractCacheIntegrationTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.MOCK, properties = {
        "rate-limit.enabled=true",
        "rate-limit.clients[0].name=it-client",
        "rate-limit.clients[0].api-key=it-secret-key"
})
@AutoConfigureMockMvc
@ContextConfiguration(initializers = RateLimitEnabledContextIT.Initializer.class)
class RateLimitEnabledContextIT extends AbstractCacheIntegrationTest {
    public static class Initializer extends AbstractCacheIntegrationTest.Initializer {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            super.initialize(context);
            TestPropertyValues.of("rate-limit.enabled=true").applyTo(context.getEnvironment());
        }
    }

    @Autowired MockMvc mvc;

    @Autowired StringRedisTemplate redis;

    @Test
    void contextBootsWithLimiterAndCache() {
        assertThat(redis).isNotNull();
    }

    @Test
    void autoconfiguredCacheTemplateStillWorks() {
        String key = "it:probe:" + System.currentTimeMillis();
        redis.opsForValue().set(key, "1");
        assertThat(redis.opsForValue().get(key)).isEqualTo("1");
    }

    @Test
    void httpRequestsPassThroughTheRealFilter() throws Exception {
        mvc.perform(get("/v1/account/12345").header("X-API-Key", "it-secret-key"))
                .andExpect(status().isNotFound());
        mvc.perform(get("/v1/account/12345"))
                .andExpect(status().isUnauthorized());
    }
}
