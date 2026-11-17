package nip.edu.jobprep.cache;

import org.junit.jupiter.api.Tag;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;


@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = AbstractCacheIntegrationTest.Initializer.class)
public abstract class AbstractCacheIntegrationTest extends AbstractIntegrationTest {

    public static class Initializer extends AbstractIntegrationTest.Initializer {
        @Override
        public void initialize(ConfigurableApplicationContext context) {
            super.initialize(context);
            String[] hostPort = TestRedis.proxyAddress().split(":");
            TestPropertyValues.of(
                    "spring.data.redis.host=" + hostPort[0],
                    "spring.data.redis.port=" + hostPort[1]
            ).applyTo(context.getEnvironment());
        }
    }
}