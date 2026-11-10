package nip.edu.jobprep.cache;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
public class CacheKafkaConfig {

    @Bean
    public CommonErrorHandler cacheInvalidationErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, Long.MAX_VALUE));
    }
}