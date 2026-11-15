package nip.edu.jobprep.config;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.kafka.autoconfigure.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.MicrometerProducerListener;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;


@Configuration
public class KafkaConfig {


    @Bean
    public CommonErrorHandler cacheInvalidationErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(1000L, Long.MAX_VALUE));
    }

    /**
     * Attaches Micrometer's Kafka client metrics to the producer. Without this
     * listener the producer exposes no kafka_producer_* series at all (the
     * consumer gets them through its own listener), so the dashboard's
     * "Kafka producer" panel stays empty forever.
     */
    @Bean
    public DefaultKafkaProducerFactoryCustomizer micrometerProducerMetrics(MeterRegistry meters) {
        return producerFactory -> producerFactory.addListener(new MicrometerProducerListener<>(meters));
    }
}
