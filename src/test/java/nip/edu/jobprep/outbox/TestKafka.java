package nip.edu.jobprep.outbox;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.Map;

final class TestKafka {
    private TestKafka() {
    }

    static KafkaTemplate<String, String> template(String bootstrapServers) {
        Map<String, Object> props = Map.ofEntries(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers),
                Map.entry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
                Map.entry(ProducerConfig.ACKS_CONFIG, "all"),
                Map.entry(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true));
        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(props));
    }
}
