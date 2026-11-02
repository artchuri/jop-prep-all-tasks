package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

class OutboxPublisherIT extends AbstractIntegrationTest {

    @Autowired
    OutboxRepository outboxRepository;

    static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1"));

    @Test
    void deliversCommittedEventAfterRestart() {
        KAFKA.start();

        outboxRepository.save(new DomainEvent(UUID.randomUUID().toString(), DomainEvent.TRANSFER_COMPLETED, 1L, Instant.now(), null),
                10L, "{\"amount\":\"100.00\",\"currency\":\"RUB\"}", Instant.now());

        OutboxPublisher publisher = new OutboxPublisher(outboxRepository, TestKafka.template(KAFKA.getBootstrapServers()), new ObjectMapper(),
                new OutboxMetrics(new SimpleMeterRegistry()));
        publisher.poll();

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.ofEntries(
                Map.entry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers()),
                Map.entry(ConsumerConfig.GROUP_ID_CONFIG, "it-" + UUID.randomUUID()),
                Map.entry(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"),
                Map.entry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
                Map.entry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())))) {
            consumer.subscribe(List.of(OutboxPublisher.TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            assertThat(records.count()).isEqualTo(1);
            ConsumerRecord<String, String> record = records.iterator().next();
            assertThat(record.key()).isEqualTo("1");
            assertThat(record.value()).contains("TRANSFER_COMPLETED");
        }
    }
}
