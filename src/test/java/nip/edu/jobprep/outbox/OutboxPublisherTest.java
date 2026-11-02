package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OutboxPublisherTest {
    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void incrementsAttemptAndLeavesRowPendingWhenKafkaIsUnavailable() {
        OutboxRow row = new OutboxRow(1L, "event-1", DomainEvent.TRANSFER_COMPLETED, 10L, 7L,
                "{\"amount\":\"10.00\"}", Instant.parse("2026-11-01T12:00:00Z"));
        when(outboxRepository.claimBatch(anyInt(), anyString(), any())).thenReturn(List.of(row));
        when(outboxRepository.renewLease(eq(1L), eq(row.createdAt()), anyString(), any())).thenReturn(true);
        when(outboxRepository.hasOlderActiveForUser(7L, 1L)).thenReturn(false);
        when(kafkaTemplate.send(eq(OutboxPublisher.TOPIC), eq("7"), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("Kafka unavailable")));
        when(outboxRepository.recordFailure(eq(1L), eq(row.createdAt()), anyString(), eq(5), anyString(), any())).thenReturn(1);

        new OutboxPublisher(outboxRepository, kafkaTemplate, new ObjectMapper(), new OutboxMetrics(new SimpleMeterRegistry())).poll();

        verify(outboxRepository).recordFailure(eq(1L), eq(row.createdAt()), anyString(), eq(5), anyString(), any());
        verify(outboxRepository, never()).markAsCompleted(anyLong(), any(), anyString());
    }
    @Test
    void publishesLaterEventWhenOlderEventIsRejected() {
        OutboxRow row = new OutboxRow(2L, "event-2", DomainEvent.ACCOUNT_FROZEN, 11L, 7L,
                "{\"status\":\"FROZEN\"}", Instant.parse("2026-11-01T12:01:00Z"));
        when(outboxRepository.claimBatch(anyInt(), anyString(), any())).thenReturn(List.of(row));
        when(outboxRepository.renewLease(anyLong(), any(), anyString(), any())).thenReturn(true);
        when(outboxRepository.hasOlderActiveForUser(7L, 2L)).thenReturn(false);
        when(kafkaTemplate.send(eq(OutboxPublisher.TOPIC), eq("7"), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));

        new OutboxPublisher(outboxRepository, kafkaTemplate, new ObjectMapper(), new OutboxMetrics(new SimpleMeterRegistry())).poll();

        verify(outboxRepository).markAsCompleted(eq(2L), eq(row.createdAt()), anyString());
        verify(outboxRepository, never()).recordFailure(anyLong(), any(), anyString(), anyInt(), anyString(), any());
    }

    @Test
    void doesNotPublishLaterEventWhileOlderEventIsActive() {
        OutboxRow row = new OutboxRow(2L, "event-2", DomainEvent.ACCOUNT_FROZEN, 11L, 7L,
                "{\"status\":\"FROZEN\"}", Instant.parse("2026-11-01T12:01:00Z"));
        when(outboxRepository.claimBatch(anyInt(), anyString(), any())).thenReturn(List.of(row));
        when(outboxRepository.renewLease(anyLong(), any(), anyString(), any())).thenReturn(true);
        when(outboxRepository.hasOlderActiveForUser(7L, 2L)).thenReturn(true);

        new OutboxPublisher(outboxRepository, kafkaTemplate, new ObjectMapper(), new OutboxMetrics(new SimpleMeterRegistry())).poll();

        verifyNoInteractions(kafkaTemplate);
        verify(outboxRepository, never()).markAsCompleted(anyLong(), any(), anyString());
    }

}
