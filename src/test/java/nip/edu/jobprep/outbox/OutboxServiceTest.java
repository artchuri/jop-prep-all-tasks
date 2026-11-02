package nip.edu.jobprep.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxServiceTest {
    @Mock
    private OutboxRepository outboxRepository;

    @Mock
    private ObjectMapper objectMapper;

    @Test
    void reusesCallerTimestampInEnvelopeAndRepositoryRow() throws Exception {
        Instant occurredAt = Instant.parse("2026-11-01T12:00:00Z");
        when(objectMapper.writeValueAsString(any())).thenReturn("{\"amount\":\"10.00\"}");
        OutboxService service = new OutboxService(outboxRepository, objectMapper,
                new OutboxMetrics(new SimpleMeterRegistry()));

        service.publish(DomainEvent.TRANSFER_COMPLETED, 7L, 42L, new Object(), occurredAt);

        ArgumentCaptor<DomainEvent> eventCaptor = ArgumentCaptor.forClass(DomainEvent.class);
        ArgumentCaptor<Instant> timestampCaptor = ArgumentCaptor.forClass(Instant.class);
        verify(outboxRepository).save(eventCaptor.capture(), eq(42L), eq("{\"amount\":\"10.00\"}"), timestampCaptor.capture());
        assertThat(eventCaptor.getValue().occurredAt()).isEqualTo(occurredAt);
        assertThat(timestampCaptor.getValue()).isEqualTo(occurredAt);
    }
}
