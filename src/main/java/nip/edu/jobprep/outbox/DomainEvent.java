package nip.edu.jobprep.outbox;


public record DomainEvent(
        String eventId,
        String eventType,
        long userId,
        java.time.Instant occurredAt,
        Object payload
) {
    public static final String TRANSFER_COMPLETED = "TRANSFER_COMPLETED";
    public static final String ACCOUNT_FROZEN = "ACCOUNT_FROZEN";
}
