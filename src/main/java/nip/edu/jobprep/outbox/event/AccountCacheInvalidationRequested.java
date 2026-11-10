package nip.edu.jobprep.outbox.event;

public record AccountCacheInvalidationRequested(
        long accountId, long newVersion
) {
}