package nip.edu.jobprep.outbox;

public record AccountCacheInvalidationRequested(
        long accountId, long newVersion
) {
}