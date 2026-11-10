package nip.edu.jobprep.cache;

public record AccountCacheInvalidationRequested(
        long accountId, long newVersion
) {
}