package nip.edu.jobprep.ratelimit;

public class RateLimitOutageException extends RuntimeException {
    public RateLimitOutageException(Throwable cause) {
        super("rate limit check failed", cause);
    }
}
