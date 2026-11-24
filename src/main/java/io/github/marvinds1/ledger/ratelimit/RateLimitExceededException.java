package io.github.marvinds1.ledger.ratelimit;

public class RateLimitExceededException extends RuntimeException {

    public static final String LIMIT_HEADER = "RateLimit-Limit";
    public static final String REMAINING_HEADER = "RateLimit-Remaining";

    private final int limit;
    private final long retryAfterSeconds;

    RateLimitExceededException(int limit, long retryAfterSeconds) {
        super("Too many requests. Retry after " + retryAfterSeconds + " second(s).");
        this.limit = limit;
        this.retryAfterSeconds = retryAfterSeconds;
    }

    public int limit() {
        return limit;
    }

    public long retryAfterSeconds() {
        return retryAfterSeconds;
    }
}
