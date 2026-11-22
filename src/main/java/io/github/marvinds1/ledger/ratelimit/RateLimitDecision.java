package io.github.marvinds1.ledger.ratelimit;

record RateLimitDecision(boolean allowed, long remaining, long retryAfterMillis, boolean limiterAvailable) {

    static RateLimitDecision unavailable() {
        return new RateLimitDecision(true, -1, 0, false);
    }

    long retryAfterSeconds() {
        return Math.max(1, (retryAfterMillis + 999) / 1000);
    }
}
