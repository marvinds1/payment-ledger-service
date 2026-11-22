package io.github.marvinds1.ledger.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;

class RedisRateLimiter {

    static final String KEY_PREFIX = "ledger:ratelimit:";

    private static final Logger log = LoggerFactory.getLogger(RedisRateLimiter.class);
    private static final long WARN_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);

    @SuppressWarnings("rawtypes")
    private static final RedisScript<List> TOKEN_BUCKET =
            RedisScript.of(new ClassPathResource("ratelimit/token_bucket.lua"), List.class);

    private final RateLimitProperties properties;
    private final StringRedisTemplate redis;
    private final Counter allowed;
    private final Counter limited;
    private final Counter unavailable;
    private final AtomicLong lastWarnNanos = new AtomicLong(System.nanoTime() - WARN_INTERVAL_NANOS);

    RedisRateLimiter(RateLimitProperties properties, StringRedisTemplate redis, MeterRegistry registry) {
        this.properties = properties;
        this.redis = redis;
        this.allowed = decisions(registry, "allowed");
        this.limited = decisions(registry, "limited");
        this.unavailable = decisions(registry, "unavailable");
    }

    RateLimitDecision tryAcquire(String clientKey) {
        List<?> result;
        try {
            result = redis.execute(TOKEN_BUCKET, List.of(KEY_PREFIX + clientKey),
                    String.valueOf(properties.capacity()),
                    String.valueOf(properties.refillPerMillisecond()),
                    String.valueOf(properties.bucketTtlMillis()));
        } catch (DataAccessException e) {
            return failOpen(e);
        }
        if (result == null || result.size() != 3) {
            return failOpen(new IllegalStateException("Unexpected rate limit script result: " + result));
        }
        RateLimitDecision decision = new RateLimitDecision(
                toLong(result.get(0)) == 1, toLong(result.get(1)), toLong(result.get(2)), true);
        (decision.allowed() ? allowed : limited).increment();
        return decision;
    }

    private RateLimitDecision failOpen(Exception e) {
        unavailable.increment();
        long now = System.nanoTime();
        long last = lastWarnNanos.get();
        if (now - last >= WARN_INTERVAL_NANOS && lastWarnNanos.compareAndSet(last, now)) {
            log.warn("Rate limiter unavailable, allowing requests (fail-open): {}", e.getMessage());
        }
        return RateLimitDecision.unavailable();
    }

    private static long toLong(Object value) {
        return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
    }

    private static Counter decisions(MeterRegistry registry, String result) {
        return Counter.builder("ledger.ratelimit.decisions")
                .description("Rate limit decisions for write requests")
                .tag("result", result)
                .register(registry);
    }
}
