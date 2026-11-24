package io.github.marvinds1.ledger.ratelimit;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.regex.Pattern;
import org.springframework.http.HttpMethod;
import org.springframework.web.servlet.HandlerInterceptor;

class RateLimitInterceptor implements HandlerInterceptor {

    static final String LIMIT_HEADER = RateLimitExceededException.LIMIT_HEADER;
    static final String REMAINING_HEADER = RateLimitExceededException.REMAINING_HEADER;

    private static final Pattern VALID_CLIENT_ID = Pattern.compile("[A-Za-z0-9._:-]{1,64}");

    private final RedisRateLimiter limiter;
    private final RateLimitProperties properties;

    RateLimitInterceptor(RedisRateLimiter limiter, RateLimitProperties properties) {
        this.limiter = limiter;
        this.properties = properties;
    }

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) {
        if (!HttpMethod.POST.matches(request.getMethod())) {
            return true;
        }
        RateLimitDecision decision = limiter.tryAcquire(clientKey(request));
        if (!decision.limiterAvailable()) {
            return true;
        }
        if (!decision.allowed()) {
            throw new RateLimitExceededException(properties.capacity(), decision.retryAfterSeconds());
        }
        response.setHeader(LIMIT_HEADER, String.valueOf(properties.capacity()));
        response.setHeader(REMAINING_HEADER, String.valueOf(decision.remaining()));
        return true;
    }

    String clientKey(HttpServletRequest request) {
        String clientId = request.getHeader(properties.clientIdHeader());
        if (clientId != null && VALID_CLIENT_ID.matcher(clientId).matches()) {
            return "client:" + clientId;
        }
        return "ip:" + request.getRemoteAddr();
    }
}
