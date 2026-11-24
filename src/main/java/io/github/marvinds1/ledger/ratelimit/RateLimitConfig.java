package io.github.marvinds1.ledger.ratelimit;

import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration(proxyBeanMethods = false)
@ConditionalOnProperty(name = "ledger.rate-limit.enabled", havingValue = "true", matchIfMissing = true)
class RateLimitConfig {

    @Bean
    RedisRateLimiter redisRateLimiter(RateLimitProperties properties, StringRedisTemplate redis,
                                      MeterRegistry registry) {
        return new RedisRateLimiter(properties, redis, registry);
    }

    @Bean
    RateLimitInterceptor rateLimitInterceptor(RedisRateLimiter limiter, RateLimitProperties properties) {
        return new RateLimitInterceptor(limiter, properties);
    }

    @Bean
    WebMvcConfigurer rateLimitWebConfigurer(RateLimitInterceptor interceptor) {
        return new WebMvcConfigurer() {
            @Override
            public void addInterceptors(InterceptorRegistry registry) {
                registry.addInterceptor(interceptor).addPathPatterns("/api/**");
            }
        };
    }
}
