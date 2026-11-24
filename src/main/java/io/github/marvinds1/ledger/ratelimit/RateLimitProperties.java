package io.github.marvinds1.ledger.ratelimit;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Positive;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties("ledger.rate-limit")
public record RateLimitProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("50") @Positive int capacity,
        @DefaultValue("20") @Positive double refillPerSecond,
        @DefaultValue("X-Client-Id") @NotBlank String clientIdHeader) {

    double refillPerMillisecond() {
        return refillPerSecond / 1000.0;
    }

    long bucketTtlMillis() {
        return (long) Math.ceil(capacity / refillPerMillisecond()) + 1_000;
    }
}
