package io.github.marvinds1.ledger.ratelimit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT, properties = {
        "ledger.rate-limit.capacity=3",
        "ledger.rate-limit.refill-per-second=0.01"
})
@Testcontainers
class RateLimitIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Container
    @ServiceConnection(name = "redis")
    static GenericContainer<?> redis = new GenericContainer<>("redis:7.4-alpine").withExposedPorts(6379);

    @Autowired
    TestRestTemplate rest;

    @Autowired
    StringRedisTemplate redisTemplate;

    @BeforeEach
    void clearBuckets() {
        var keys = redisTemplate.keys(RedisRateLimiter.KEY_PREFIX + "*");
        if (keys != null && !keys.isEmpty()) {
            redisTemplate.delete(keys);
        }
    }

    @Test
    void writesBeyondCapacityAreRejectedWith429AndRetryAfter() {
        for (int remaining = 2; remaining >= 0; remaining--) {
            ResponseEntity<JsonNode> ok = createAccount("client-a");
            assertThat(ok.getStatusCode()).isEqualTo(HttpStatus.CREATED);
            assertThat(ok.getHeaders().getFirst("RateLimit-Limit")).isEqualTo("3");
            assertThat(ok.getHeaders().getFirst("RateLimit-Remaining")).isEqualTo(String.valueOf(remaining));
        }

        ResponseEntity<JsonNode> limited = createAccount("client-a");

        assertThat(limited.getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(limited.getBody().get("code").asText()).isEqualTo("RATE_LIMITED");
        assertThat(Long.parseLong(limited.getHeaders().getFirst(HttpHeaders.RETRY_AFTER))).isPositive();
        assertThat(limited.getHeaders().get("RateLimit-Remaining")).containsExactly("0");
        assertThat(limited.getHeaders().get("RateLimit-Limit")).containsExactly("3");
        assertThat(limited.getHeaders().get(HttpHeaders.RETRY_AFTER)).hasSize(1);
    }

    @Test
    void eachClientHasItsOwnBucket() {
        for (int i = 0; i < 3; i++) {
            createAccount("client-a");
        }
        assertThat(createAccount("client-a").getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(createAccount("client-b").getStatusCode()).isEqualTo(HttpStatus.CREATED);
    }

    @Test
    void readsAreNotRateLimited() {
        UUID id = UUID.fromString(createAccount("client-a").getBody().get("id").asText());
        createAccount("client-a");
        createAccount("client-a");
        assertThat(createAccount("client-a").getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);

        for (int i = 0; i < 5; i++) {
            ResponseEntity<JsonNode> read = rest.getForEntity("/api/v1/accounts/{id}", JsonNode.class, id);
            assertThat(read.getStatusCode()).isEqualTo(HttpStatus.OK);
        }
    }

    @Test
    void requestsWithoutClientIdAreLimitedByRemoteAddress() {
        for (int i = 0; i < 3; i++) {
            assertThat(createAccount(null).getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
        assertThat(createAccount(null).getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        assertThat(createAccount("not valid!").getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void tokensRefillOverTime() {
        RedisRateLimiter limiter = new RedisRateLimiter(
                new RateLimitProperties(true, 1, 5, "X-Client-Id"), redisTemplate, new SimpleMeterRegistry());

        assertThat(limiter.tryAcquire("refill").allowed()).isTrue();
        RateLimitDecision denied = limiter.tryAcquire("refill");
        assertThat(denied.allowed()).isFalse();
        assertThat(denied.retryAfterMillis()).isBetween(1L, 200L);

        await().atMost(Duration.ofSeconds(2)).pollInterval(Duration.ofMillis(50))
                .until(() -> limiter.tryAcquire("refill").allowed());
        Long ttl = redisTemplate.getExpire(RedisRateLimiter.KEY_PREFIX + "refill");
        assertThat(ttl).isPositive();
    }

    private ResponseEntity<JsonNode> createAccount(String clientId) {
        HttpHeaders headers = new HttpHeaders();
        if (clientId != null) {
            headers.set("X-Client-Id", clientId);
        }
        return rest.exchange("/api/v1/accounts", HttpMethod.POST,
                new HttpEntity<>(Map.of("ownerName", "Rate Limited", "currency", "IDR"), headers), JsonNode.class);
    }
}
