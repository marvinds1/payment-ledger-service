package io.github.marvinds1.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = "ledger.rate-limit.enabled=false")
@AutoConfigureObservability
@Testcontainers
class LedgerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer("apache/kafka:3.8.0");

    @Autowired
    TestRestTemplate rest;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    MeterRegistry meters;

    @Test
    void transferMovesMoneyAndWritesBalancedEntries() {
        UUID alice = fundedAccount("Alice", 100_000);
        UUID bob = fundedAccount("Bob", 0);

        ResponseEntity<JsonNode> response = transfer(newKey(), alice, bob, 30_000);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(response.getBody().get("status").asText()).isEqualTo("COMPLETED");
        assertThat(balance(alice)).isEqualTo(70_000);
        assertThat(balance(bob)).isEqualTo(30_000);

        JsonNode bobEntries = rest.getForObject("/api/v1/accounts/{id}/entries", JsonNode.class, bob);
        assertThat(bobEntries).hasSize(1);
        assertThat(bobEntries.get(0).get("direction").asText()).isEqualTo("CREDIT");
        assertThat(bobEntries.get(0).get("balanceAfter").asLong()).isEqualTo(30_000);
    }

    @Test
    void retryWithSameKeyReplaysOriginalResultWithoutMovingMoneyTwice() {
        UUID alice = fundedAccount("Alice", 50_000);
        UUID bob = fundedAccount("Bob", 0);
        String key = newKey();

        ResponseEntity<JsonNode> first = transfer(key, alice, bob, 10_000);
        ResponseEntity<JsonNode> retry = transfer(key, alice, bob, 10_000);

        assertThat(first.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        assertThat(retry.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(retry.getHeaders().getFirst("Idempotent-Replayed")).isEqualTo("true");
        assertThat(retry.getBody().get("id")).isEqualTo(first.getBody().get("id"));
        assertThat(balance(alice)).isEqualTo(40_000);
    }

    @Test
    void reusingKeyWithDifferentPayloadIsRejected() {
        UUID alice = fundedAccount("Alice", 50_000);
        UUID bob = fundedAccount("Bob", 0);
        String key = newKey();

        transfer(key, alice, bob, 10_000);
        ResponseEntity<JsonNode> conflict = transfer(key, alice, bob, 20_000);

        assertThat(conflict.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(conflict.getBody().get("code").asText()).isEqualTo("IDEMPOTENCY_KEY_REUSED");
        assertThat(balance(alice)).isEqualTo(40_000);
    }

    @Test
    void insufficientFundsIsRecordedAsRejectedAndLeavesBalancesUntouched() {
        UUID alice = fundedAccount("Alice", 5_000);
        UUID bob = fundedAccount("Bob", 0);

        ResponseEntity<JsonNode> response = transfer(newKey(), alice, bob, 10_000);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);
        assertThat(response.getBody().get("status").asText()).isEqualTo("REJECTED");
        assertThat(response.getBody().get("failureReason").asText()).isEqualTo("INSUFFICIENT_FUNDS");
        assertThat(balance(alice)).isEqualTo(5_000);
        assertThat(balance(bob)).isZero();
    }

    @Test
    void missingIdempotencyKeyIsBadRequest() {
        UUID alice = fundedAccount("Alice", 5_000);
        UUID bob = fundedAccount("Bob", 0);

        ResponseEntity<JsonNode> response = rest.exchange("/api/v1/transfers", HttpMethod.POST,
                new HttpEntity<>(body(alice, bob, 1_000)), JsonNode.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST);
    }

    @Test
    void concurrentTransfersNeverOverdrawAnAccount() throws Exception {
        UUID alice = fundedAccount("Alice", 1_000);
        UUID bob = fundedAccount("Bob", 0);

        List<ResponseEntity<JsonNode>> results = runConcurrently(30, i -> transfer(newKey(), alice, bob, 100));

        long completed = results.stream().filter(r -> r.getStatusCode() == HttpStatus.CREATED).count();
        assertThat(completed).isEqualTo(10);
        assertThat(balance(alice)).isZero();
        assertThat(balance(bob)).isEqualTo(1_000);
    }

    @Test
    void opposingConcurrentTransfersDoNotDeadlockAndConserveMoney() throws Exception {
        UUID alice = fundedAccount("Alice", 10_000);
        UUID bob = fundedAccount("Bob", 10_000);

        List<ResponseEntity<JsonNode>> results = runConcurrently(40, i -> i % 2 == 0
                ? transfer(newKey(), alice, bob, 150)
                : transfer(newKey(), bob, alice, 100));

        assertThat(results).allSatisfy(r -> assertThat(r.getStatusCode()).isEqualTo(HttpStatus.CREATED));
        assertThat(balance(alice)).isEqualTo(10_000 - 20 * 150 + 20 * 100);
        assertThat(balance(alice) + balance(bob)).isEqualTo(20_000);
    }

    @Test
    void concurrentRequestsWithSameKeyCreateExactlyOneTransfer() throws Exception {
        UUID alice = fundedAccount("Alice", 10_000);
        UUID bob = fundedAccount("Bob", 0);
        String key = newKey();

        List<ResponseEntity<JsonNode>> results = runConcurrently(10, i -> transfer(key, alice, bob, 1_000));

        assertThat(results).extracting(r -> r.getBody().get("id").asText()).containsOnly(
                results.get(0).getBody().get("id").asText());
        assertThat(results).filteredOn(r -> r.getStatusCode() == HttpStatus.CREATED).hasSize(1);
        assertThat(balance(alice)).isEqualTo(9_000);
    }

    @Test
    void ledgerStaysBalancedAcrossAllAccounts() {
        UUID alice = fundedAccount("Alice", 7_500);
        UUID bob = fundedAccount("Bob", 0);
        transfer(newKey(), alice, bob, 2_500);

        long totalBalance = jdbc.sql("SELECT COALESCE(SUM(balance), 0) FROM accounts").query(Long.class).single();
        long mismatchedAccounts = jdbc.sql("""
                SELECT COUNT(*) FROM accounts a
                WHERE a.balance <> (
                    SELECT COALESCE(SUM(CASE WHEN e.direction = 'CREDIT' THEN e.amount ELSE -e.amount END), 0)
                    FROM ledger_entries e WHERE e.account_id = a.id)
                """).query(Long.class).single();

        assertThat(totalBalance).isZero();
        assertThat(mismatchedAccounts).isZero();
    }

    @Test
    void completedTransferIsPublishedToKafkaThroughTheOutbox() {
        UUID alice = fundedAccount("Alice", 5_000);
        UUID bob = fundedAccount("Bob", 0);
        String transferId = transfer(newKey(), alice, bob, 1_234).getBody().get("id").asText();

        await().atMost(Duration.ofSeconds(30)).until(() -> jdbc.sql(
                        "SELECT COUNT(*) FROM outbox_events WHERE aggregate_id = :id AND published_at IS NULL")
                .param("id", UUID.fromString(transferId))
                .query(Long.class)
                .single() == 0);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            List<TopicPartition> partitions = consumer.partitionsFor("ledger.transfer-events").stream()
                    .map(p -> new TopicPartition(p.topic(), p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            List<ConsumerRecord<String, String>> received = new ArrayList<>();
            await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
                consumer.poll(Duration.ofMillis(500)).forEach(received::add);
                assertThat(received).anySatisfy(record -> {
                    assertThat(record.key()).isEqualTo(transferId);
                    assertThat(record.value()).containsPattern("\"amount\":\\s*1234").contains("COMPLETED");
                });
            });
        }
    }

    @Test
    void transferOutcomesAreRecordedAsMetricsAndExposedToPrometheus() {
        UUID alice = fundedAccount("Alice", 10_000);
        UUID bob = fundedAccount("Bob", 0);
        double completed = transferCount("completed", "none");
        double replayed = transferCount("replayed", "none");
        double rejected = transferCount("rejected", "INSUFFICIENT_FUNDS");
        double failed = transferCount("failed", "SAME_ACCOUNT");
        double volume = transferVolume();

        String key = newKey();
        transfer(key, alice, bob, 4_000);
        transfer(key, alice, bob, 4_000);
        transfer(newKey(), alice, bob, 50_000);
        assertThat(transfer(newKey(), alice, alice, 1_000).getStatusCode()).isEqualTo(HttpStatus.UNPROCESSABLE_ENTITY);

        assertThat(transferCount("completed", "none")).isEqualTo(completed + 1);
        assertThat(transferCount("replayed", "none")).isEqualTo(replayed + 1);
        assertThat(transferCount("rejected", "INSUFFICIENT_FUNDS")).isEqualTo(rejected + 1);
        assertThat(transferCount("failed", "SAME_ACCOUNT")).isEqualTo(failed + 1);
        assertThat(transferVolume()).isEqualTo(volume + 4_000);

        ResponseEntity<String> scrape = rest.getForEntity("/actuator/prometheus", String.class);
        assertThat(scrape.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(scrape.getBody()).contains(
                "ledger_transfers_total{",
                "ledger_transfer_duration_seconds_bucket{",
                "ledger_transfer_volume_total{",
                "ledger_outbox_pending_events{",
                "ledger_outbox_oldest_pending_age_seconds{",
                "ledger_outbox_published_events_total{",
                "application=\"payment-ledger-service\"");
    }

    @Test
    void outboxBacklogMetricsDrainToZeroOncePublished() {
        UUID alice = fundedAccount("Alice", 1_000);
        UUID bob = fundedAccount("Bob", 0);
        double publishedBefore = meters.get("ledger.outbox.published.events").counter().count();
        transfer(newKey(), alice, bob, 500);

        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(meters.get("ledger.outbox.published.events").counter().count()).isGreaterThan(publishedBefore);
            assertThat(meters.get("ledger.outbox.pending.events").gauge().value()).isZero();
            assertThat(meters.get("ledger.outbox.oldest.pending.age").gauge().value()).isZero();
        });
    }

    private UUID fundedAccount(String owner, long initialBalance) {
        JsonNode account = rest.postForObject("/api/v1/accounts",
                Map.of("ownerName", owner, "currency", "IDR"), JsonNode.class);
        UUID id = UUID.fromString(account.get("id").asText());
        if (initialBalance > 0) {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Idempotency-Key", newKey());
            ResponseEntity<JsonNode> deposit = rest.exchange("/api/v1/accounts/{id}/deposits", HttpMethod.POST,
                    new HttpEntity<>(Map.of("amount", initialBalance), headers), JsonNode.class, id);
            assertThat(deposit.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        }
        return id;
    }

    private ResponseEntity<JsonNode> transfer(String key, UUID source, UUID target, long amount) {
        HttpHeaders headers = new HttpHeaders();
        headers.set("Idempotency-Key", key);
        return rest.exchange("/api/v1/transfers", HttpMethod.POST,
                new HttpEntity<>(body(source, target, amount), headers), JsonNode.class);
    }

    private static Map<String, Object> body(UUID source, UUID target, long amount) {
        return Map.of("sourceAccountId", source, "targetAccountId", target, "amount", amount, "currency", "IDR");
    }

    private double transferCount(String outcome, String reason) {
        Counter counter = meters.find("ledger.transfers")
                .tags("operation", "transfer", "outcome", outcome, "reason", reason)
                .counter();
        return counter == null ? 0 : counter.count();
    }

    private double transferVolume() {
        Counter counter = meters.find("ledger.transfer.volume")
                .tags("operation", "transfer", "currency", "IDR")
                .counter();
        return counter == null ? 0 : counter.count();
    }

    private long balance(UUID accountId) {
        return rest.getForObject("/api/v1/accounts/{id}", JsonNode.class, accountId).get("balance").asLong();
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private static <T> List<T> runConcurrently(int tasks, IndexedTask<T> task) throws Exception {
        ExecutorService pool = Executors.newFixedThreadPool(tasks);
        CountDownLatch start = new CountDownLatch(1);
        try {
            List<Future<T>> futures = new ArrayList<>();
            for (int i = 0; i < tasks; i++) {
                int index = i;
                Callable<T> callable = () -> {
                    start.await();
                    return task.run(index);
                };
                futures.add(pool.submit(callable));
            }
            start.countDown();
            List<T> results = new ArrayList<>();
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return results;
        } finally {
            pool.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface IndexedTask<T> {
        T run(int index);
    }
}
