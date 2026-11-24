package io.github.marvinds1.ledger.outbox;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
@ConditionalOnProperty(name = "ledger.outbox.relay-enabled", havingValue = "true", matchIfMissing = true)
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final OutboxProperties properties;
    private final Counter published;
    private final Counter failures;

    public OutboxRelay(OutboxRepository outbox, KafkaTemplate<String, String> kafka, OutboxProperties properties,
                       MeterRegistry registry) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.properties = properties;
        this.published = Counter.builder("ledger.outbox.published.events")
                .description("Outbox events acknowledged by Kafka and marked as published")
                .register(registry);
        this.failures = Counter.builder("ledger.outbox.publish.failures")
                .description("Outbox batches that failed to publish and will be retried")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.poll-interval-ms:500}")
    @Transactional
    public void publishPending() {
        List<OutboxEvent> events = outbox.lockUnpublished(properties.batchSize());
        if (events.isEmpty()) {
            return;
        }
        List<CompletableFuture<SendResult<String, String>>> acks = events.stream()
                .map(this::send)
                .toList();
        try {
            CompletableFuture.allOf(acks.toArray(CompletableFuture[]::new)).get(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            failures.increment();
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while publishing outbox batch", e);
        } catch (ExecutionException | TimeoutException e) {
            failures.increment();
            throw new IllegalStateException("Failed to publish outbox batch starting at id " + events.get(0).id(), e);
        }
        outbox.markPublished(events.stream().map(OutboxEvent::id).toList());
        published.increment(events.size());
        log.debug("Published {} outbox events", events.size());
    }

    private CompletableFuture<SendResult<String, String>> send(OutboxEvent event) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(properties.topic(), event.aggregateId().toString(), event.payload());
        record.headers().add("eventType", event.eventType().getBytes(StandardCharsets.UTF_8));
        return kafka.send(record);
    }
}
