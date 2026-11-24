package io.github.marvinds1.ledger.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
class OutboxMetrics {

    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private final OutboxRepository outbox;
    private volatile double pending = Double.NaN;
    private volatile double oldestAgeSeconds = Double.NaN;

    OutboxMetrics(OutboxRepository outbox, MeterRegistry registry) {
        this.outbox = outbox;
        Gauge.builder("ledger.outbox.pending.events", this, m -> m.pending)
                .description("Outbox events not yet published to Kafka")
                .register(registry);
        Gauge.builder("ledger.outbox.oldest.pending.age", this, m -> m.oldestAgeSeconds)
                .description("Age of the oldest unpublished outbox event (0 when nothing is pending)")
                .baseUnit("seconds")
                .register(registry);
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.metrics-refresh-ms:5000}")
    void refresh() {
        try {
            OutboxRepository.PendingStats stats = outbox.pendingStats();
            pending = stats.pending();
            oldestAgeSeconds = stats.oldestAgeSeconds();
        } catch (RuntimeException e) {
            pending = Double.NaN;
            oldestAgeSeconds = Double.NaN;
            log.warn("Could not refresh outbox metrics: {}", e.getMessage());
        }
    }
}
