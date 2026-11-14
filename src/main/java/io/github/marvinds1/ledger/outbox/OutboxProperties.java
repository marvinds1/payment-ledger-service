package io.github.marvinds1.ledger.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("ledger.outbox")
public record OutboxProperties(String topic, int batchSize) {
}
