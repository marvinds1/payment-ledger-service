package io.github.marvinds1.ledger.outbox;

import java.util.UUID;

public record OutboxEvent(long id, String aggregateType, UUID aggregateId, String eventType, String payload) {
}
