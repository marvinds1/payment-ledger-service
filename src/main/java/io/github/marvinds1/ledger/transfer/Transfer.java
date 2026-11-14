package io.github.marvinds1.ledger.transfer;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Transfer(
        UUID id,
        String idempotencyKey,
        String requestHash,
        UUID sourceAccountId,
        UUID targetAccountId,
        long amount,
        String currency,
        TransferStatus status,
        String failureReason,
        String reference,
        OffsetDateTime createdAt) {
}
