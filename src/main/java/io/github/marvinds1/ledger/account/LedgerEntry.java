package io.github.marvinds1.ledger.account;

import java.time.OffsetDateTime;
import java.util.UUID;

public record LedgerEntry(
        long id,
        UUID transferId,
        UUID accountId,
        Direction direction,
        long amount,
        long balanceAfter,
        OffsetDateTime createdAt) {

    public enum Direction {
        DEBIT,
        CREDIT
    }
}
