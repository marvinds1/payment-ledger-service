package io.github.marvinds1.ledger.account;

import java.time.OffsetDateTime;
import java.util.UUID;

public record Account(
        UUID id,
        String ownerName,
        String currency,
        long balance,
        AccountType type,
        OffsetDateTime createdAt) {

    public boolean canGoNegative() {
        return type == AccountType.SETTLEMENT;
    }
}
