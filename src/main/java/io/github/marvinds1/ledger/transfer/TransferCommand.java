package io.github.marvinds1.ledger.transfer;

import io.github.marvinds1.ledger.common.Hashing;
import java.util.Objects;
import java.util.UUID;

public record TransferCommand(
        String idempotencyKey,
        UUID sourceAccountId,
        UUID targetAccountId,
        long amount,
        String currency,
        String reference,
        boolean allowSettlementAccounts) {

    public String requestHash() {
        return Hashing.sha256Hex(String.join("|",
                sourceAccountId.toString(),
                targetAccountId.toString(),
                Long.toString(amount),
                currency,
                Objects.requireNonNullElse(reference, "")));
    }
}
