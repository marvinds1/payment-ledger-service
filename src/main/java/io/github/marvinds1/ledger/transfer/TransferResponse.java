package io.github.marvinds1.ledger.transfer;

import java.net.URI;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public record TransferResponse(
        UUID id,
        UUID sourceAccountId,
        UUID targetAccountId,
        long amount,
        String currency,
        TransferStatus status,
        String failureReason,
        String reference,
        OffsetDateTime createdAt) {

    public static final String REPLAY_HEADER = "Idempotent-Replayed";

    public static TransferResponse from(Transfer t) {
        return new TransferResponse(t.id(), t.sourceAccountId(), t.targetAccountId(), t.amount(), t.currency(),
                t.status(), t.failureReason(), t.reference(), t.createdAt());
    }

    public static ResponseEntity<TransferResponse> toEntity(TransferResult result) {
        Transfer transfer = result.transfer();
        HttpStatus status;
        if (transfer.status() == TransferStatus.REJECTED) {
            status = HttpStatus.UNPROCESSABLE_ENTITY;
        } else {
            status = result.replayed() ? HttpStatus.OK : HttpStatus.CREATED;
        }
        return ResponseEntity.status(status)
                .location(URI.create("/api/v1/transfers/" + transfer.id()))
                .header(REPLAY_HEADER, Boolean.toString(result.replayed()))
                .body(from(transfer));
    }
}
