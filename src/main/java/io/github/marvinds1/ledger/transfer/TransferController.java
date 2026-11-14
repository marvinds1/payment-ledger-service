package io.github.marvinds1.ledger.transfer;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/transfers")
public class TransferController {

    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;
    }

    @PostMapping
    public ResponseEntity<TransferResponse> create(
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 64) String idempotencyKey,
            @Valid @RequestBody TransferRequest request) {
        return TransferResponse.toEntity(transferService.transfer(idempotencyKey, request.sourceAccountId(),
                request.targetAccountId(), request.amount(), request.currency(), request.reference()));
    }

    @GetMapping("/{id}")
    public TransferResponse get(@PathVariable UUID id) {
        return TransferResponse.from(transferService.get(id));
    }

    public record TransferRequest(
            @NotNull UUID sourceAccountId,
            @NotNull UUID targetAccountId,
            @Positive long amount,
            @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency,
            @Size(max = 140) String reference) {
    }
}
