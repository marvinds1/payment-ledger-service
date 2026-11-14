package io.github.marvinds1.ledger.account;

import io.github.marvinds1.ledger.transfer.TransferResponse;
import io.github.marvinds1.ledger.transfer.TransferService;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;
import jakarta.validation.constraints.Size;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/accounts")
public class AccountController {

    private final AccountService accountService;
    private final TransferService transferService;

    public AccountController(AccountService accountService, TransferService transferService) {
        this.accountService = accountService;
        this.transferService = transferService;
    }

    @PostMapping
    public ResponseEntity<AccountResponse> create(@Valid @RequestBody CreateAccountRequest request) {
        Account account = accountService.create(request.ownerName(), request.currency());
        return ResponseEntity.created(URI.create("/api/v1/accounts/" + account.id()))
                .body(AccountResponse.from(account));
    }

    @GetMapping("/{id}")
    public AccountResponse get(@PathVariable UUID id) {
        return AccountResponse.from(accountService.get(id));
    }

    @GetMapping("/{id}/entries")
    public List<LedgerEntry> entries(
            @PathVariable UUID id,
            @RequestParam(required = false) Long beforeId,
            @RequestParam(defaultValue = "50") @Min(1) @Max(200) int limit) {
        return accountService.entries(id, beforeId, limit);
    }

    @PostMapping("/{id}/deposits")
    public ResponseEntity<TransferResponse> deposit(
            @PathVariable UUID id,
            @RequestHeader("Idempotency-Key") @NotBlank @Size(max = 64) String idempotencyKey,
            @Valid @RequestBody DepositRequest request) {
        return TransferResponse.toEntity(transferService.deposit(id, idempotencyKey, request.amount(), request.reference()));
    }

    public record CreateAccountRequest(
            @NotBlank @Size(max = 120) String ownerName,
            @NotBlank @Pattern(regexp = "[A-Z]{3}") String currency) {
    }

    public record DepositRequest(@Positive long amount, @Size(max = 140) String reference) {
    }

    public record AccountResponse(
            UUID id, String ownerName, String currency, long balance, AccountType type, OffsetDateTime createdAt) {

        static AccountResponse from(Account a) {
            return new AccountResponse(a.id(), a.ownerName(), a.currency(), a.balance(), a.type(), a.createdAt());
        }
    }
}
