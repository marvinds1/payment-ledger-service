package io.github.marvinds1.ledger.transfer;

import io.github.marvinds1.ledger.account.Account;
import io.github.marvinds1.ledger.account.AccountRepository;
import io.github.marvinds1.ledger.common.BusinessRuleException;
import io.github.marvinds1.ledger.common.NotFoundException;
import java.util.UUID;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.stereotype.Service;

@Service
public class TransferService {

    private final TransferExecutor executor;
    private final TransferRepository transfers;
    private final AccountRepository accounts;

    TransferService(TransferExecutor executor, TransferRepository transfers, AccountRepository accounts) {
        this.executor = executor;
        this.transfers = transfers;
        this.accounts = accounts;
    }

    public TransferResult transfer(String idempotencyKey, UUID source, UUID target, long amount, String currency,
                                   String reference) {
        if (source.equals(target)) {
            throw new BusinessRuleException("SAME_ACCOUNT", "Source and target accounts must differ");
        }
        return execute(new TransferCommand(idempotencyKey, source, target, amount, currency, reference, false));
    }

    public TransferResult deposit(UUID accountId, String idempotencyKey, long amount, String reference) {
        Account account = accounts.findById(accountId)
                .orElseThrow(() -> new NotFoundException("Account " + accountId + " not found"));
        Account settlement = accounts.findSettlement(account.currency())
                .orElseThrow(() -> new IllegalStateException("No settlement account for " + account.currency()));
        if (account.canGoNegative()) {
            throw new BusinessRuleException("SETTLEMENT_ACCOUNT_NOT_ALLOWED", "Cannot deposit into a settlement account");
        }
        return execute(new TransferCommand(idempotencyKey, settlement.id(), accountId, amount, account.currency(),
                reference, true));
    }

    public Transfer get(UUID id) {
        return transfers.findById(id).orElseThrow(() -> new NotFoundException("Transfer " + id + " not found"));
    }

    private TransferResult execute(TransferCommand cmd) {
        var existing = transfers.findByIdempotencyKey(cmd.idempotencyKey());
        if (existing.isPresent()) {
            return replay(existing.get(), cmd);
        }
        try {
            return new TransferResult(executor.execute(cmd), false);
        } catch (DuplicateKeyException race) {
            Transfer winner = transfers.findByIdempotencyKey(cmd.idempotencyKey()).orElseThrow(() -> race);
            return replay(winner, cmd);
        }
    }

    private static TransferResult replay(Transfer existing, TransferCommand cmd) {
        if (!existing.requestHash().equals(cmd.requestHash())) {
            throw new BusinessRuleException("IDEMPOTENCY_KEY_REUSED",
                    "Idempotency-Key was already used with a different request");
        }
        return new TransferResult(existing, true);
    }
}
