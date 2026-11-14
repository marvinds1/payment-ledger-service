package io.github.marvinds1.ledger.transfer;

import io.github.marvinds1.ledger.account.Account;
import io.github.marvinds1.ledger.account.AccountRepository;
import io.github.marvinds1.ledger.account.LedgerEntry.Direction;
import io.github.marvinds1.ledger.common.BusinessRuleException;
import io.github.marvinds1.ledger.common.NotFoundException;
import io.github.marvinds1.ledger.outbox.OutboxRepository;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

@Component
class TransferExecutor {

    static final String INSUFFICIENT_FUNDS = "INSUFFICIENT_FUNDS";

    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final OutboxRepository outbox;

    TransferExecutor(AccountRepository accounts, TransferRepository transfers, OutboxRepository outbox) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.outbox = outbox;
    }

    @Transactional
    Transfer execute(TransferCommand cmd) {
        Map<UUID, Account> locked = accounts.lockForUpdate(List.of(cmd.sourceAccountId(), cmd.targetAccountId()))
                .stream()
                .collect(Collectors.toMap(Account::id, Function.identity()));

        Account source = require(locked, cmd.sourceAccountId());
        Account target = require(locked, cmd.targetAccountId());
        validate(cmd, source, target);

        UUID transferId = UUID.randomUUID();
        if (!source.canGoNegative() && source.balance() < cmd.amount()) {
            Transfer rejected = transfers.insert(cmd, transferId, TransferStatus.REJECTED, INSUFFICIENT_FUNDS);
            outbox.append("TRANSFER", transferId, "TransferRejected", payload(rejected));
            return rejected;
        }

        long sourceBalance = source.balance() - cmd.amount();
        long targetBalance = target.balance() + cmd.amount();
        Transfer completed = transfers.insert(cmd, transferId, TransferStatus.COMPLETED, null);

        accounts.updateBalance(source.id(), sourceBalance);
        accounts.updateBalance(target.id(), targetBalance);
        accounts.insertEntry(transferId, source.id(), Direction.DEBIT, cmd.amount(), sourceBalance);
        accounts.insertEntry(transferId, target.id(), Direction.CREDIT, cmd.amount(), targetBalance);
        outbox.append("TRANSFER", transferId, "TransferCompleted", payload(completed));
        return completed;
    }

    private static Account require(Map<UUID, Account> locked, UUID id) {
        Account account = locked.get(id);
        if (account == null) {
            throw new NotFoundException("Account " + id + " not found");
        }
        return account;
    }

    private static void validate(TransferCommand cmd, Account source, Account target) {
        if (!cmd.allowSettlementAccounts() && (source.canGoNegative() || target.canGoNegative())) {
            throw new BusinessRuleException("SETTLEMENT_ACCOUNT_NOT_ALLOWED",
                    "Settlement accounts cannot be used in customer transfers");
        }
        if (!source.currency().equals(cmd.currency()) || !target.currency().equals(cmd.currency())) {
            throw new BusinessRuleException("CURRENCY_MISMATCH",
                    "Both accounts must be denominated in " + cmd.currency());
        }
    }

    private static Map<String, Object> payload(Transfer t) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("transferId", t.id());
        payload.put("sourceAccountId", t.sourceAccountId());
        payload.put("targetAccountId", t.targetAccountId());
        payload.put("amount", t.amount());
        payload.put("currency", t.currency());
        payload.put("status", t.status());
        payload.put("failureReason", t.failureReason());
        payload.put("occurredAt", t.createdAt());
        return payload;
    }
}
