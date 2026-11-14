package io.github.marvinds1.ledger.account;

import io.github.marvinds1.ledger.common.BusinessRuleException;
import io.github.marvinds1.ledger.common.NotFoundException;
import java.util.List;
import java.util.UUID;
import org.springframework.stereotype.Service;

@Service
public class AccountService {

    private final AccountRepository accounts;

    public AccountService(AccountRepository accounts) {
        this.accounts = accounts;
    }

    public Account create(String ownerName, String currency) {
        if (accounts.findSettlement(currency).isEmpty()) {
            throw new BusinessRuleException("UNSUPPORTED_CURRENCY", "Currency " + currency + " is not supported");
        }
        return accounts.insert(UUID.randomUUID(), ownerName, currency);
    }

    public Account get(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new NotFoundException("Account " + id + " not found"));
    }

    public List<LedgerEntry> entries(UUID accountId, Long beforeId, int limit) {
        get(accountId);
        return accounts.findEntries(accountId, beforeId, limit);
    }
}
