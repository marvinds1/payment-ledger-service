package io.github.marvinds1.ledger.account;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class AccountRepository {

    private static final String COLUMNS = "id, owner_name, currency, balance, type, created_at";

    private final JdbcClient jdbc;

    public AccountRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Account insert(UUID id, String ownerName, String currency) {
        return jdbc.sql("INSERT INTO accounts (id, owner_name, currency, balance, type) "
                        + "VALUES (:id, :owner, :currency, 0, 'CUSTOMER') RETURNING " + COLUMNS)
                .param("id", id)
                .param("owner", ownerName)
                .param("currency", currency)
                .query(AccountRepository::map)
                .single();
    }

    public Optional<Account> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM accounts WHERE id = :id")
                .param("id", id)
                .query(AccountRepository::map)
                .optional();
    }

    public Optional<Account> findSettlement(String currency) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM accounts WHERE type = 'SETTLEMENT' AND currency = :currency")
                .param("currency", currency)
                .query(AccountRepository::map)
                .optional();
    }

    public List<Account> lockForUpdate(Collection<UUID> ids) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM accounts WHERE id IN (:ids) ORDER BY id FOR UPDATE")
                .param("ids", ids)
                .query(AccountRepository::map)
                .list();
    }

    public void updateBalance(UUID id, long balance) {
        jdbc.sql("UPDATE accounts SET balance = :balance WHERE id = :id")
                .param("balance", balance)
                .param("id", id)
                .update();
    }

    public void insertEntry(UUID transferId, UUID accountId, LedgerEntry.Direction direction, long amount, long balanceAfter) {
        jdbc.sql("INSERT INTO ledger_entries (transfer_id, account_id, direction, amount, balance_after) "
                        + "VALUES (:transferId, :accountId, :direction, :amount, :balanceAfter)")
                .param("transferId", transferId)
                .param("accountId", accountId)
                .param("direction", direction.name())
                .param("amount", amount)
                .param("balanceAfter", balanceAfter)
                .update();
    }

    public List<LedgerEntry> findEntries(UUID accountId, Long beforeId, int limit) {
        return jdbc.sql("SELECT id, transfer_id, account_id, direction, amount, balance_after, created_at "
                        + "FROM ledger_entries WHERE account_id = :accountId "
                        + "AND (CAST(:beforeId AS BIGINT) IS NULL OR id < :beforeId) "
                        + "ORDER BY id DESC LIMIT :limit")
                .param("accountId", accountId)
                .param("beforeId", beforeId)
                .param("limit", limit)
                .query((rs, n) -> new LedgerEntry(
                        rs.getLong("id"),
                        rs.getObject("transfer_id", UUID.class),
                        rs.getObject("account_id", UUID.class),
                        LedgerEntry.Direction.valueOf(rs.getString("direction")),
                        rs.getLong("amount"),
                        rs.getLong("balance_after"),
                        rs.getObject("created_at", OffsetDateTime.class)))
                .list();
    }

    private static Account map(ResultSet rs, int rowNum) throws SQLException {
        return new Account(
                rs.getObject("id", UUID.class),
                rs.getString("owner_name"),
                rs.getString("currency"),
                rs.getLong("balance"),
                AccountType.valueOf(rs.getString("type")),
                rs.getObject("created_at", OffsetDateTime.class));
    }
}
