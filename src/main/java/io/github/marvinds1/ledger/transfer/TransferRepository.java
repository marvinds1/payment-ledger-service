package io.github.marvinds1.ledger.transfer;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class TransferRepository {

    private static final String COLUMNS = "id, idempotency_key, request_hash, source_account_id, target_account_id, "
            + "amount, currency, status, failure_reason, reference, created_at";

    private final JdbcClient jdbc;

    public TransferRepository(JdbcClient jdbc) {
        this.jdbc = jdbc;
    }

    public Transfer insert(TransferCommand cmd, UUID id, TransferStatus status, String failureReason) {
        return jdbc.sql("INSERT INTO transfers (id, idempotency_key, request_hash, source_account_id, target_account_id, "
                        + "amount, currency, status, failure_reason, reference) "
                        + "VALUES (:id, :key, :hash, :source, :target, :amount, :currency, :status, :failureReason, :reference) "
                        + "RETURNING " + COLUMNS)
                .param("id", id)
                .param("key", cmd.idempotencyKey())
                .param("hash", cmd.requestHash())
                .param("source", cmd.sourceAccountId())
                .param("target", cmd.targetAccountId())
                .param("amount", cmd.amount())
                .param("currency", cmd.currency())
                .param("status", status.name())
                .param("failureReason", failureReason)
                .param("reference", cmd.reference())
                .query(TransferRepository::map)
                .single();
    }

    public Optional<Transfer> findById(UUID id) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transfers WHERE id = :id")
                .param("id", id)
                .query(TransferRepository::map)
                .optional();
    }

    public Optional<Transfer> findByIdempotencyKey(String key) {
        return jdbc.sql("SELECT " + COLUMNS + " FROM transfers WHERE idempotency_key = :key")
                .param("key", key)
                .query(TransferRepository::map)
                .optional();
    }

    private static Transfer map(ResultSet rs, int rowNum) throws SQLException {
        return new Transfer(
                rs.getObject("id", UUID.class),
                rs.getString("idempotency_key"),
                rs.getString("request_hash"),
                rs.getObject("source_account_id", UUID.class),
                rs.getObject("target_account_id", UUID.class),
                rs.getLong("amount"),
                rs.getString("currency"),
                TransferStatus.valueOf(rs.getString("status")),
                rs.getString("failure_reason"),
                rs.getString("reference"),
                rs.getObject("created_at", OffsetDateTime.class));
    }
}
