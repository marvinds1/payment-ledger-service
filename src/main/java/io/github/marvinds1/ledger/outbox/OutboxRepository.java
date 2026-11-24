package io.github.marvinds1.ledger.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Repository;

@Repository
public class OutboxRepository {

    private final JdbcClient jdbc;
    private final ObjectMapper objectMapper;

    public OutboxRepository(JdbcClient jdbc, ObjectMapper objectMapper) {
        this.jdbc = jdbc;
        this.objectMapper = objectMapper;
    }

    public void append(String aggregateType, UUID aggregateId, String eventType, Map<String, Object> payload) {
        jdbc.sql("INSERT INTO outbox_events (aggregate_type, aggregate_id, event_type, payload) "
                        + "VALUES (:type, :aggregateId, :eventType, CAST(:payload AS JSONB))")
                .param("type", aggregateType)
                .param("aggregateId", aggregateId)
                .param("eventType", eventType)
                .param("payload", toJson(payload))
                .update();
    }

    List<OutboxEvent> lockUnpublished(int limit) {
        return jdbc.sql("SELECT id, aggregate_type, aggregate_id, event_type, payload::text AS payload "
                        + "FROM outbox_events WHERE published_at IS NULL "
                        + "ORDER BY id LIMIT :limit FOR UPDATE SKIP LOCKED")
                .param("limit", limit)
                .query((rs, n) -> new OutboxEvent(
                        rs.getLong("id"),
                        rs.getString("aggregate_type"),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getString("event_type"),
                        rs.getString("payload")))
                .list();
    }

    PendingStats pendingStats() {
        return jdbc.sql("SELECT COUNT(*) AS pending, "
                        + "COALESCE(EXTRACT(EPOCH FROM (now() - MIN(created_at))), 0) AS oldest_age_seconds "
                        + "FROM outbox_events WHERE published_at IS NULL")
                .query((rs, n) -> new PendingStats(rs.getLong("pending"), rs.getDouble("oldest_age_seconds")))
                .single();
    }

    record PendingStats(long pending, double oldestAgeSeconds) {
    }

    void markPublished(List<Long> ids) {
        jdbc.sql("UPDATE outbox_events SET published_at = now() WHERE id IN (:ids)")
                .param("ids", ids)
                .update();
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Cannot serialize outbox payload", e);
        }
    }
}
