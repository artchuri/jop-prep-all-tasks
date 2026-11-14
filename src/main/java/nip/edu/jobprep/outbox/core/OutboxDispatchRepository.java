package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.utils.mapper.OutboxRowMapper;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Condition;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.ConcurrentModificationException;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class OutboxDispatchRepository {
    private static final Field<?>[] OUTBOX_FIELDS = {
            field("id"), field("event_id"), field("event_type"), field("aggregate_id"),
            field("user_id"), field("payload"), field("created_at")
    };

    private final DSLContext dsl;

    public OutboxDispatchRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public List<OutboxRow> claimBatch(int limit, String owner, Duration lease) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(lease);

        return dsl.transactionResult(cfg -> {
            DSLContext tx = cfg.dsl();

            List<OutboxRow> rows = findClaimableRows(tx, now, limit);
            leaseRows(tx, rows, owner, leaseUntil, now);

            return rows;
        });
    }

    private List<OutboxRow> findClaimableRows(DSLContext tx, Instant now, int limit) {
        return tx.select(OUTBOX_FIELDS)
                .from(table("outbox"))
                .where(nextAttemptIsDue(now))
                .and(statusIsClaimable(now))
                .orderBy(field("id").asc())
                .limit(limit)
                .forUpdate()
                .skipLocked()
                .fetchStream()
                .map(OutboxRowMapper::map)
                .toList();
    }

    private void leaseRows(DSLContext tx, List<OutboxRow> rows, String owner, Instant leaseUntil, Instant now) {
        if (rows.isEmpty()) {
            return;
        }

        int updated = tx.update(table("outbox"))
                .set(field("status"), GlobalValues.STATUS_PROCESSING)
                .set(field("lease_owner"), owner)
                .set(field("lease_until"), leaseUntil)
                .set(field("last_attempt_at"), now)
                .where(field("id").in(rows.stream().map(OutboxRow::id).toList()))
                .execute();

        if (updated != rows.size()) {
            throw new ConcurrentModificationException(
                    "some outbox rows were changed while claiming: expected %d, updated %d"
                            .formatted(rows.size(), updated));
        }
    }

    private Condition nextAttemptIsDue(Instant now) {
        return field("next_attempt_at").isNull().or(field("next_attempt_at").le(now));
    }

    private Condition statusIsClaimable(Instant now) {
        return field("status").eq(GlobalValues.STATUS_NEW)
                .or(field("status").eq(GlobalValues.STATUS_PROCESSING).and(field("lease_until").le(now)));
    }

    public boolean renewLease(long id, Instant createdAt, String owner, Duration lease) {
        Instant now = Instant.now();
        return dsl.update(table("outbox"))
                .set(field("lease_until"), now.plus(lease))
                .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                .and(field("status").eq(GlobalValues.STATUS_PROCESSING))
                .and(field("lease_owner").eq(owner))
                .and(field("lease_until").gt(now))
                .execute() == 1;
    }

    public void markAsCompleted(long id, Instant createdAt, String owner) {
        dsl.update(table("outbox"))
                .set(field("status"), GlobalValues.STATUS_COMPLETED)
                .set(field("processed_at"), Instant.now())
                .setNull(field("lease_owner"))
                .setNull(field("lease_until"))
                .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                .and(field("status").eq(GlobalValues.STATUS_PROCESSING)).and(field("lease_owner").eq(owner))
                .execute();
    }

    public int recordFailure(long id, Instant createdAt, String owner, int maxAttempts, String error,
                             OutboxRetryPolicy retryPolicy) {
        return dsl.transactionResult(configuration -> {
            DSLContext tx = configuration.dsl();

            Integer currentAttempts = findCurrentAttempts(tx, id, createdAt, owner);
            if (currentAttempts == null) {
                return 0;
            }

            OutboxRetryPolicy.RetryDecision decision =
                    retryPolicy.nextFailure(currentAttempts, maxAttempts, Instant.now());
            applyFailure(tx, id, createdAt, decision, error);

            return decision.attempts();
        });
    }

    private Integer findCurrentAttempts(DSLContext tx, long id, Instant createdAt, String owner) {
        return tx.select(field("attempt_count", Integer.class))
                .from(table("outbox"))
                .where(field("id").eq(id))
                .and(field("created_at").eq(createdAt))
                .and(field("status").eq(GlobalValues.STATUS_PROCESSING))
                .and(field("lease_owner").eq(owner))
                .forUpdate()
                .fetchOne(field("attempt_count", Integer.class));
    }

    private void applyFailure(
            DSLContext tx,
            long id,
            Instant createdAt,
            OutboxRetryPolicy.RetryDecision decision,
            String error
    ) {
        tx.update(table("outbox"))
                .set(field("attempt_count"), decision.attempts())
                .set(field("status"), decision.rejected() ? GlobalValues.STATUS_REJECTED : GlobalValues.STATUS_NEW)
                .set(field("last_error"), truncateError(error))
                .set(field("last_attempt_at"), Instant.now())
                .set(field("next_attempt_at"), decision.nextAttemptAt())
                .set(field("rejected_at"), decision.rejectedAt())
                .setNull(field("lease_owner"))
                .setNull(field("lease_until"))
                .where(field("id").eq(id))
                .and(field("created_at").eq(createdAt))
                .execute();
    }

    public boolean hasOlderActiveForUser(long userId, long id) {
        return dsl.fetchExists(
                dsl.selectOne().from(table("outbox"))
                        .where(field("user_id").eq(userId))
                        .and(field("id").lessThan(id))
                        .and(field("status").in(GlobalValues.STATUS_NEW, GlobalValues.STATUS_PROCESSING))
                        .limit(1)
        );
    }

    private String truncateError(String error) {
        return error == null ? "unknown" : error.substring(0, Math.min(error.length(), 2000));
    }
}
