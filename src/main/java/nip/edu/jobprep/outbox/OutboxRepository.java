package nip.edu.jobprep.outbox;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class OutboxRepository {
    private static final Field<?>[] OUTBOX_FIELDS = {
            field("id"), field("event_id"), field("event_type"), field("aggregate_id"),
            field("user_id"), field("payload"), field("created_at")
    };
    private static final Pattern PARTITION_NAME = Pattern.compile("\\d{4}_\\d{2}");

    private final DSLContext dsl;

    public OutboxRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void save(DomainEvent event, long aggregateId, String payloadJson, Instant occurredAt) {
        dsl.insertInto(table("outbox"))
                .set(field("event_id"), UUID.fromString(event.eventId()))
                .set(field("event_type"), event.eventType())
                .set(field("aggregate_id"), aggregateId)
                .set(field("user_id"), event.userId())
                .set(field("payload"), DSL.val(payloadJson).cast(SQLDataType.JSONB))
                .set(field("created_at"), occurredAt)
                .set(field("status"), "NEW")
                .execute();
    }

    public List<OutboxRow> claimBatch(int limit, String owner, Duration lease) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(lease);
        return dsl.transactionResult(configuration -> {
            DSLContext tx = configuration.dsl();
            List<Record> records = tx.select(OUTBOX_FIELDS)
                    .from(table("outbox"))
                    .where(field("next_attempt_at").isNull().or(field("next_attempt_at").le(now)))
                    .and(field("status").eq("NEW")
                            .or(field("status").eq("PROCESSING").and(field("lease_until").le(now))))
                    .orderBy(field("id").asc())
                    .limit(limit)
                    .forUpdate().skipLocked()
                    .fetch();
            for (Record record : records) {
                tx.update(table("outbox"))
                        .set(field("status"), "PROCESSING")
                        .set(field("lease_owner"), owner)
                        .set(field("lease_until"), leaseUntil)
                        .set(field("last_attempt_at"), now)
                        .where(field("id").eq(record.get(field("id", Long.class))))
                        .and(field("created_at").eq(toInstant(record.get(field("created_at")))))
                        .execute();
            }
            return records.stream().map(this::map).toList();
        });
    }

    public boolean renewLease(long id, Instant createdAt, String owner, Duration lease) {
        Instant now = Instant.now();
        return dsl.update(table("outbox"))
                .set(field("lease_until"), now.plus(lease))
                .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                .and(field("status").eq("PROCESSING"))
                .and(field("lease_owner").eq(owner))
                .and(field("lease_until").gt(now))
                .execute() == 1;
    }

    public void markAsCompleted(long id, Instant createdAt, String owner) {
        dsl.update(table("outbox"))
                .set(field("status"), "COMPLETED")
                .set(field("processed_at"), Instant.now())
                .setNull(field("lease_owner"))
                .setNull(field("lease_until"))
                .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                .and(field("status").eq("PROCESSING")).and(field("lease_owner").eq(owner))
                .execute();
    }

    public int recordFailure(long id, Instant createdAt, String owner, int maxAttempts, String error, Instant nextAttemptAt) {
        return dsl.transactionResult(configuration -> {
            DSLContext tx = configuration.dsl();
            Integer current = tx.select(field("attempt_count", Integer.class)).from(table("outbox"))
                    .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                    .and(field("status").eq("PROCESSING")).and(field("lease_owner").eq(owner))
                    .forUpdate().fetchOne(field("attempt_count", Integer.class));
            if (current == null) return 0;
            int attempts = current + 1;
            Instant retryAt = Instant.now().plusSeconds(Math.min(300, 1L << Math.min(attempts, 8)));
            boolean rejected = attempts >= maxAttempts;
            tx.update(table("outbox"))
                    .set(field("attempt_count"), attempts)
                    .set(field("status"), rejected ? "REJECTED" : "NEW")
                    .set(field("last_error"), error == null ? "unknown" : error.substring(0, Math.min(error.length(), 2000)))
                    .set(field("last_attempt_at"), Instant.now())
                    .set(field("next_attempt_at"), rejected ? null : retryAt)
                    .set(field("rejected_at"), rejected ? Instant.now() : null)
                    .setNull(field("lease_owner")).setNull(field("lease_until"))
                    .where(field("id").eq(id)).and(field("created_at").eq(createdAt))
                    .execute();
            return attempts;
        });
    }

    public boolean hasOlderActiveForUser(long userId, long id) {
        return dsl.fetchCount(
                dsl.selectOne().from(table("outbox"))
                        .where(field("user_id").eq(userId))
                        .and(field("id").lessThan(id))
                        .and(field("status").in("NEW", "PROCESSING"))
        ) > 0;
    }

    public long countByStatus(String status) {
        return dsl.fetchCount(
                dsl.selectOne().from(table("outbox"))
                        .where(field("status").eq(status))
        );
    }

    public java.util.Optional<Instant> findOldestNewCreatedAt() {
        Record row = dsl.select(field("created_at")).from(table("outbox"))
                .where(field("status").eq("NEW"))
                .orderBy(field("created_at").asc(), field("id").asc())
                .limit(1)
                .fetchOne();
        return row == null ? java.util.Optional.empty() : java.util.Optional.of(toInstant(row.get(field("created_at"))));
    }

    public int countScheduledRetries() {
        return dsl.fetchCount(
                dsl.selectOne().from(table("outbox"))
                        .where(field("status").eq("NEW"))
                        .and(field("next_attempt_at").isNotNull())
                        .and(field("next_attempt_at").gt(Instant.now()))
        );
    }

    public int pendingCount() {
        return dsl.fetchCount(
                dsl.selectOne().from(table("outbox"))
                        .where(field("status").eq("NEW"))
        );
    }

    public List<String> registeredPartitionsBefore(String cutoffYearMonth) {
        return dsl.select(field("year_month", String.class))
                .from(table("outbox_partition"))
                .where(field("year_month").lt(cutoffYearMonth))
                .orderBy(field("year_month"))
                .fetch(field("year_month", String.class));
    }

    public void forgetPartition(String yearMonth) {
        dsl.deleteFrom(table("outbox_partition"))
                .where(field("year_month").eq(yearMonth))
                .execute();
    }

    public boolean partitionExists(String yearMonth) {
        Record row = dsl.fetchOne("select 1 from pg_class where relname = 'outbox_" + yearMonth + "'");
        return row != null;
    }

    public boolean dropPartition(String yearMonth) {
        if (!PARTITION_NAME.matcher(yearMonth).matches()) {
            throw new IllegalArgumentException("unexpected partition name: " + yearMonth);
        }
        dsl.execute("DROP TABLE IF EXISTS outbox_" + yearMonth);
        return true;
    }

    public int pendingInPartition(String yearMonth) {
        if (!PARTITION_NAME.matcher(yearMonth).matches()) {
            throw new IllegalArgumentException("unexpected partition name: " + yearMonth);
        }
        Record row = dsl.fetchOne("select count(*) as cnt from outbox_" + yearMonth + " where status = 'NEW'");
        return row == null ? 0 : row.get("cnt", Integer.class);
    }

    private OutboxRow map(Record record) {
        return new OutboxRow(
                record.get(field("id", Long.class)),
                record.get(field("event_id", UUID.class)).toString(),
                record.get(field("event_type", String.class)),
                record.get(field("aggregate_id", Long.class)),
                record.get(field("user_id", Long.class)),
                record.get(field("payload")).toString(),
                record.get(field("created_at", Timestamp.class)).toInstant()
        );
    }

    private Instant toInstant(Object value) {
        if (value instanceof Instant instant) return instant;
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        throw new IllegalStateException("unsupported created_at type: " + value);
    }
}
