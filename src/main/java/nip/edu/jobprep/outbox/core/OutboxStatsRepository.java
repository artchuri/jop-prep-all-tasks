package nip.edu.jobprep.outbox.core;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.selectOne;
import static org.jooq.impl.DSL.table;

@Repository
public class OutboxStatsRepository {
    private final DSLContext dsl;

    public OutboxStatsRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public long countByStatus(String status) {
        return dsl.fetchCount(
                dsl.selectOne().from(table("outbox"))
                        .where(field("status").eq(status))
        );
    }

    public Optional<Instant> findOldestNewCreatedAt() {
        Record row = dsl.select(field("created_at")).from(table("outbox"))
                .where(field("status").eq("NEW"))
                .orderBy(field("created_at").asc(), field("id").asc())
                .limit(1)
                .fetchOne();
        return row == null ? Optional.empty() : Optional.of(toInstant(row.get(field("created_at"))));
    }

    public int countScheduledRetries() {
        return dsl.fetchCount(
                selectOne().from(table("outbox"))
                        .where(field("status").eq("NEW"))
                        .and(field("next_attempt_at").isNotNull())
                        .and(field("next_attempt_at").gt(Instant.now()))
        );
    }

    private Instant toInstant(Object value) {
        if (value instanceof Instant instant) return instant;
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        throw new IllegalStateException("unsupported created_at type: " + value);
    }
}
