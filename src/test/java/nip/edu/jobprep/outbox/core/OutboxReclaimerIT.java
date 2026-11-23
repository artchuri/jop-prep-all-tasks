package nip.edu.jobprep.outbox.core;

import org.jooq.impl.DSL;
import nip.edu.jobprep.model.GlobalValues;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Instant;
import java.util.UUID;
import org.jooq.impl.SQLDataType;
import org.jooq.Field;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;
import static org.assertj.core.api.Assertions.assertThat;

class OutboxReclaimerIT extends AbstractIntegrationTest {

    @Autowired
    OutboxReclaimer reclaimer;

    @Autowired
    DSLContext dsl;

    private void insertRow(long id, String status, Instant rejectedAt) {
        dsl.insertInto(table("outbox"))
                .set(field("id"), id)
                .set(field("event_id"), UUID.randomUUID())
                .set(field("event_type"), "TRANSFER_COMPLETED")
                .set(field("aggregate_id"), 1L)
                .set(field("user_id"), 1L)
                .set(DSL.field("payload", SQLDataType.JSONB), DSL.val("{}").cast(SQLDataType.JSONB))
                .set(field("created_at"), Instant.now())
                .set(field("attempt_count"), 5)
                .set(field("status"), status)
                .set(field("rejected_at"), rejectedAt)
                .set(field("last_error"), "kafka down")
                .execute();
    }

    private String statusOf(long id) {
        return dsl.select(field("status")).from(table("outbox")).where(field("id").eq(id)).fetchOne(field("status", String.class));
    }

    private Integer attemptsOf(long id) {
        return dsl.select(field("attempt_count")).from(table("outbox")).where(field("id").eq(id)).fetchOne(field("attempt_count", Integer.class));
    }

    @Test
    void returnsOldRejectedRowsToNewAndResetsAttempts() {
        insertRow(9101L, GlobalValues.STATUS_REJECTED, Instant.now().minus(OutboxReclaimer.REDRIVE_AFTER).minusSeconds(10));

        reclaimer.redrive();

        assertThat(statusOf(9101L)).isEqualTo(GlobalValues.STATUS_NEW);
        assertThat(attemptsOf(9101L)).isZero();
    }

    @Test
    void leavesFreshRejectedRowsAlone() {
        insertRow(9102L, GlobalValues.STATUS_REJECTED, Instant.now());

        reclaimer.redrive();

        assertThat(statusOf(9102L)).isEqualTo(GlobalValues.STATUS_REJECTED);
    }

    @Test
    void doesNotTouchOtherStatuses() {
        insertRow(9103L, GlobalValues.STATUS_NEW, null);
        insertRow(9104L, GlobalValues.STATUS_PROCESSING, null);
        insertRow(9105L, GlobalValues.STATUS_COMPLETED, Instant.now().minus(OutboxReclaimer.REDRIVE_AFTER));

        reclaimer.redrive();

        assertThat(statusOf(9103L)).isEqualTo(GlobalValues.STATUS_NEW);
        assertThat(statusOf(9104L)).isEqualTo(GlobalValues.STATUS_PROCESSING);
        assertThat(statusOf(9105L)).isEqualTo(GlobalValues.STATUS_COMPLETED);
    }
}