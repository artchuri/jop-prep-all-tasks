package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.outbox.event.DomainEvent;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.UUID;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class OutboxEventRepository {
    private final DSLContext dsl;

    public OutboxEventRepository(DSLContext dsl) {
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
                .set(field("status"), GlobalValues.STATUS_NEW)
                .execute();
    }
}
