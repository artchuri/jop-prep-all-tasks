package nip.edu.jobprep.utils.mapper;

import nip.edu.jobprep.outbox.core.OutboxRow;
import org.jooq.Field;
import org.jooq.Record;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;

import static org.jooq.impl.DSL.field;

public final class OutboxRowMapper {

    private static final Field<Long> ID = field("id", Long.class);
    private static final Field<UUID> EVENT_ID = field("event_id", UUID.class);
    private static final Field<String> EVENT_TYPE = field("event_type", String.class);
    private static final Field<Long> AGGREGATE_ID = field("aggregate_id", Long.class);
    private static final Field<Long> USER_ID = field("user_id", Long.class);
    private static final Field<Object> PAYLOAD = field("payload", Object.class);
    private static final Field<Instant> CREATED_AT = field("created_at", Instant.class);

    private OutboxRowMapper() {
    }

    public static OutboxRow map(Record record) {
        return new OutboxRow(
                record.get(ID),
                record.get(EVENT_ID).toString(),
                record.get(EVENT_TYPE),
                record.get(AGGREGATE_ID),
                record.get(USER_ID),
                payloadAsString(record.get(PAYLOAD)),
                toInstant(record.get(CREATED_AT))
        );
    }

    private static String payloadAsString(Object value) {
        if (value == null) return null;
        if (value instanceof String s) return s;
        return value.toString();
    }

    private static Instant toInstant(Object value) {
        if (value instanceof Instant instant) return instant;
        if (value instanceof Timestamp timestamp) return timestamp.toInstant();
        throw new IllegalStateException("unsupported created_at type: " + value);
    }
}