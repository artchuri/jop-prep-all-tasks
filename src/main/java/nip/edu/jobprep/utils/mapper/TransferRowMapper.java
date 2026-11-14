package nip.edu.jobprep.utils.mapper;

import nip.edu.jobprep.entity.TransferHistory;
import org.jooq.Field;
import org.jooq.Record;

import java.math.BigDecimal;
import java.time.Instant;
import java.sql.Timestamp;
import java.time.OffsetDateTime;

import static org.jooq.impl.DSL.field;

public final class TransferRowMapper {

    private static final Field<Long> ID = field("id", Long.class);
    private static final Field<Long> RECIPIENT_ACCOUNT_ID = field("recipient_account_id", Long.class);
    private static final Field<Long> FROM_ACCOUNT_ID = field("from_account_id", Long.class);
    private static final Field<BigDecimal> AMOUNT = field("amount", BigDecimal.class);
    private static final Field<Object> CREATED_AT = field("created_at", Object.class);
    private static final Field<String> STATUS = field("status", String.class);
    private static final Field<String> IDEMPOTENCY_KEY = field("idempotency_key", String.class);
    private static final Field<String> CURRENCY = field("currency", String.class);

    private TransferRowMapper() {
    }

    private static Instant toInstant(Object value) {
        return switch (value) {
            case null -> null;
            case Instant instant -> instant;
            case OffsetDateTime odt -> odt.toInstant();
            case Timestamp ts -> ts.toInstant();
            default -> throw new IllegalStateException("unsupported created_at type: " + value.getClass());
        };
    }

    public static TransferHistory map(Record record) {
        return TransferHistory.builder()
                .id(record.get(ID))
                .recipientAccountId(record.get(RECIPIENT_ACCOUNT_ID))
                .fromAccountId(record.get(FROM_ACCOUNT_ID))
                .amount(record.get(AMOUNT))
                .createdAt(toInstant(record.get(CREATED_AT)))
                .status(record.get(STATUS))
                .idempotencyKey(record.get(IDEMPOTENCY_KEY))
                .currency(record.get(CURRENCY))
                .build();
    }
}