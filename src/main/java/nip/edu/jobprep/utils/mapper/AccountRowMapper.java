package nip.edu.jobprep.utils.mapper;

import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.User;
import org.jooq.Field;
import org.jooq.Record;

import static org.jooq.impl.DSL.field;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.sql.Timestamp;

public final class AccountRowMapper {

    private static final Field<Long> ID = field("id", Long.class);
    private static final Field<Long> USER_ID = field("user_id", Long.class);
    private static final Field<String> STATUS = field("status", String.class);
    private static final Field<Object> CREATED_AT = field("created_at", Object.class);
    private static final Field<Object> UPDATED_AT = field("updated_at", Object.class);
    private static final Field<Object> STATUS_CHANGED_AT = field("status_changed_at", Object.class);
    private static final Field<Long> VERSION = field("version", Long.class);
    private static final Field<BigDecimal> AMOUNT = field("amount", BigDecimal.class);
    private static final Field<String> CURRENCY = field("currency", String.class);

    private AccountRowMapper() {
    }

    private static Instant toInstant(Object value) {
        return switch (value) {
            case null -> null;
            case Instant instant -> instant;
            case OffsetDateTime odt -> odt.toInstant();
            case Timestamp ts -> ts.toInstant();
            default -> throw new IllegalStateException("unsupported timestamp type: " + value.getClass());
        };
    }

    public static Account map(Record record) {
        User user = User.builder()
                .id(record.get(USER_ID))
                .build();

        return Account.builder()
                .id(record.get(ID))
                .user(user)
                .status(AccountStatus.valueOf(record.get(STATUS)))
                .createdAt(toInstant(record.get(CREATED_AT)))
                .updatedAt(toInstant(record.get(UPDATED_AT)))
                .statusChangedAt(toInstant(record.get(STATUS_CHANGED_AT)))
                .version(record.get(VERSION))
                .amount(record.get(AMOUNT))
                .currency(record.get(CURRENCY))
                .build();
    }
}