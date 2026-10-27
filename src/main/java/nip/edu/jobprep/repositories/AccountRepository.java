package nip.edu.jobprep.repositories;

import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.User;
import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Record;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class AccountRepository {
    private static final Field<?>[] ACCOUNT_FIELDS = {
            field("id"), field("user_id"), field("status"), field("created_at"), field("updated_at"),
            field("status_changed_at"), field("version"), field("amount"), field("currency")
    };

    private final DSLContext dsl;

    public AccountRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Optional<Account> findByIdForUpdate(long id) {
        Record record = dsl.select().from(table("accounts"))
                .where(field("id").eq(id)).forUpdate().fetchOne();
        return Optional.ofNullable(record).map(this::map);
    }

    public Account updateStatus(long id, long expectedVersion, AccountStatus status, Instant changedAt) {
        Record record = dsl.update(table("accounts"))
                .set(field("status"), status.name())
                .set(field("updated_at"), changedAt)
                .set(field("status_changed_at"), changedAt)
                .set(field("version"), expectedVersion + 1)
                .where(field("id").eq(id).and(field("version").eq(expectedVersion)))
                .returning(ACCOUNT_FIELDS).fetchOne();
        return map(record);
    }

    public Account updateAmount(long id, long expectedVersion, BigDecimal amount, Instant updatedAt) {
        Record record = dsl.update(table("accounts"))
                .set(field("amount"), amount)
                .set(field("updated_at"), updatedAt)
                .set(field("version"), expectedVersion + 1)
                .where(field("id").eq(id).and(field("version").eq(expectedVersion)))
                .returning(ACCOUNT_FIELDS).fetchOne();
        return map(record);
    }

    private Account map(Record record) {
        User user = User.builder()
                .id(record.get(field("user_id", Long.class)))
                .build();

        return Account.builder()
                .id(record.get(field("id", Long.class)))
                .user(user)
                .status(AccountStatus.valueOf(record.get(field("status", String.class))))
                .createdAt(record.get(field("created_at"), Instant.class))
                .updatedAt(record.get(field("updated_at"), Instant.class))
                .statusChangedAt(record.get(field("status_changed_at"), Instant.class))
                .version(record.get(field("version", Long.class)))
                .amount(record.get(field("amount", BigDecimal.class)))
                .currency(record.get(field("currency", String.class)))
                .build();
    }
}
