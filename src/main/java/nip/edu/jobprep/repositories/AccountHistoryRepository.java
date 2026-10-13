package nip.edu.jobprep.repositories;

import nip.edu.jobprep.entity.AccountStatus;
import org.jooq.DSLContext;
import org.springframework.stereotype.Repository;

import java.time.Instant;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class AccountHistoryRepository {
    private final DSLContext dsl;

    public AccountHistoryRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public void save(long accountId, AccountStatus oldStatus, AccountStatus newStatus,
                     Long changedByUserId, Instant changedAt) {
        dsl.insertInto(table("account_status_history"))
                .set(field("account_id"), accountId)
                .set(field("old_status"), oldStatus == null ? null : oldStatus.name())
                .set(field("new_status"), newStatus.name())
                .set(field("changed_by"), changedByUserId)
                .set(field("changed_at"), changedAt)
                .execute();
    }
}
