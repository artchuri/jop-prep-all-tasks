package nip.edu.jobprep.repositories;

import nip.edu.jobprep.entity.TransferHistory;
import nip.edu.jobprep.utils.mapper.TransferRowMapper;
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
public class TransferRepository {
    private static final Field<?>[] TRANSFER_FIELDS = {
            field("id"), field("recipient_account_id"), field("from_account_id"), field("amount"),
            field("created_at"), field("status"), field("idempotency_key"), field("currency")
    };

    private final DSLContext dsl;

    public TransferRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public Optional<TransferHistory> findByIdempotencyKey(String idempotencyKey) {
        Record record = dsl.select().from(table("transfers_history"))
                .where(field("idempotency_key").eq(idempotencyKey))
                .fetchOne();
        return Optional.ofNullable(record).map(TransferRowMapper::map);
    }

    public TransferHistory insertIfAbsent(long fromAccountId, long recipientAccountId, BigDecimal amount,
                                          Instant createdAt, String status, String idempotencyKey, String currency) {
        Record record = dsl.insertInto(table("transfers_history"))
                .set(field("from_account_id"), fromAccountId)
                .set(field("recipient_account_id"), recipientAccountId)
                .set(field("amount"), amount)
                .set(field("created_at"), createdAt)
                .set(field("status"), status)
                .set(field("idempotency_key"), idempotencyKey)
                .set(field("currency"), currency)
                .onConflict(field("idempotency_key"))
                .doNothing()
                .returning(TRANSFER_FIELDS).fetchOne();
        return record == null ? null : TransferRowMapper.map(record);
    }
}
