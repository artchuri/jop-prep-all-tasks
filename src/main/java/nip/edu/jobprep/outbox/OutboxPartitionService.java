package nip.edu.jobprep.outbox;

import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Component
public class OutboxPartitionService {
    private static final DateTimeFormatter YM = DateTimeFormatter.ofPattern("yyyy_MM");
    private static final String DEFAULT_PARTITION = "outbox_default";
    private static final List<String> COLUMNS = List.of(
            "id", "event_id", "event_type", "aggregate_id", "user_id",
            "payload", "created_at", "processed_at", "rejected_at", "attempt_count", "status",
            "lease_owner", "lease_until", "next_attempt_at", "last_attempt_at", "last_error");

    private final DSLContext dsl;

    public OutboxPartitionService(DSLContext dsl) {
        this.dsl = dsl;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initPartitions() {
        YearMonth current = YearMonth.now(ZoneOffset.UTC);
        ensurePartition(current.minusMonths(1));
        ensurePartition(current);
        ensurePartition(current.plusMonths(1));
    }

    @Scheduled(cron = "0 0 2 25 * *")
    public void ensureNextMonthPartition() {
        ensurePartition(YearMonth.now(ZoneOffset.UTC).plusMonths(1));
    }

    public void ensurePartition(YearMonth month) {
        String name = month.format(YM);
        String from = month.atDay(1).toString();
        String to = month.plusMonths(1).atDay(1).toString();
        dsl.transactionResult(cfg -> {
            DSLContext d = cfg.dsl();
            d.execute("SELECT pg_advisory_xact_lock(hashtext('outbox_partition_ddl'))");
            if (partitionExists(d, name)) {
                register(d, name);
                return null;
            }
            d.execute("ALTER TABLE public.outbox DETACH PARTITION " + DEFAULT_PARTITION);
            try {
                d.execute("CREATE TABLE public.outbox_" + name
                        + " PARTITION OF public.outbox FOR VALUES FROM ('" + from + "') TO ('" + to + "')");
                d.execute("WITH moved AS ("
                        + "DELETE FROM public." + DEFAULT_PARTITION
                        + " WHERE created_at >= '" + from + "' AND created_at < '" + to + "'"
                        + " RETURNING " + String.join(", ", COLUMNS) + ") "
                        + "INSERT INTO public.outbox (" + String.join(", ", COLUMNS) + ") SELECT " + String.join(", ", COLUMNS) + " FROM moved");
                d.execute("ALTER TABLE outbox ATTACH PARTITION " + DEFAULT_PARTITION + " DEFAULT");
            } catch (RuntimeException e) {
                d.execute("ALTER TABLE outbox ATTACH PARTITION " + DEFAULT_PARTITION + " DEFAULT");
                throw e;
            }
            register(d, name);
            return null;
        });
    }

    private boolean partitionExists(DSLContext d, String name) {
        return d.fetchValue("select to_regclass(\'public.outbox_" + name + "\')") != null;
    }

    private void register(DSLContext d, String name) {
        d.insertInto(table("outbox_partition"))
                .set(field("year_month"), name)
                .set(field("created_at"), Instant.now())
                .onConflictDoNothing()
                .execute();
    }
}
