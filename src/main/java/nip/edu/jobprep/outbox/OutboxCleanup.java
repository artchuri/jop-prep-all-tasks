package nip.edu.jobprep.outbox;

import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Component
public class OutboxCleanup {
    private static final Logger log = LoggerFactory.getLogger(OutboxCleanup.class);
    private static final DateTimeFormatter YM = DateTimeFormatter.ofPattern("yyyy_MM");
    private static final Pattern PARTITION_NAME = Pattern.compile("\\d{4}_\\d{2}");
    private static final int RETENTION_MONTHS = 1;
    private static final String DDL_LOCK = "outbox_partition_ddl";

    private final DSLContext dsl;

    public OutboxCleanup(DSLContext dsl) {
        this.dsl = dsl;
    }

    @Scheduled(cron = "0 0 3 * * *")
    public void cleanup() {
        String cutoff = YearMonth.now(ZoneOffset.UTC).minusMonths(RETENTION_MONTHS).format(YM);
        dsl.transactionResult(configuration -> {
            DSLContext tx = configuration.dsl();
            tx.execute("SELECT pg_advisory_xact_lock(hashtext('" + DDL_LOCK + "'))");
            tx.execute("LOCK TABLE outbox IN ACCESS EXCLUSIVE MODE");
            List<String> months = tx.select(field("year_month", String.class))
                    .from(table("outbox_partition"))
                    .where(field("year_month").lt(cutoff))
                    .orderBy(field("year_month"))
                    .fetch(field("year_month", String.class));
            for (String month : months) {
                if (!PARTITION_NAME.matcher(month).matches()) {
                    throw new IllegalStateException("unexpected outbox partition name: " + month);
                }
                Record unresolved = tx.fetchOne("select count(*) as cnt from public.outbox_" + month
                        + " where status in ('NEW', 'PROCESSING', 'REJECTED')");
                int unresolvedCount = unresolved == null ? 0 : unresolved.get("cnt", Integer.class);
                if (unresolvedCount > 0) {
                    log.warn("outbox partition {} has {} unresolved rows, skipping drop", month, unresolvedCount);
                    continue;
                }
                tx.execute("DROP TABLE IF EXISTS public.outbox_" + month);
                tx.deleteFrom(table("outbox_partition"))
                        .where(field("year_month").eq(month))
                        .execute();
                log.info("outbox partition {} dropped", month);
            }
            return null;
        });
    }
}
