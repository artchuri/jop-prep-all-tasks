package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import org.jooq.DSLContext;
import org.jooq.impl.DSL;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.time.YearMonth;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;

@Repository
public class OutboxPartitionRepository {

    private final DSLContext dsl;

    public OutboxPartitionRepository(DSLContext dsl) {
        this.dsl = dsl;
    }

    public boolean exists(String name) {
        requireValidPartitionName(name);
        return dsl.fetchValue("select to_regclass('public.outbox_" + name + "')") != null;
    }

    /** Registry names are interpolated into DDL - the regex check must sit next to every interpolation. */
    private static void requireValidPartitionName(String name) {
        if (!GlobalValues.PARTITION_NAME.matcher(name).matches()) {
            throw new IllegalStateException("unexpected outbox partition name: " + name);
        }
    }

    /**
     * Moves the default partition aside, creates a real month partition, and
     * re-homes rows that landed in the default partition for that month. Takes
     * a typed {@link YearMonth} so the DDL identifiers are derived internally -
     * there is no string input that could carry an injection payload.
     */
    public void createDefaultMigration(YearMonth month) {
        String name = month.format(GlobalValues.YEAR_MONTH_FORMAT);
        String from = month.atDay(1).toString();
        String to = month.plusMonths(1).atDay(1).toString();
        dsl.execute("ALTER TABLE public.outbox DETACH PARTITION " + GlobalValues.DEFAULT_PARTITION);
        try {
            dsl.execute("CREATE TABLE public.outbox_" + name
                    + " PARTITION OF public.outbox FOR VALUES FROM ('" + from + "') TO ('" + to + "')");
            dsl.execute("WITH moved AS ("
                    + "DELETE FROM public." + GlobalValues.DEFAULT_PARTITION
                    + " WHERE created_at >= '" + from + "' AND created_at < '" + to + "'"
                    + " RETURNING " + String.join(", ", GlobalValues.OUTBOX_COLUMNS) + ") "
                    + "INSERT INTO public.outbox (" + String.join(", ", GlobalValues.OUTBOX_COLUMNS) + ") SELECT " + String.join(", ", GlobalValues.OUTBOX_COLUMNS) + " FROM moved");
            dsl.execute("ALTER TABLE outbox ATTACH PARTITION " + GlobalValues.DEFAULT_PARTITION + " DEFAULT");
        } catch (RuntimeException e) {
            dsl.execute("ALTER TABLE outbox ATTACH PARTITION " + GlobalValues.DEFAULT_PARTITION + " DEFAULT");
            throw e;
        }
    }


    public void lockPartitionDdl(DSLContext tx) {
        tx.execute("SELECT pg_advisory_xact_lock(hashtext(?))", GlobalValues.DDL_LOCK);
    }


    public PartitionCleanupReport dropPartitionsOlderThan(String cutoff) {
        return dsl.transactionResult(cfg -> {
            DSLContext tx = cfg.dsl();
            lockPartitionDdl(tx);
            tx.execute("LOCK TABLE outbox IN ACCESS EXCLUSIVE MODE");

            List<String> months = tx.select(field("year_month", String.class))
                    .from(table("outbox_partition"))
                    .where(field("year_month").lt(cutoff))
                    .orderBy(field("year_month"))
                    .fetch(field("year_month", String.class));

            List<String> dropped = new ArrayList<>();
            Map<String, Integer> skipped = new LinkedHashMap<>();
            for (String month : months) {
                if (!GlobalValues.PARTITION_NAME.matcher(month).matches()) {
                    throw new IllegalStateException("unexpected outbox partition name: " + month);
                }
                String unresolved = "select count(*) from public.outbox_" + month
                        + " where status in ('NEW', 'PROCESSING', 'REJECTED')";
                Integer unresolvedCount = tx.fetchOne(unresolved).get(0, Integer.class);
                if (unresolvedCount != null && unresolvedCount > 0) {
                    skipped.put(month, unresolvedCount);
                    continue;
                }
                tx.execute("DROP TABLE IF EXISTS public.outbox_" + month);
                tx.deleteFrom(table("outbox_partition"))
                        .where(field("year_month").eq(month))
                        .execute();
                dropped.add(month);
            }
            return new PartitionCleanupReport(dropped, skipped);
        });
    }


    public record PartitionCleanupReport(List<String> dropped, Map<String, Integer> skippedUnresolved) {
    }

    public void register(String name) {
        dsl.insertInto(table("outbox_partition"))
                .set(field("year_month"), name)
                .set(field("created_at"), Instant.now())
                .onConflictDoNothing()
                .execute();
    }
}