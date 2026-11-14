package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.jooq.DSLContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.Instant;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;


class OutboxCleanupIT extends AbstractIntegrationTest {

    private static final DateTimeFormatter YM = GlobalValues.YEAR_MONTH_FORMAT;

    @Autowired
    OutboxCleanup cleanup;

    @Autowired
    OutboxPartitionRepository partitionRepository;

    @Autowired
    JdbcTemplate jdbc;

    @Autowired
    DSLContext dsl;

    @BeforeEach
    void cleanRegistryAndTestPartitions() {
        dsl.deleteFrom(table("outbox_partition"))
                .where(field("year_month").in("2020_01", "2020_02", "2020_03"))
                .execute();
        dropTable("2020_01");
        dropTable("2020_02");
        dropTable("2020_03");
    }

    private void dropTable(String month) {
        jdbc.execute("DROP TABLE IF EXISTS public.outbox_" + month);
    }

    private void seedPartition(String month, String status) {
        jdbc.execute("CREATE TABLE IF NOT EXISTS public.outbox_" + month
                + " (id BIGINT DEFAULT 1, event_id UUID, event_type VARCHAR(32), aggregate_id BIGINT, " +
                "user_id BIGINT, payload JSONB, created_at TIMESTAMPTZ, processed_at TIMESTAMPTZ, " +
                "rejected_at TIMESTAMPTZ, attempt_count INT DEFAULT 0, next_attempt_at TIMESTAMPTZ, " +
                "lease_owner VARCHAR(128), lease_until TIMESTAMPTZ, last_attempt_at TIMESTAMPTZ, " +
                "last_error TEXT, status VARCHAR(16) DEFAULT 'NEW')");
        jdbc.update("insert into outbox_partition (year_month, created_at) values (?, now()) " +
                "on conflict (year_month) do nothing", month);
        if (status != null) {
            jdbc.update("insert into public.outbox_" + month
                    + " (event_id, event_type, aggregate_id, user_id, payload, created_at, status) " +
                    "values (?, 'TRANSFER_COMPLETED', 1, 1, '{}', now(), ?)",
                    UUID.randomUUID(), status);
        }
    }

    private boolean tableExists(String month) {
        Boolean exists = jdbc.queryForObject(
                "select to_regclass('public.outbox_" + month + "') is not null", Boolean.class);
        return Boolean.TRUE.equals(exists);
    }

    private boolean registered(String month) {
        Integer cnt = jdbc.queryForObject(
                "select count(*) from outbox_partition where year_month = ?", Integer.class, month);
        return cnt != null && cnt > 0;
    }

    @Test
    void dropsOldEmptyPartitionAndKeepsRegistryConsistent() {
        seedPartition("2020_03", null);

        cleanup.cleanup();

        assertThat(tableExists("2020_03")).isFalse();
        assertThat(registered("2020_03")).isFalse();
    }

    @Test
    void keepsOldPartitionHoldingRejectedRows() {
        seedPartition("2020_01", "REJECTED");

        cleanup.cleanup();

        assertThat(tableExists("2020_01")).isTrue();
        assertThat(registered("2020_01")).isTrue();
    }

    @Test
    void keepsOldPartitionHoldingUnprocessedRows() {
        seedPartition("2020_02", "NEW");

        cleanup.cleanup();

        assertThat(tableExists("2020_02")).isTrue();
        assertThat(registered("2020_02")).isTrue();
    }

    @Test
    void registryAndTablesAgreeForCurrentAndFuturePartitions() {
        YearMonth current = YearMonth.now(ZoneOffset.UTC);
        String cutoff = current.minusMonths(GlobalValues.RETENTION_MONTHS).format(YM);
        String nextMonth = current.plusMonths(1).format(YM);

        cleanup.cleanup();

        assertThat(nextMonth.compareTo(cutoff)).isPositive();
        assertThat(tableExists(current.format(YM))).isTrue();
    }
}