package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import org.jooq.DSLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;

import static org.jooq.impl.DSL.field;
import static org.jooq.impl.DSL.table;


@Component
public class OutboxReclaimer {

    private static final Logger log = LoggerFactory.getLogger(OutboxReclaimer.class);

    static final Duration REDRIVE_AFTER = Duration.ofMinutes(5);

    private final DSLContext dsl;
    private final OutboxMetrics metrics;

    public OutboxReclaimer(DSLContext dsl, OutboxMetrics metrics) {
        this.dsl = dsl;
        this.metrics = metrics;
    }

    @Scheduled(fixedDelayString = "${outbox.reclaimer.interval-ms:60000}")
    public void redrive() {
        int recovered = dsl.transactionResult(cfg ->
                cfg.dsl().update(table("outbox"))
                        .set(field("status"), GlobalValues.STATUS_NEW)
                        .set(field("attempt_count"), 0)
                        .setNull(field("next_attempt_at"))
                        .setNull(field("rejected_at"))
                        .setNull(field("last_error"))
                        .where(field("status").eq(GlobalValues.STATUS_REJECTED))
                        .and(field("rejected_at").le(Instant.now().minus(REDRIVE_AFTER)))
                        .execute());
        if (recovered > 0) {
            metrics.recovered(recovered);
            log.warn("outbox re-drive: {} REJECTED row(s) returned to NEW after {} quiet period",
                    recovered, REDRIVE_AFTER);
        }
    }
}
