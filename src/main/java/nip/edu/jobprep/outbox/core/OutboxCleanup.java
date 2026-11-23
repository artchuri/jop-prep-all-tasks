package nip.edu.jobprep.outbox.core;
import lombok.extern.slf4j.Slf4j;

import nip.edu.jobprep.model.GlobalValues;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.YearMonth;

/**
 * Nightly retention job: asks the partition repository to drop partitions older
 * than the retention window. Holds no SQL and no transaction of its own - the
 * repository owns both, so the decision of what is safe to drop stays next to
 * the statements that perform the drop.
 */
@Component
@Slf4j
public class OutboxCleanup {

    private final OutboxPartitionRepository partitionRepository;

    public OutboxCleanup(OutboxPartitionRepository partitionRepository) {
        this.partitionRepository = partitionRepository;
    }

    @Scheduled(cron = "${outbox.cleanup.cron:0 0 3 * * *}")
    public void cleanup() {
        String cutoff = YearMonth.now(GlobalValues.ZONE_UTC)
                .minusMonths(GlobalValues.RETENTION_MONTHS)
                .format(GlobalValues.YEAR_MONTH_FORMAT);

        OutboxPartitionRepository.PartitionCleanupReport report =
                partitionRepository.dropPartitionsOlderThan(cutoff);

        report.skippedUnresolved().forEach((month, unresolved) ->
                log.warn("outbox partition {} has {} unresolved rows, skipping drop", month, unresolved));
        if (!report.dropped().isEmpty()) {
            log.info("outbox partitions dropped: {}", report.dropped());
        } else if (report.skippedUnresolved().isEmpty()) {
            log.debug("no outbox partitions older than {} to drop", cutoff);
        }
    }
}
