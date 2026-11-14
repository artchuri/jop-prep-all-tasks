package nip.edu.jobprep.outbox.core;

import nip.edu.jobprep.model.GlobalValues;
import org.jooq.DSLContext;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.YearMonth;
import java.time.format.DateTimeFormatter;

@Component
public class OutboxPartitionService {

    private final DSLContext dsl;
    private final OutboxPartitionRepository partitionRepository;

    public OutboxPartitionService(DSLContext dsl, OutboxPartitionRepository partitionRepository) {
        this.dsl = dsl;
        this.partitionRepository = partitionRepository;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void initPartitions() {
        YearMonth current = YearMonth.now(GlobalValues.ZONE_UTC);
        ensurePartition(current.minusMonths(1));
        ensurePartition(current);
        ensurePartition(current.plusMonths(1));
    }

    @Scheduled(cron = "0 0 2 25 * *")
    public void ensureNextMonthPartition() {
        ensurePartition(YearMonth.now(GlobalValues.ZONE_UTC).plusMonths(1));
    }

    public void ensurePartition(YearMonth month) {
        dsl.transactionResult(cfg -> {
            partitionRepository.lockPartitionDdl(cfg.dsl());
            if (partitionRepository.exists(month.format(GlobalValues.YEAR_MONTH_FORMAT))) {
                partitionRepository.register(month.format(GlobalValues.YEAR_MONTH_FORMAT));
                return null;
            }
            partitionRepository.createDefaultMigration(month);
            partitionRepository.register(month.format(GlobalValues.YEAR_MONTH_FORMAT));
            return null;
        });
    }
}