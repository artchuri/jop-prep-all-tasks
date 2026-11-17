package nip.edu.jobprep.model;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.regex.Pattern;

public final class GlobalValues {

    /** One zone for every partition boundary and retention cutoff: mixed zones would shift months. */
    public static final java.time.ZoneOffset ZONE_UTC = java.time.ZoneOffset.UTC;
    public static final DateTimeFormatter YEAR_MONTH_FORMAT = DateTimeFormatter.ofPattern("yyyy_MM");
    public static final Pattern PARTITION_NAME = Pattern.compile("\\d{4}_\\d{2}");
    public static final String DDL_LOCK = "outbox_partition_ddl";
    public static final String DEFAULT_PARTITION = "outbox_default";
    public static final List<String> OUTBOX_COLUMNS = List.of(
            "id", "event_id", "event_type", "aggregate_id", "user_id",
            "payload", "created_at", "processed_at", "rejected_at", "attempt_count", "status",
            "lease_owner", "lease_until", "next_attempt_at", "last_attempt_at", "last_error");

    public static final String STATUS_NEW = "NEW";
    public static final String STATUS_PROCESSING = "PROCESSING";
    public static final String STATUS_COMPLETED = "COMPLETED";
    public static final String STATUS_REJECTED = "REJECTED";
    public static final int RETENTION_MONTHS = 1;
    public static final int MAX_ATTEMPTS = 5;
    public static final Duration MAX_BACKOFF = Duration.ofSeconds(300);
    public static final int BATCH_SIZE = 100;
    public static final Duration LEASE = Duration.ofSeconds(30);
    public static final Duration SEND_TIMEOUT = Duration.ofSeconds(10);
    public static final String KAFKA_TOPIC = "account-events";

    public static final String METRIC_TAG_UNKNOWN = "unknown";

    public static final String TRANSFER_STATUS_COMPLETED = "COMPLETED";

    private GlobalValues() {
    }
}