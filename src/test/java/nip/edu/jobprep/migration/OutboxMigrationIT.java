package nip.edu.jobprep.migration;

import liquibase.Contexts;
import liquibase.Liquibase;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.sql.DriverManager;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers
class OutboxMigrationIT {
    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:17-alpine");

    @Test
    void preservesLegacyRowsWhenCreatingPartitionedOutbox() throws Exception {
        try (var connection = DriverManager.getConnection(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())) {
            try (var statement = connection.createStatement()) {
                statement.execute("CREATE TABLE public.outbox (id BIGINT PRIMARY KEY, event_id UUID NOT NULL UNIQUE, event_type VARCHAR(32) NOT NULL, aggregate_id BIGINT NOT NULL, user_id BIGINT NOT NULL, payload JSONB NOT NULL, created_at TIMESTAMP WITH TIME ZONE NOT NULL, processed_at TIMESTAMP WITH TIME ZONE, attempt_count INT NOT NULL DEFAULT 0)");
                statement.execute("INSERT INTO public.outbox (id,event_id,event_type,aggregate_id,user_id,payload,created_at,attempt_count) VALUES (7,'" + UUID.randomUUID() + "','TRANSFER_COMPLETED',11,42,'{\"amount\":\"10.00\"}',now(),2)");
                statement.execute("INSERT INTO public.outbox (id,event_id,event_type,aggregate_id,user_id,payload,created_at,processed_at,attempt_count) VALUES (8,'" + UUID.randomUUID() + "','ACCOUNT_FROZEN',12,42,'{}',now(),now(),4)");
            }
            var database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            var liquibase = new Liquibase("db/changelog/2026/11/01-01-changelog.xml", new ClassLoaderResourceAccessor(), database);
            liquibase.update(new Contexts());
            liquibase.update(new Contexts());
            try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT id,status,attempt_count FROM public.outbox ORDER BY id")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getLong("id")).isEqualTo(7);
                assertThat(result.getString("status")).isEqualTo("NEW");
                assertThat(result.getInt("attempt_count")).isEqualTo(2);
                assertThat(result.next()).isTrue();
                assertThat(result.getLong("id")).isEqualTo(8);
                assertThat(result.getString("status")).isEqualTo("COMPLETED");
                assertThat(result.getInt("attempt_count")).isEqualTo(4);
                assertThat(result.next()).isFalse();
            }
            try (var statement = connection.createStatement(); var result = statement.executeQuery("SELECT to_regclass('public.outbox_legacy')")) {
                assertThat(result.next()).isTrue();
                assertThat(result.getString(1)).isEqualTo("outbox_legacy");
            }
            try (var statement = connection.createStatement()) {
                try (var result = statement.executeQuery("SELECT nextval(pg_get_serial_sequence('public.outbox', 'id'))")) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getLong(1)).isEqualTo(9);
                }
                try (var result = statement.executeQuery("SELECT count(*) FROM public.outbox_legacy")) {
                    assertThat(result.next()).isTrue();
                    assertThat(result.getInt(1)).isEqualTo(2);
                }
            }
        }
    }
}
