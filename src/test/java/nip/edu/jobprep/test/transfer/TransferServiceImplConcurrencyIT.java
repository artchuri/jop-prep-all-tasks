package nip.edu.jobprep.test.transfer;

import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.InsufficientFundsException;
import nip.edu.jobprep.service.TransferService;
import nip.edu.jobprep.test.integration.AbstractIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.assertj.core.api.Assertions.assertThat;

class TransferServiceImplConcurrencyIT extends AbstractIntegrationTest {

    private static final long USER_ID = 1L;
    private static final long FIRST_ID = 1L;
    private static final long SECOND_ID = 2L;

    @Autowired
    private TransferService transferService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @BeforeEach
    void cleanData() {
        jdbcTemplate.update("delete from transfers_history");
        jdbcTemplate.update("delete from account_status_history");
        jdbcTemplate.update("delete from accounts");
        jdbcTemplate.update("delete from users");
        jdbcTemplate.update("insert into users (id, created_at, version) values (?, now(), 0)", USER_ID);
    }

    @Test
    void shouldPreventDoubleSpend_whenConcurrentTransfers() throws Exception {
        account(FIRST_ID, "100.00");
        account(SECOND_ID, "0.00");

        List<Object> results = concurrently(
                call(key(), FIRST_ID, SECOND_ID, "80"),
                call(key(), FIRST_ID, SECOND_ID, "80"));

        assertThat(results).filteredOn(TransferResponseDto.class::isInstance).hasSize(1);
        assertThat(results).filteredOn(InsufficientFundsException.class::isInstance).hasSize(1);
        assertThat(balance(FIRST_ID)).isEqualByComparingTo("20.00");
        assertThat(balance(SECOND_ID)).isEqualByComparingTo("80.00");
        assertThat(historyCount()).isEqualTo(1);
    }

    @Test
    void shouldNotDeadlock_whenConcurrentTransfersInOppositeDirections() throws Exception {
        account(FIRST_ID, "100.00");
        account(SECOND_ID, "100.00");

        List<Object> results = concurrently(
                call(key(), FIRST_ID, SECOND_ID, "10"),
                call(key(), SECOND_ID, FIRST_ID, "10"));

        assertThat(results).allSatisfy(result -> assertThat(result).isInstanceOf(TransferResponseDto.class));
        assertThat(balance(FIRST_ID)).isEqualByComparingTo("100.00");
        assertThat(balance(SECOND_ID)).isEqualByComparingTo("100.00");
        assertThat(historyCount()).isEqualTo(2);
    }

    @Test
    void shouldHandleIdempotencyRace_whenSameKeySentConcurrently() throws Exception {
        account(FIRST_ID, "100.00");
        account(SECOND_ID, "0.00");
        String idempotencyKey = key();

        List<Object> results = concurrently(
                call(idempotencyKey, FIRST_ID, SECOND_ID, "10"),
                call(idempotencyKey, FIRST_ID, SECOND_ID, "10"));

        assertThat(results).allSatisfy(result -> assertThat(result).isInstanceOf(TransferResponseDto.class));
        TransferResponseDto first = (TransferResponseDto) results.get(0);
        TransferResponseDto second = (TransferResponseDto) results.get(1);
        assertThat(second.transferId()).isEqualTo(first.transferId());
        assertThat(balance(FIRST_ID)).isEqualByComparingTo("90.00");
        assertThat(balance(SECOND_ID)).isEqualByComparingTo("10.00");
        assertThat(historyCount()).isEqualTo(1);
    }

    private List<Object> concurrently(Supplier<Object> first, Supplier<Object> second) throws Exception {
        try (var executor = Executors.newFixedThreadPool(2)) {
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            var firstFuture = executor.submit(() -> {
                ready.countDown();
                start.await();
                return first.get();
            });
            var secondFuture = executor.submit(() -> {
                ready.countDown();
                start.await();
                return second.get();
            });
            boolean bothReady = ready.await(5, TimeUnit.SECONDS);
            start.countDown();
            assertThat(bothReady).isTrue();
            return List.of(firstFuture.get(15, TimeUnit.SECONDS), secondFuture.get(15, TimeUnit.SECONDS));
        }
    }

    private Supplier<Object> call(String idempotencyKey, long fromId, long toId, String amount) {
        return () -> {
            try {
                return transferService.transfer(idempotencyKey,
                        new TransferRequestDto(toId, USER_ID, fromId, new BigDecimal(amount)));
            } catch (Exception exception) {
                return exception;
            }
        };
    }

    private void account(long id, String amount) {
        jdbcTemplate.update("""
                insert into accounts (id, user_id, status, created_at, updated_at, status_changed_at, version, amount, currency)
                values (?, ?, ?, now(), now(), now(), 0, ?, 'RUB')
                """, id, USER_ID, AccountStatus.ACTIVE.name(), new BigDecimal(amount));
    }

    private BigDecimal balance(long accountId) {
        return jdbcTemplate.queryForObject("select amount from accounts where id = ?", BigDecimal.class, accountId);
    }

    private long historyCount() {
        return jdbcTemplate.queryForObject("select count(*) from transfers_history", Long.class);
    }

    private String key() {
        return UUID.randomUUID().toString();
    }
}
