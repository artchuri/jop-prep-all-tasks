package nip.edu.jobprep.test.transfer;

import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.TransferHistory;
import nip.edu.jobprep.entity.User;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.exceptions.InsufficientFundsException;
import nip.edu.jobprep.exceptions.InvalidTransferException;
import nip.edu.jobprep.exceptions.TransferForbiddenException;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.repositories.TransferRepository;
import nip.edu.jobprep.service.TransferServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.OngoingStubbing;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TransferServiceImplTest {

    private static final String KEY = "11111111-1111-1111-1111-111111111111";
    private static final Instant NOW = Instant.parse("2026-10-25T10:00:00Z");

    @InjectMocks
    private TransferServiceImpl transferService;

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private TransferRepository transferRepository;

    @Test
    void shouldTransferSuccessfully_whenAccountsAreActive() {
        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));
        stubInsert().thenReturn(history(10, 1, 2, "10.55"));

        TransferResponseDto response = transferService.transfer(KEY, request(1, 2, 1, "10.555"));

        assertThat(response.transferId()).isEqualTo(10L);
        assertThat(response.fromAccountId()).isEqualTo(1L);
        assertThat(response.recipientAccountId()).isEqualTo(2L);
        assertThat(response.amount()).isEqualByComparingTo("10.55");
        assertThat(response.status()).isEqualTo("COMPLETED");
        verify(accountRepository).updateAmount(eq(1L), eq(0L), eq(new BigDecimal("89.45")), any());
        verify(accountRepository).updateAmount(eq(2L), eq(0L), eq(new BigDecimal("10.55")), any());
        verify(transferRepository).insertIfAbsent(eq(1L), eq(2L), eq(new BigDecimal("10.55")),
                any(), eq("COMPLETED"), eq(KEY), eq("RUB"));
    }

    @Test
    void shouldThrow400_whenAmountIsInvalidOrSameAccount() {
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 1, 1, "10")))
                .isInstanceOf(InvalidTransferException.class);
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "0")))
                .isInstanceOf(InvalidTransferException.class);
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "-5")))
                .isInstanceOf(InvalidTransferException.class);
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "0.001")))
                .isInstanceOf(InvalidTransferException.class);
        verifyNoInteractions(accountRepository, transferRepository);
    }

    @Test
    void shouldThrow403_whenFromAccountBelongsToAnotherUser() {
        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));

        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 2, "10")))
                .isInstanceOf(TransferForbiddenException.class);
        verify(transferRepository, never()).insertIfAbsent(anyLong(), anyLong(), any(), any(), any(), any(), any());
    }

    @Test
    void shouldThrow409_whenSenderIsClosedOrFrozen() {
        mockAccounts(account(1, 1, AccountStatus.CLOSED, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));
        stubInsert().thenReturn(history(10, 1, 2, "10.00"));
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "10")))
                .isInstanceOf(AccountStatusConflictException.class);

        mockAccounts(account(1, 1, AccountStatus.FROZEN, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));
        stubInsert().thenReturn(history(11, 1, 2, "10.00"));
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "10")))
                .isInstanceOf(AccountStatusConflictException.class);

        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.CLOSED, "0.00"));
        stubInsert().thenReturn(history(12, 1, 2, "10.00"));
        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "10")))
                .isInstanceOf(AccountStatusConflictException.class);

        verify(accountRepository, never()).updateAmount(anyLong(), anyLong(), any(), any());
    }

    @Test
    void shouldAllowTransferToFrozenRecipient() {
        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.FROZEN, "0.00"));
        stubInsert().thenReturn(history(10, 1, 2, "10.00"));

        TransferResponseDto response = transferService.transfer(KEY, request(1, 2, 1, "10"));

        assertThat(response.transferId()).isEqualTo(10L);
        verify(accountRepository).updateAmount(eq(1L), eq(0L), eq(new BigDecimal("90.00")), any());
        verify(accountRepository).updateAmount(eq(2L), eq(0L), eq(new BigDecimal("10.00")), any());
    }

    @Test
    void shouldThrow422_whenInsufficientFunds() {
        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));
        stubInsert().thenReturn(history(10, 1, 2, "101.00"));

        assertThatThrownBy(() -> transferService.transfer(KEY, request(1, 2, 1, "101")))
                .isInstanceOf(InsufficientFundsException.class);
        verify(accountRepository, never()).updateAmount(anyLong(), anyLong(), any(), any());
    }

    @Test
    void shouldReturnCachedResult_whenIdempotencyKeyIsRepeatedSequentially() {
        mockAccounts(account(1, 1, AccountStatus.ACTIVE, "100.00"),
                account(2, 1, AccountStatus.ACTIVE, "0.00"));
        stubInsert().thenReturn(history(10, 1, 2, "10.55")).thenReturn(null);
        when(transferRepository.findByIdempotencyKey(KEY))
                .thenReturn(Optional.of(history(10, 1, 2, "10.55")));

        TransferResponseDto first = transferService.transfer(KEY, request(1, 2, 1, "10.555"));
        TransferResponseDto second = transferService.transfer(KEY, request(1, 2, 1, "10.555"));

        assertThat(second.transferId()).isEqualTo(first.transferId());
        verify(accountRepository).updateAmount(eq(1L), eq(0L), eq(new BigDecimal("89.45")), any());
        verify(accountRepository).updateAmount(eq(2L), eq(0L), eq(new BigDecimal("10.55")), any());
    }

    private void mockAccounts(Account from, Account to) {
        when(accountRepository.findByIdForUpdate(from.getId())).thenReturn(Optional.of(from));
        when(accountRepository.findByIdForUpdate(to.getId())).thenReturn(Optional.of(to));
    }

    private OngoingStubbing<TransferHistory> stubInsert() {
        return when(transferRepository.insertIfAbsent(anyLong(), anyLong(), any(), any(), any(), any(), any()));
    }

    private TransferRequestDto request(long fromId, long toId, long userId, String amount) {
        return new TransferRequestDto(toId, userId, fromId, new BigDecimal(amount));
    }

    private Account account(long id, long userId, AccountStatus status, String amount) {
        return Account.builder()
                .id(id)
                .user(User.builder().id(userId).build())
                .status(status)
                .amount(new BigDecimal(amount))
                .currency("RUB")
                .createdAt(NOW)
                .updatedAt(NOW)
                .statusChangedAt(NOW)
                .version(0L)
                .build();
    }

    private TransferHistory history(long id, long fromId, long toId, String amount) {
        return TransferHistory.builder()
                .id(id)
                .fromAccountId(fromId)
                .recipientAccountId(toId)
                .amount(new BigDecimal(amount))
                .createdAt(NOW)
                .status("COMPLETED")
                .idempotencyKey(KEY)
                .currency("RUB")
                .build();
    }
}
