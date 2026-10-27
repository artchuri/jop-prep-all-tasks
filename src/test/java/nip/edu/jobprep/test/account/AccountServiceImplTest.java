package nip.edu.jobprep.test.account;

import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.User;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.repositories.AccountHistoryRepository;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.repositories.UserRepository;
import nip.edu.jobprep.service.AccountServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class AccountServiceImplTest {

    @InjectMocks
    private AccountServiceImpl accountService;

    @Mock
    private AccountRepository accountRepository;

    @Mock
    private AccountHistoryRepository historyRepository;

    @Mock
    private UserRepository userRepository;

    @Captor
    private ArgumentCaptor<Instant> changedAtCaptor;

    private Account account(AccountStatus status) {
        return Account.builder()
                .id(1L)
                .user(User.builder()
                        .id(0L)
                        .version(0L)
                        .createdAt(Instant.now())
                        .build())
                .createdAt(Instant.now())
                .status(status)
                .statusChangedAt(Instant.now())
                .version(0L)
                .updatedAt(Instant.now())
                .build();
    }

    @Test
    void freezeAccount() {
        when(accountRepository.findByIdForUpdate(1L))
                .thenReturn(Optional.of(account(AccountStatus.ACTIVE)));
        when(accountRepository.updateStatus(eq(1L), eq(0L), eq(AccountStatus.FROZEN), any()))
                .thenReturn(account(AccountStatus.FROZEN));

        FreezeAccountResponseDto dto = accountService.freeze(1L, 0L);

        assertThat(dto.status()).isEqualTo(AccountStatus.FROZEN);
        verify(historyRepository).save(eq(1L), eq(AccountStatus.ACTIVE),
                eq(AccountStatus.FROZEN), eq(0L), any());
    }

    @Test
    void tryFreezeAccountSecondAttempt(){
        when(accountRepository.findByIdForUpdate(1L))
                .thenReturn(Optional.of(account(AccountStatus.FROZEN)));

        FreezeAccountResponseDto dto = accountService.freeze(1L, 0L);

        verify(accountRepository, never()).updateStatus(anyLong(), anyLong(), any(), any());
    }

    @Test
    void rejectFreezeWhenAccountIsClosed(){
        when(accountRepository.findByIdForUpdate(1L))
                .thenReturn(Optional.of(account(AccountStatus.CLOSED)));

        assertThatThrownBy(() -> accountService.freeze(1L, 0L)).isInstanceOf(AccountStatusConflictException.class);

        verify(accountRepository, never()).updateStatus(anyLong(), anyLong(), any(), any());

    }

    @Test
    void checkInstantTimeIsEquals() {
        when(accountRepository.findByIdForUpdate(42L))
                .thenReturn(Optional.of(account(AccountStatus.ACTIVE)));
        when(accountRepository.updateStatus(anyLong(), anyLong(), any(), any()))
                .thenReturn(account(AccountStatus.FROZEN));

        accountService.freeze(42L, 7L);

        verify(accountRepository).updateStatus(anyLong(), anyLong(), any(), changedAtCaptor.capture());
        verify(historyRepository).save(anyLong(), any(), any(), anyLong(), changedAtCaptor.capture());

        assertThat(changedAtCaptor.getAllValues().get(0))
                .isEqualTo(changedAtCaptor.getAllValues().get(1));
    }
}