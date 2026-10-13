package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.repositories.AccountHistoryRepository;
import nip.edu.jobprep.repositories.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class AccountServiceImpl implements AccountService {
    private final AccountRepository accountRepository;
    private final AccountHistoryRepository historyRepository;

    public AccountServiceImpl(AccountRepository accountRepository, AccountHistoryRepository historyRepository) {
        this.accountRepository = accountRepository;
        this.historyRepository = historyRepository;
    }

    @Override
    @Transactional(rollbackFor = AccountNotFoundException.class)
    public FreezeAccountResponseDto freeze(long accountId, long changedByUserId) {
        Account account = accountRepository.findByIdForUpdate(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account "+accountId+" not found"));

        if (account.getStatus() == AccountStatus.CLOSED) throw new AccountStatusConflictException("closed account cannot frozen");

        if (account.getStatus() == AccountStatus.FROZEN) return toResponse(account);

        Instant now = Instant.now();
        Account updated = accountRepository.updateStatus(accountId, account.getVersion(), AccountStatus.FROZEN, now);
        historyRepository.save(accountId, account.getStatus(), AccountStatus.FROZEN, changedByUserId, now);

        return toResponse(updated);
    }

    private FreezeAccountResponseDto toResponse(Account account) {
        return new FreezeAccountResponseDto(account.getId(), account.getStatus(), account.getStatusChangedAt());
    }
}
