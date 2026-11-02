package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.outbox.AccountFrozenPayload;
import nip.edu.jobprep.outbox.DomainEvent;
import nip.edu.jobprep.outbox.OutboxService;
import nip.edu.jobprep.repositories.AccountHistoryRepository;
import nip.edu.jobprep.repositories.AccountRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class AccountServiceImpl implements AccountService {
    private final AccountRepository accountRepository;
    private final AccountHistoryRepository historyRepository;
    private final OutboxService outboxService;

    public AccountServiceImpl(AccountRepository accountRepository, AccountHistoryRepository historyRepository,
                              OutboxService outboxService) {
        this.accountRepository = accountRepository;
        this.historyRepository = historyRepository;
        this.outboxService = outboxService;
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

        outboxService.publish(
                DomainEvent.ACCOUNT_FROZEN,
                account.getUser().getId(),
                accountId,
                new AccountFrozenPayload(accountId, account.getStatus().name(), AccountStatus.FROZEN.name(),
                        changedByUserId, now),
                now);

        return toResponse(updated);
    }

    private FreezeAccountResponseDto toResponse(Account account) {
        return new FreezeAccountResponseDto(account.getId(), account.getStatus(), account.getStatusChangedAt());
    }
}
