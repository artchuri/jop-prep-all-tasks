package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.utils.mapper.AccountResponseMapper;
import nip.edu.jobprep.outbox.event.AccountFrozenPayload;
import nip.edu.jobprep.outbox.event.DomainEvent;
import nip.edu.jobprep.outbox.core.OutboxService;
import nip.edu.jobprep.repositories.AccountHistoryRepository;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.utils.cache.CacheEvictAfterCommit;
import nip.edu.jobprep.utils.cache.LuaCache;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accountRepository;
    private final AccountHistoryRepository historyRepository;
    private final OutboxService outboxService;
    private final CacheEvictAfterCommit cacheEvict;
    private final AccountResponseMapper responseMapper;

    public AccountServiceImpl(AccountRepository accountRepository, AccountHistoryRepository historyRepository,
                              OutboxService outboxService, CacheEvictAfterCommit cacheEvict,
                              AccountResponseMapper responseMapper) {
        this.accountRepository = accountRepository;
        this.historyRepository = historyRepository;
        this.outboxService = outboxService;
        this.cacheEvict = cacheEvict;
        this.responseMapper = responseMapper;
    }

    // region freeze

    @Override
    @Transactional(rollbackFor = AccountNotFoundException.class)
    public FreezeAccountResponseDto freeze(long accountId, long changedByUserId) {
        Account account = accountRepository.findByIdForUpdate(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));

        if (account.getStatus() == AccountStatus.CLOSED) throw new AccountStatusConflictException("closed account cannot frozen");

        // Idempotent replay: the account is already FROZEN, the version is unchanged,
        // so the cached snapshot cannot be stale in a way a re-evict would fix -
        // no invalidation needed (same reasoning as the transfer replay path).
        if (account.getStatus() == AccountStatus.FROZEN) return responseMapper.toResponse(account);

        Instant now = Instant.now();
        Account updated = accountRepository.updateStatus(accountId, account.getVersion(), AccountStatus.FROZEN, now);
        historyRepository.save(accountId, account.getStatus(), AccountStatus.FROZEN, changedByUserId, now);

        outboxService.publish(
                DomainEvent.ACCOUNT_FROZEN,
                account.getUser().getId(),
                accountId,
                new AccountFrozenPayload(accountId, account.getStatus().name(), AccountStatus.FROZEN.name(),
                        updated.getVersion(), changedByUserId, now),
                now);

        cacheEvict.evict(accountId, updated.getVersion());

        return responseMapper.toResponse(updated);
    }

    // endregion

    //region get
    @Override
    @Cacheable(cacheNames = LuaCache.NAME, key = "#accountId", sync = true)
    public AccountResponseDto get(long accountId) {
        var account = accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));
        return new AccountResponseDto(account.getId(), account.getAmount(), account.getStatus().name(), account.getVersion());
    }

    // endregion
}