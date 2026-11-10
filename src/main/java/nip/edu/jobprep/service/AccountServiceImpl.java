package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.outbox.event.AccountCacheInvalidationRequested;
import nip.edu.jobprep.outbox.event.AccountFrozenPayload;
import nip.edu.jobprep.outbox.event.DomainEvent;
import nip.edu.jobprep.outbox.core.OutboxService;
import nip.edu.jobprep.repositories.AccountCacheService;
import nip.edu.jobprep.repositories.AccountHistoryRepository;
import nip.edu.jobprep.repositories.AccountRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
public class AccountServiceImpl implements AccountService {

    private static final int WAIT_ATTEMPTS = 3;
    private static final long WAIT_PAUSE_MS = 20;

    private final AccountRepository accountRepository;
    private final AccountHistoryRepository historyRepository;
    private final AccountCacheService accountCacheService;
    private final OutboxService outboxService;
    private final ApplicationEventPublisher events;

    public AccountServiceImpl(AccountRepository accountRepository, AccountHistoryRepository historyRepository,
                              AccountCacheService accountCacheService, OutboxService outboxService,
                              ApplicationEventPublisher events) {
        this.accountRepository = accountRepository;
        this.historyRepository = historyRepository;
        this.accountCacheService = accountCacheService;
        this.outboxService = outboxService;
        this.events = events;
    }

    // region freeze (write path: PG under row lock, cache never used)

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
                        updated.getVersion(), changedByUserId, now),
                now);

        // Tier 1: delivered by the AFTER_COMMIT listener after this transaction commits.
        events.publishEvent(new AccountCacheInvalidationRequested(accountId, updated.getVersion()));

        return toResponse(updated);
    }

    // endregion

    // region get (read path: cache-aside with lease + stale, see ACC-106)

    @Override
    public AccountResponseDto get(long accountId) {
        try {
            AccountCacheService.ReadResult r = accountCacheService.read(accountId);
            return switch (r.status()) {
                case HIT, STALE -> toDto(accountId, r);
                case LEASE -> loadAndFill(accountId, r.token());
                case WAIT -> waitThenReload(accountId);
            };
        } catch (Exception redisUnavailable) {
            // Redis errors must not break reads: fall back to PostgreSQL without caching.
            return fallbackFromPg(accountId);
        }
    }

    private AccountResponseDto loadAndFill(long accountId, String token) {
        Account account = fromPg(accountId);
        try {
            accountCacheService.fill(accountId, token, account.getVersion(),
                    account.getAmount().toPlainString(), account.getStatus().name());
        } catch (Exception fillRejected) {
            // rejected fill (token void / older version / evicted): value was still fresh when read
        }
        return toDto(account);
    }

    private AccountResponseDto waitThenReload(long accountId) throws InterruptedException {
        for (int i = 0; i < WAIT_ATTEMPTS; i++) {
            Thread.sleep(WAIT_PAUSE_MS);
            AccountCacheService.ReadResult r = accountCacheService.read(accountId);
            if (r.status() == AccountCacheService.Status.HIT || r.status() == AccountCacheService.Status.STALE) {
                return toDto(accountId, r);
            }
            if (r.status() == AccountCacheService.Status.LEASE) {
                return loadAndFill(accountId, r.token());
            }
        }
        // Controlled fallback through PG; no cache write.
        return fallbackFromPg(accountId);
    }

    private AccountResponseDto fallbackFromPg(long accountId) {
        return toDto(fromPg(accountId));
    }

    private Account fromPg(long accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));
    }

    // endregion

    private AccountResponseDto toDto(Account account) {
        return new AccountResponseDto(account.getId(), account.getAmount(), account.getStatus().name(), account.getVersion());
    }

    private AccountResponseDto toDto(long accountId, AccountCacheService.ReadResult r) {
        return new AccountResponseDto(accountId, new java.math.BigDecimal(r.balance()), r.statusName(), r.version());
    }

    private FreezeAccountResponseDto toResponse(Account account) {
        return new FreezeAccountResponseDto(account.getId(), account.getStatus(), account.getStatusChangedAt());
    }
}