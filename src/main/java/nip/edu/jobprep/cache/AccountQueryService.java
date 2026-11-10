package nip.edu.jobprep.cache;

import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.repositories.AccountRepository;
import org.springframework.stereotype.Service;


@Service
public class AccountQueryService {

    private static final int WAIT_ATTEMPTS = 3;
    private static final long WAIT_PAUSE_MS = 20;

    private final AccountCacheService cache;
    private final AccountRepository accountRepository;

    public AccountQueryService(AccountCacheService cache, AccountRepository accountRepository) {
        this.cache = cache;
        this.accountRepository = accountRepository;
    }

    public AccountResponseDto get(long accountId) {
        try {
            AccountCacheService.ReadResult r = cache.read(accountId);
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
            cache.fill(accountId, token, account.getVersion(),
                    account.getAmount().toPlainString(), account.getStatus().name());
        } catch (Exception fillRejected) {
            // rejected fill (token void / older version / evicted): value was still fresh when read
        }
        return toDto(account);
    }

    private AccountResponseDto waitThenReload(long accountId) throws InterruptedException {
        for (int i = 0; i < WAIT_ATTEMPTS; i++) {
            Thread.sleep(WAIT_PAUSE_MS);
            AccountCacheService.ReadResult r = cache.read(accountId);
            if (r.status() == AccountCacheService.Status.HIT || r.status() == AccountCacheService.Status.STALE) {
                return toDto(accountId, r);
            }
            if (r.status() == AccountCacheService.Status.LEASE) {
                return loadAndFill(accountId, r.token());
            }
        }
        // Controllled fallback through bulkhead-limited PG path; no cache write.
        return fallbackFromPg(accountId);
    }

    private AccountResponseDto fallbackFromPg(long accountId) {
        return toDto(fromPg(accountId));
    }

    private Account fromPg(long accountId) {
        return accountRepository.findById(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));
    }

    private AccountResponseDto toDto(Account account) {
        return new AccountResponseDto(account.getId(), account.getAmount(), account.getStatus().name(), account.getVersion());
    }

    private AccountResponseDto toDto(long accountId, AccountCacheService.ReadResult r) {
        return new AccountResponseDto(accountId, new java.math.BigDecimal(r.balance()), r.statusName(), r.version());
    }
}