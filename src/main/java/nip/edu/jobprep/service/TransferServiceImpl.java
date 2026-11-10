package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.TransferHistory;
import nip.edu.jobprep.exceptions.*;
import nip.edu.jobprep.outbox.AccountCacheInvalidationRequested;
import nip.edu.jobprep.outbox.DomainEvent;
import nip.edu.jobprep.outbox.OutboxService;
import nip.edu.jobprep.outbox.TransferCompletedPayload;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.repositories.TransferRepository;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;

@Service
public class TransferServiceImpl implements TransferService {
    private static final String COMPLETED = "COMPLETED";

    private final AccountRepository accountRepository;
    private final TransferRepository transferRepository;
    private final OutboxService outboxService;
    private final ApplicationEventPublisher events;

    public TransferServiceImpl(AccountRepository accountRepository, TransferRepository transferRepository,
                               OutboxService outboxService, ApplicationEventPublisher events) {
        this.accountRepository = accountRepository;
        this.transferRepository = transferRepository;
        this.outboxService = outboxService;
        this.events = events;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public TransferResponseDto transfer(String idempotencyKey, TransferRequestDto request) throws TransferForbiddenException, IdempotencyConflictException, InvalidTransferException {
        long fromId = request.fromAccountId();
        long toId = request.recipientAccountId();
        BigDecimal amount = request.amount().setScale(2, RoundingMode.DOWN);

        if (fromId == toId) {
            throw new InvalidTransferException("from and recipient accounts must be difference");
        }
        if (amount.signum() <= 0) {
            throw new InvalidTransferException("amount must be positive");
        }

        Account first = lock(Math.min(fromId, toId));
        Account second = lock(Math.max(fromId, toId));
        Account from = fromId == first.getId() ? first : second;
        Account to = fromId == first.getId() ? second : first;

        if (!from.getUser().getId().equals(request.userId())) {
            throw new TransferForbiddenException("account " + fromId + " does not belong to user " + request.userId());
        }

        Instant now = Instant.now();
        TransferHistory history = transferRepository.insertIfAbsent(fromId, toId, amount, now,
                COMPLETED, idempotencyKey, from.getCurrency());
        if (history == null) {
            TransferHistory existing = transferRepository.findByIdempotencyKey(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException("transfer " + idempotencyKey + " not found"));
            boolean same = existing.getAmount().compareTo(amount) == 0
                    && existing.getFromAccountId() == fromId
                    && existing.getRecipientAccountId() == toId;
            if (!same){
                throw new IdempotencyConflictException(
                        "idempotency key was already used with different transfer parameters");
            }
            // Idempotent replay: state was not changed by this attempt, but the cache must still be
            // invalidated (Tier 1) in case the original invalidation was lost. invalidate() is
            // idempotent and version-guarded, so this is always safe.
            events.publishEvent(new AccountCacheInvalidationRequested(fromId, from.getVersion()));
            events.publishEvent(new AccountCacheInvalidationRequested(toId, to.getVersion()));
            return toResponse(existing);
        }

        if (!from.getCurrency().equals(to.getCurrency())) {
            throw new InvalidTransferException("accounts have different currencies");
        }
        if (from.getStatus() == AccountStatus.CLOSED) {
            throw new AccountStatusConflictException("closed account cannot send transfers");
        }
        if (from.getStatus() == AccountStatus.FROZEN) {
            throw new AccountStatusConflictException("frozen account cannot send transfers");
        }
        if (to.getStatus() == AccountStatus.CLOSED) {
            throw new AccountStatusConflictException("closed account cannot receive transfers");
        }
        if (from.getAmount().compareTo(amount) < 0) {
            throw new InsufficientFundsException("insufficient funds on account " + fromId);
        }

        accountRepository.updateAmount(from.getId(), from.getVersion(), from.getAmount().subtract(amount), now);
        accountRepository.updateAmount(to.getId(), to.getVersion(), to.getAmount().add(amount), now);

        outboxService.publish(
                DomainEvent.TRANSFER_COMPLETED,
                request.userId(),
                history.getId(),
                new TransferCompletedPayload(history.getId(), idempotencyKey, fromId, toId,
                        amount, from.getCurrency(), from.getVersion() + 1, to.getVersion() + 1, now),
                now);

        // Tier 1: published inside the transaction, delivered by the AFTER_COMMIT listener.
        events.publishEvent(new AccountCacheInvalidationRequested(fromId, from.getVersion() + 1));
        events.publishEvent(new AccountCacheInvalidationRequested(toId, to.getVersion() + 1));

        return toResponse(history);
    }

    private Account lock(long accountId) {
        return accountRepository.findByIdForUpdate(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));
    }

    private TransferResponseDto toResponse(TransferHistory history) {
        return new TransferResponseDto(history.getId(), history.getFromAccountId(), history.getRecipientAccountId(),
                history.getAmount(), history.getCurrency(), history.getStatus(), history.getCreatedAt());
    }
}