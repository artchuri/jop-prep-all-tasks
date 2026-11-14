package nip.edu.jobprep.service;

import nip.edu.jobprep.model.GlobalValues;
import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.TransferHistory;
import nip.edu.jobprep.exceptions.*;
import nip.edu.jobprep.utils.mapper.TransferResponseMapper;
import nip.edu.jobprep.outbox.event.DomainEvent;
import nip.edu.jobprep.outbox.core.OutboxService;
import nip.edu.jobprep.outbox.event.TransferCompletedPayload;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.utils.cache.CacheEvictAfterCommit;
import nip.edu.jobprep.repositories.TransferRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.Objects;

@Service
public class TransferServiceImpl implements TransferService {

    private final AccountRepository accountRepository;
    private final TransferRepository transferRepository;
    private final OutboxService outboxService;
    private final CacheEvictAfterCommit cacheEvict;
    private final TransferResponseMapper responseMapper;

    public TransferServiceImpl(AccountRepository accountRepository, TransferRepository transferRepository,
                               OutboxService outboxService, CacheEvictAfterCommit cacheEvict,
                               TransferResponseMapper responseMapper) {
        this.accountRepository = accountRepository;
        this.transferRepository = transferRepository;
        this.outboxService = outboxService;
        this.cacheEvict = cacheEvict;
        this.responseMapper = responseMapper;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public TransferResponseDto transfer(String idempotencyKey, TransferRequestDto request)
            throws TransferForbiddenException, IdempotencyConflictException, InvalidTransferException {

        BigDecimal amount = normalizeAmount(request.amount());
        TransferTargets targets = resolveTargets(request, amount);

        TransferHistory history = registerTransfer(idempotencyKey, request, targets, amount);
        if (history == null) {
            return replayExistingTransfer(idempotencyKey, request, targets, amount);
        }

        validateTransferRules(targets, amount);
        applyBalances(targets, amount);
        publishTransferCompleted(request, targets, history, idempotencyKey, amount);

        return responseMapper.toResponse(history);
    }

    private BigDecimal normalizeAmount(BigDecimal rawAmount) throws InvalidTransferException {
        BigDecimal amount = rawAmount.setScale(2, RoundingMode.DOWN);
        if (amount.signum() <= 0) {
            throw new InvalidTransferException("amount must be positive");
        }
        return amount;
    }


    private TransferTargets resolveTargets(TransferRequestDto request, BigDecimal amount)
            throws InvalidTransferException, TransferForbiddenException {
        long fromId = request.fromAccountId();
        long toId = request.recipientAccountId();

        if (fromId == toId) {
            throw new InvalidTransferException("from and recipient accounts must be difference");
        }

        Account lower = lock(Math.min(fromId, toId));
        Account higher = lock(Math.max(fromId, toId));
        boolean senderIsLower = fromId == lower.getId();

        Account from = senderIsLower ? lower : higher;
        Account to = senderIsLower ? higher : lower;

        if (!from.getUser().getId().equals(request.userId())) {
            throw new TransferForbiddenException(
                    "account " + fromId + " does not belong to user " + request.userId());
        }
        return new TransferTargets(from, to);
    }

    private TransferHistory registerTransfer(String idempotencyKey, TransferRequestDto request,
                                             TransferTargets targets, BigDecimal amount) {
        return transferRepository.insertIfAbsent(
                targets.from().getId(), targets.to().getId(), amount, Instant.now(),
                GlobalValues.TRANSFER_STATUS_COMPLETED, idempotencyKey, targets.from().getCurrency());
    }


    private TransferResponseDto replayExistingTransfer(String idempotencyKey, TransferRequestDto request,
                                                       TransferTargets targets, BigDecimal amount)
            throws IdempotencyConflictException {
        TransferHistory existing = transferRepository.findByIdempotencyKey(idempotencyKey)
                .orElseThrow(() -> new IllegalStateException("transfer " + idempotencyKey + " not found"));

        boolean sameParameters = existing.getAmount().compareTo(amount) == 0
                && Objects.equals(existing.getFromAccountId(), targets.from().getId())
                && Objects.equals(existing.getRecipientAccountId(), targets.to().getId());

        if (!sameParameters) {
            throw new IdempotencyConflictException(
                    "idempotency key was already used with different transfer parameters");
        }
        return responseMapper.toResponse(existing);
    }

    private void validateTransferRules(TransferTargets targets, BigDecimal amount)
            throws InvalidTransferException, AccountStatusConflictException, InsufficientFundsException {
        Account from = targets.from();
        Account to = targets.to();

        if (!from.getCurrency().equals(to.getCurrency())) {
            throw new InvalidTransferException("accounts have different currencies");
        }
        requireCanSend(from, amount);
        requireCanReceive(to);
    }

    private void requireCanSend(Account from, BigDecimal amount)
            throws AccountStatusConflictException, InsufficientFundsException {
        switch (from.getStatus()) {
            case CLOSED -> throw new AccountStatusConflictException("closed account cannot send transfers");
            case FROZEN -> throw new AccountStatusConflictException("frozen account cannot send transfers");
            case ACTIVE -> {}
        }
        if (from.getAmount().compareTo(amount) < 0) {
            throw new InsufficientFundsException("insufficient funds on account " + from.getId());
        }
    }

    private void requireCanReceive(Account to) throws AccountStatusConflictException {
        switch (to.getStatus()) {
            case CLOSED -> throw new AccountStatusConflictException("closed account cannot receive transfers");
            case ACTIVE, FROZEN -> {}
        }
    }

    private void applyBalances(TransferTargets targets, BigDecimal amount) {
        Account from = targets.from();
        Account to = targets.to();
        Instant now = Instant.now();

        accountRepository.updateAmount(from.getId(), from.getVersion(), from.getAmount().subtract(amount), now);
        accountRepository.updateAmount(to.getId(), to.getVersion(), to.getAmount().add(amount), now);
    }

    private void publishTransferCompleted(TransferRequestDto request, TransferTargets targets,
                                          TransferHistory history, String idempotencyKey, BigDecimal amount) {
        Account from = targets.from();
        Account to = targets.to();
        long newFromVersion = from.getVersion() + 1;
        long newToVersion = to.getVersion() + 1;
        Instant now = Instant.now();

        outboxService.publish(
                DomainEvent.TRANSFER_COMPLETED,
                request.userId(),
                history.getId(),
                new TransferCompletedPayload(history.getId(), idempotencyKey,
                        from.getId(), to.getId(), amount, from.getCurrency(),
                        newFromVersion, newToVersion, now),
                now);

        cacheEvict.evict(from.getId(), newFromVersion);
        cacheEvict.evict(to.getId(), newToVersion);
    }

    private Account lock(long accountId) {
        return accountRepository.findByIdForUpdate(accountId)
                .orElseThrow(() -> new AccountNotFoundException("account " + accountId + " not found"));
    }

    private record TransferTargets(Account from, Account to) {
    }
}