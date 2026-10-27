package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.Account;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.entity.TransferHistory;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.exceptions.AccountStatusConflictException;
import nip.edu.jobprep.exceptions.InsufficientFundsException;
import nip.edu.jobprep.exceptions.InvalidTransferException;
import nip.edu.jobprep.exceptions.TransferForbiddenException;
import nip.edu.jobprep.repositories.AccountRepository;
import nip.edu.jobprep.repositories.TransferRepository;
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

    public TransferServiceImpl(AccountRepository accountRepository, TransferRepository transferRepository) {
        this.accountRepository = accountRepository;
        this.transferRepository = transferRepository;
    }

    @Override
    @Transactional(rollbackFor = Exception.class)
    public TransferResponseDto transfer(String idempotencyKey, TransferRequestDto request) {
        long fromId = request.fromAccountId();
        long toId = request.recipientAccountId();
        // сразу тут округлим без проверки что сумма не нулевая тк упали бы мы раньше еще на уровне контроллера вроде как
        BigDecimal amount = request.amount().setScale(2, RoundingMode.DOWN);

        if (fromId == toId) {
            throw new InvalidTransferException("from and recipient accounts must be difference");
        }
        if (amount.signum() <= 0) {
            throw new InvalidTransferException("amount must be positive");
        }



        // лок имеется ввиду из за селект фор апдейт на бд
        // правило лока сначала берется минимальный айди и блокируется а затем максимальный что гарантирпует нам что локи будут взяты в одном порядке
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
            return transferRepository.findByIdempotencyKey(idempotencyKey)
                    .map(this::toResponse)
                    .orElseThrow(() -> new IllegalStateException("transfer " + idempotencyKey + " not found"));
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
