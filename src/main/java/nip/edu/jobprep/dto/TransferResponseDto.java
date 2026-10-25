package nip.edu.jobprep.dto;

import java.math.BigDecimal;
import java.time.Instant;

public record TransferResponseDto(

        Long transferId,
        Long fromAccountId,
        Long recipientAccountId,
        BigDecimal amount,
        String currency,
        String status,
        Instant createdAt

) {
}
