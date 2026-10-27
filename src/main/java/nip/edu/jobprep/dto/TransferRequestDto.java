package nip.edu.jobprep.dto;

import jakarta.validation.constraints.NotNull;

import java.math.BigDecimal;

public record TransferRequestDto(

        @NotNull Long recipientAccountId,
        @NotNull Long userId,
        @NotNull Long fromAccountId,
        @NotNull BigDecimal amount

) {
}
