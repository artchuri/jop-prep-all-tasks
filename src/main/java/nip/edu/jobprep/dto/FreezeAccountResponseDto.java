package nip.edu.jobprep.dto;

import nip.edu.jobprep.entity.AccountStatus;

import java.time.Instant;

public record FreezeAccountResponseDto(

        long accountId,
        AccountStatus status,
        Instant statusChangedAt
) {
}
