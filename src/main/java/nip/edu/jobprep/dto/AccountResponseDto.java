package nip.edu.jobprep.dto;

import java.math.BigDecimal;

public record AccountResponseDto(long accountId, BigDecimal balance, String status, long version) {
}