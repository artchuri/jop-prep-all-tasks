package nip.edu.jobprep.dto;

import jakarta.validation.constraints.NotNull;

public record FreezeAccountRequestDto(

        @NotNull Long changedByUserId

) {
}
