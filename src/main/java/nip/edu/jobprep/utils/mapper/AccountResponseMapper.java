package nip.edu.jobprep.utils.mapper;

import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.Account;
import org.springframework.stereotype.Component;

@Component
public class AccountResponseMapper {

    public FreezeAccountResponseDto toResponse(Account account) {
        return new FreezeAccountResponseDto(account.getId(), account.getStatus(), account.getStatusChangedAt());
    }
}
