package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.FreezeAccountResponseDto;

public interface AccountService {
    FreezeAccountResponseDto freeze(long accountId, long changedBy);
}
