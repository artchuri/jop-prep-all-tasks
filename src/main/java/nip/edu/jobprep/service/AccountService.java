package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.dto.FreezeAccountResponseDto;

public interface AccountService {

    FreezeAccountResponseDto freeze(long accountId, long changedBy);

    AccountResponseDto get(long accountId);
}