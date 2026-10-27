package nip.edu.jobprep.service;

import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;

public interface TransferService {
    TransferResponseDto transfer(String idempotencyKey, TransferRequestDto request);
}
