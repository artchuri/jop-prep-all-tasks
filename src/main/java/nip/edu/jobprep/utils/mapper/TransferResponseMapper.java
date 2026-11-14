package nip.edu.jobprep.utils.mapper;

import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.entity.TransferHistory;
import org.springframework.stereotype.Component;

@Component
public class TransferResponseMapper {

    public TransferResponseDto toResponse(TransferHistory history) {
        return new TransferResponseDto(history.getId(), history.getFromAccountId(), history.getRecipientAccountId(),
                history.getAmount(), history.getCurrency(), history.getStatus(), history.getCreatedAt());
    }
}
