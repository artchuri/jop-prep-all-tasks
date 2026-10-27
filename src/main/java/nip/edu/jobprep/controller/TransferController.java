package nip.edu.jobprep.controller;

import jakarta.validation.Valid;
import nip.edu.jobprep.dto.TransferRequestDto;
import nip.edu.jobprep.dto.TransferResponseDto;
import nip.edu.jobprep.exceptions.InvalidTransferException;
import nip.edu.jobprep.service.TransferService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

@RestController
@RequestMapping("/v1/transfer")
public class TransferController {
    private final TransferService transferService;

    public TransferController(TransferService transferService) {
        this.transferService = transferService;

    }

    @PostMapping
    public ResponseEntity<TransferResponseDto> transfer(
            @RequestHeader("Idempotency-key") String idempotencyKey,
            @Valid @RequestBody TransferRequestDto request) {
        validateIdempotencyKey(idempotencyKey);
        return ResponseEntity.ok(transferService.transfer(idempotencyKey, request));
    }

    private void validateIdempotencyKey(String value) {
        try {
            UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            throw new InvalidTransferException("idempotency key must be a UUID");
        }
    }
}
