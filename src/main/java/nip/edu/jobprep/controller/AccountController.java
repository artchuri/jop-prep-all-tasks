package nip.edu.jobprep.controller;

import jakarta.validation.Valid;
import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.dto.FreezeAccountRequestDto;
import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.service.AccountQueryService;
import nip.edu.jobprep.service.AccountService;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/v1/account")
public class AccountController {
    private final AccountService accountService;
    private final AccountQueryService accountQueryService;

    public AccountController(AccountService accountService, AccountQueryService accountQueryService) {
        this.accountService = accountService;
        this.accountQueryService = accountQueryService;
    }


    @GetMapping("/{id}")
    public ResponseEntity<AccountResponseDto> get(@PathVariable long id) {
        return ResponseEntity.ok()
                .header(HttpHeaders.CACHE_CONTROL, "no-store")
                .body(accountQueryService.get(id));
    }

    @PostMapping("/{id}/freeze")
    public ResponseEntity<FreezeAccountResponseDto> freeze(@PathVariable long id,
                                                           @Valid @RequestBody FreezeAccountRequestDto request) {
        return ResponseEntity.ok(accountService.freeze(id, request.changedByUserId()));
    }
}