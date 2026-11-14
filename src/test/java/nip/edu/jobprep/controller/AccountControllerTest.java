package nip.edu.jobprep.controller;

import nip.edu.jobprep.dto.AccountResponseDto;
import nip.edu.jobprep.dto.FreezeAccountRequestDto;
import nip.edu.jobprep.dto.FreezeAccountResponseDto;
import nip.edu.jobprep.entity.AccountStatus;
import nip.edu.jobprep.exceptions.AccountNotFoundException;
import nip.edu.jobprep.service.AccountService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@WebMvcTest(AccountController.class)
class AccountControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private AccountService accountService;

    @Test
    void getAccountServesSnapshotWithNoStore() throws Exception {
        when(accountService.get(42L)).thenReturn(
                new AccountResponseDto(42L, new BigDecimal("100.50"), "ACTIVE", 7L));

        mockMvc.perform(get("/v1/account/{id}", 42L))
                .andExpect(status().isOk())
                .andExpect(header().string("Cache-Control", "no-store"))
                .andExpect(jsonPath("$.accountId").value(42))
                .andExpect(jsonPath("$.balance").value(100.50))
                .andExpect(jsonPath("$.status").value("ACTIVE"))
                .andExpect(jsonPath("$.version").value(7));
    }

    @Test
    void getUnknownAccountReturnsNotFound() throws Exception {
        when(accountService.get(999L)).thenThrow(new AccountNotFoundException("account 999 not found"));

        mockMvc.perform(get("/v1/account/{id}", 999L))
                .andExpect(status().isNotFound());
    }

    @Test
    void freezeUsesPostMapping() throws Exception {
        when(accountService.freeze(eq(42L), eq(5L))).thenReturn(
                new FreezeAccountResponseDto(42L, AccountStatus.FROZEN, null));

        mockMvc.perform(post("/v1/account/{id}/freeze", 42L)
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"changedByUserId\": 5}"))
                .andExpect(status().isOk());
    }
}