package nip.edu.jobprep.exceptions;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(AccountNotFoundException.class)
    public ProblemDetail accountNotFound(AccountNotFoundException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.NOT_FOUND, exception.getMessage());
        detail.setTitle("account not found");
        return detail;
    }

    @ExceptionHandler(AccountStatusConflictException.class)
    public ProblemDetail accountStatusConflict(AccountStatusConflictException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.CONFLICT, exception.getMessage());
        detail.setTitle("account status conflict");
        return detail;
    }

    @ExceptionHandler(InvalidTransferException.class)
    public ProblemDetail invalidTransfer(InvalidTransferException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, exception.getMessage());
        detail.setTitle("invalid transfer");
        return detail;
    }

    @ExceptionHandler(InsufficientFundsException.class)
    public ProblemDetail insufficientFunds(InsufficientFundsException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.UNPROCESSABLE_ENTITY, exception.getMessage());
        detail.setTitle("insufficient funds");
        detail.setProperty("code", "INSUFFICIENT_FUNDS");
        return detail;
    }

    @ExceptionHandler(TransferForbiddenException.class)
    public ProblemDetail transferForbidden(TransferForbiddenException exception) {
        ProblemDetail detail = ProblemDetail.forStatusAndDetail(HttpStatus.FORBIDDEN, exception.getMessage());
        detail.setTitle("transfer forbidden");
        return detail;
    }
}
