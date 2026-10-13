package nip.edu.jobprep.exceptions;

public class AccountStatusConflictException extends RuntimeException {
    public AccountStatusConflictException(String message) {
        super(message);
    }
}
