package nip.edu.jobprep.ratelimit;


public enum SlotOutcome {
    ACCEPTED,
    REFUNDED,
    COUNTED;

    static SlotOutcome of(int status) {
        return switch (status) {
            case 404, 405 -> COUNTED;
            default -> status < 400 ? ACCEPTED : REFUNDED;
        };
    }
}
