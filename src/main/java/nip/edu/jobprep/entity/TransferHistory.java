package nip.edu.jobprep.entity;

import jakarta.persistence.*;
import lombok.*;

import java.math.BigDecimal;
import java.time.Instant;

@Entity
@Getter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "transfers_history",
        uniqueConstraints = {
                @UniqueConstraint(name = "uk_transfers_history_idempotency_key",
                        columnNames = "idempotency_key"),
                @UniqueConstraint(name = "uk_transfers_history_idempotency_key_amount",
                        columnNames = {"idempotency_key", "amount"})
        },
        indexes = {
                @Index(name = "idx_transfers_history_id_and_idempotency_key",
                        columnList = "id, idempotency_key"),
                @Index(name = "idx_transfers_history_amount_and_idempotency_key",
                        columnList = "amount, idempotency_key")
        })
public class TransferHistory {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Setter
    @Column(name = "recipient_account_id", nullable = false)
    private Long recipientAccountId;

    @Setter
    @Column(name = "from_account_id", nullable = false)
    private Long fromAccountId;

    @Setter
    @Column(name = "amount", nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Setter
    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Setter
    @Column(name = "status", nullable = false)
    private String status;

    @Setter
    @Column(name = "idempotency_key", nullable = false)
    private String idempotencyKey;

    @Setter
    @Column(name = "currency", nullable = false)
    private String currency;
}
