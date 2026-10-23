package nip.edu.jobprep.entity;

import jakarta.persistence.*;
import lombok.*;

import java.time.OffsetDateTime;

@Entity
@Table(name = "account_status_history")
@Getter

@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AccountStatusHistory {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Setter
    @Column(name = "account_id", nullable = false)
    private Long accountId;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(name = "old_status")
    private AccountStatus oldStatus;

    @Setter
    @Enumerated(EnumType.STRING)
    @Column(name = "new_status", nullable = false)
    private AccountStatus newStatus;

    @Setter
    @Column(name = "changed_by")
    private Long changedBy;

    @Setter
    @Column(name = "changed_at", nullable = false)
    private OffsetDateTime changedAt;

    @Setter
    @Version
    @Column(name = "version", nullable = false)
    private Long version;
}
