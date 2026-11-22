package nip.edu.jobprep.ratelimit;

import nip.edu.jobprep.model.GlobalValues;
import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.List;

public final class PeakProfileEvaluator {
    private record Profile(CronExpression cron, Duration duration, double multiplier) {
    }

    private final List<Profile> profiles;

    public PeakProfileEvaluator(RateLimitProperties properties) {
        this.profiles = properties.peakProfiles().stream()
                .map(p -> new Profile(CronExpression.parse(p.cron()), p.duration(), p.multiplier()))
                .toList();
    }

    public double multiplierAt(long nowMs) {
        double m = 1.0;
        Instant now = Instant.ofEpochMilli(nowMs);
        for (Profile p : profiles) {
            Instant from = now.minus(p.duration());
            ZonedDateTime fire = p.cron().next(from.atZone(GlobalValues.ZONE_UTC));
            if (fire != null && !fire.toInstant().isAfter(now)) {
                m = Math.max(m, p.multiplier());
            }
        }
        return m;
    }
}
