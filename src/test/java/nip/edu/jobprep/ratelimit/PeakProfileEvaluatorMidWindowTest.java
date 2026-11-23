package nip.edu.jobprep.ratelimit;

import java.time.LocalTime;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PeakProfileEvaluatorMidWindowTest {
    private static final RateLimitProperties.PeakProfile PEAK =
            new RateLimitProperties.PeakProfile("0 0 12 * * *", Duration.ofMinutes(30), 2.5);

    private static long atUtc(int hour, int minute) {
        ZonedDateTime t = ZonedDateTime.of(LocalDate.of(2026, 11, 21), LocalTime.of(hour, minute), ZoneOffset.UTC);
        return t.toInstant().toEpochMilli();
    }

    @Test
    void beforeFireMultiplierIsOne() {
        assertThat(new PeakProfileEvaluator(props(List.of(PEAK))).multiplierAt(atUtc(11, 59))).isEqualTo(1.0);
    }

    @Test
    void midWindowMultiplierApplies() {
        assertThat(new PeakProfileEvaluator(props(List.of(PEAK))).multiplierAt(atUtc(12, 15))).isEqualTo(2.5);
    }

    @Test
    void exactlyAtFireTimeApplies() {
        assertThat(new PeakProfileEvaluator(props(List.of(PEAK))).multiplierAt(atUtc(12, 0))).isEqualTo(2.5);
    }

    @Test
    void afterDurationRevertsToOne() {
        assertThat(new PeakProfileEvaluator(props(List.of(PEAK))).multiplierAt(atUtc(12, 30))).isEqualTo(1.0);
    }

    @Test
    void overlappingProfilesUseMax() {
        RateLimitProperties.PeakProfile small =
                new RateLimitProperties.PeakProfile("0 5 12 * * *", Duration.ofMinutes(60), 1.5);
        PeakProfileEvaluator eval = new PeakProfileEvaluator(props(List.of(PEAK, small)));
        assertThat(eval.multiplierAt(atUtc(12, 15))).isEqualTo(2.5);
        assertThat(eval.multiplierAt(atUtc(12, 35))).isEqualTo(1.5, within(0.0001));
    }

    private RateLimitProperties props(List<RateLimitProperties.PeakProfile> peaks) {
        return new RateLimitProperties(true, "reject", "/v1", 60000L, 600, 30,
                List.of(new RateLimitProperties.ClientSpec("c", "k")),
                List.of(new RateLimitProperties.EndpointLimit("get-account", "GET", "/v1/account/*", 10)),
                16, 8, 2, 25, peaks);
    }
}
