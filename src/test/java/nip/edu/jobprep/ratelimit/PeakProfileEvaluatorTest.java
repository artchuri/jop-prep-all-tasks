package nip.edu.jobprep.ratelimit;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class PeakProfileEvaluatorTest {
    private static RateLimitProperties props(RateLimitProperties.PeakProfile... profiles) {
        return new RateLimitProperties(false, "reject", "/v1", 60000L, 600, 30,
                List.of(), List.of(), 16, 8, 2, 25, List.of(profiles));
    }

    private static RateLimitProperties.PeakProfile dailyNoon(Duration duration, double multiplier) {
        return new RateLimitProperties.PeakProfile("0 0 12 * * *", duration, multiplier);
    }

    private static long at(String iso) {
        return java.time.Instant.parse(iso).toEpochMilli();
    }

    @Test
    void activeInsideDuration() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(dailyNoon(Duration.ofSeconds(10), 2.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:05Z"))).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void activeExactlyAtFireTime() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(dailyNoon(Duration.ofSeconds(10), 2.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:00Z"))).isCloseTo(2.0, within(1e-9));
    }

    @Test
    void inactiveAfterDuration() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(dailyNoon(Duration.ofSeconds(10), 2.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:11Z"))).isEqualTo(1.0);
    }

    @Test
    void inactiveBeforeAnyFire() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(dailyNoon(Duration.ofSeconds(10), 2.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-19T11:59:59Z"))).isEqualTo(1.0);
    }

    @Test
    void highestActiveMultiplierWins() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(
                dailyNoon(Duration.ofSeconds(10), 2.0),
                dailyNoon(Duration.ofSeconds(30), 3.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:05Z"))).isCloseTo(3.0, within(1e-9));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:15Z"))).isCloseTo(3.0, within(1e-9));
        assertThat(evaluator.multiplierAt(at("2026-11-19T12:00:35Z"))).isEqualTo(1.0);
    }

    @Test
    void otherProfileFireTomorrowIsNotActive() {
        PeakProfileEvaluator evaluator = new PeakProfileEvaluator(props(dailyNoon(Duration.ofSeconds(10), 2.0)));
        assertThat(evaluator.multiplierAt(at("2026-11-20T06:00:00Z"))).isEqualTo(1.0);
    }
}
