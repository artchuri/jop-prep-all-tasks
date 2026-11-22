package nip.edu.jobprep.ratelimit;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class RateLimitMetrics {
    private final MeterRegistry meters;

    private final Map<String, Counter> accepted = new HashMap<>();
    private final Map<String, Counter> rejected = new HashMap<>();
    private final Map<String, Counter> refunded = new HashMap<>();
    private final Map<String, Counter> counted = new HashMap<>();

    private final Map<String, AtomicReference<Double>> usage = new HashMap<>();

    private final Counter unauthorized;

    private final Counter redisErrorsReserve;
    private final Counter redisErrorsRefund;

    private final AtomicInteger inFlight = new AtomicInteger();

    private final Timer reserveSuccess;
    private final Timer reserveError;
    private final Timer refundSuccess;
    private final Timer refundError;

    public RateLimitMetrics(MeterRegistry meters, RateLimitProperties properties) {
        this.meters = meters;
        for (RateLimitProperties.ClientSpec c : properties.clients()) {
            for (String ep : endpointIds(properties)) {
                accepted.put(tag(c.name(), ep), counter("rate_limit_requests_total", "client", c.name(), "endpoint", ep, "result", "accepted"));
                rejected.put(tag(c.name(), ep), counter("rate_limit_requests_total", "client", c.name(), "endpoint", ep, "result", "rejected"));
                refunded.put(tag(c.name(), ep), counter("rate_limit_refunds_total", "client", c.name(), "endpoint", ep));
                counted.put(tag(c.name(), ep), counter("rate_limit_requests_total", "client", c.name(), "endpoint", ep, "result", "counted"));
                AtomicReference<Double> ratio = new AtomicReference<>(0.0);
                usage.put(tag(c.name(), ep), ratio);
                Gauge.builder("rate_limit_window_usage_ratio", ratio, r -> r.get())
                        .tag("client", c.name()).tag("endpoint", ep).register(meters);
            }
        }
        for (RateLimitProperties.ClientSpec c : properties.clients()) {
            counter("rate_limit_outage_total", "client", c.name());
        }
        this.unauthorized = counter("rate_limit_unauthorized_total");
        this.redisErrorsReserve = counter("rate_limit_redis_errors_total", "op", "reserve");
        this.redisErrorsRefund = counter("rate_limit_redis_errors_total", "op", "refund");
        Gauge.builder("rate_limit_in_flight_reservations", inFlight, AtomicInteger::doubleValue).register(meters);
        this.reserveSuccess = evalTimer("reserve", "success");
        this.reserveError = evalTimer("reserve", "error");
        this.refundSuccess = evalTimer("refund", "success");
        this.refundError = evalTimer("refund", "error");
    }

    private static Iterable<String> endpointIds(RateLimitProperties properties) {
        java.util.List<String> ids = new java.util.ArrayList<>();
        for (RateLimitProperties.EndpointLimit e : properties.endpoints()) ids.add(e.id());
        ids.add("default");
        return ids;
    }

    private static String tag(String client, String endpoint) {
        return client + "|" + endpoint;
    }

    private Counter counter(String name, String... keyValues) {
        return Counter.builder(name).tags(keyValues).register(meters);
    }

    private Timer evalTimer(String op, String outcome) {
        return Timer.builder("rate_limit_eval_seconds")
                .description("Redis EVAL latency for rate limiter operations")
                .tag("op", op).tag("outcome", outcome)
                .publishPercentileHistogram()
                .register(meters);
    }

    public void accepted(String client, String endpoint) { accepted.get(tag(client, endpoint)).increment(); }

    public void rejected(String client, String endpoint) { rejected.get(tag(client, endpoint)).increment(); }

    public void refunded(String client, String endpoint) { refunded.get(tag(client, endpoint)).increment(); }

    public void counted(String client, String endpoint) { counted.get(tag(client, endpoint)).increment(); }

    public void unauthorized() { unauthorized.increment(); }

    public void outage(String client) {
        counter("rate_limit_outage_total", "client", client).increment();
    }

    public void redisErrorReserve() { redisErrorsReserve.increment(); }

    public void redisErrorRefund() { redisErrorsRefund.increment(); }

    public void evalReserve(boolean failed, long nanos) {
        (failed ? reserveError : reserveSuccess).record(nanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    public void evalRefund(boolean failed, long nanos) {
        (failed ? refundError : refundSuccess).record(nanos, java.util.concurrent.TimeUnit.NANOSECONDS);
    }

    public void windowUsage(String client, String endpoint, double used, double limit) {
        AtomicReference<Double> r = usage.get(tag(client, endpoint));
        if (r != null && limit > 0) r.set(Math.min(used / limit, 1.0));
    }

    public void reservationStarted() { inFlight.incrementAndGet(); }

    public void reservationClosed() { inFlight.decrementAndGet(); }
}
