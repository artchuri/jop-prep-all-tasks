package nip.edu.jobprep.ratelimit;

import java.time.Clock;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.jspecify.annotations.NonNull;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.filter.OncePerRequestFilter;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;

public final class RateLimitFilter extends OncePerRequestFilter {
    private final RateLimitProperties properties;

    private final ApiKeyResolver apiKeyResolver;

    private final EndpointClassifier endpointClassifier;

    private final RateLimiter rateLimiter;

    private final RateLimitMetrics metrics;

    private final PeakProfileEvaluator peakProfiles;

    private final Clock clock;

    private final ObjectMapper objectMapper;

    public RateLimitFilter(RateLimitProperties properties,
                           ApiKeyResolver apiKeyResolver,
                           EndpointClassifier endpointClassifier,
                           RateLimiter rateLimiter,
                           RateLimitMetrics metrics,
                           PeakProfileEvaluator peakProfiles,
                           Clock clock,
                           ObjectMapper objectMapper) {
        this.properties = properties;
        this.apiKeyResolver = apiKeyResolver;
        this.endpointClassifier = endpointClassifier;
        this.rateLimiter = rateLimiter;
        this.metrics = metrics;
        this.peakProfiles = peakProfiles;
        this.clock = clock;
        this.objectMapper = objectMapper;
    }

    @Override
    protected boolean shouldNotFilter(@NonNull HttpServletRequest request) {
        String path = pathWithinApplication(request);
        String prefix = properties.pathPrefix();
        return !properties.enabled() || !(path.equals(prefix) || path.startsWith(prefix + "/"));
    }

    static String pathWithinApplication(HttpServletRequest request) {
        String uri = request.getRequestURI();
        String context = request.getContextPath();
        return context == null || context.isEmpty() || !uri.startsWith(context)
                ? uri : uri.substring(context.length());
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                    @NonNull HttpServletResponse response,
                                    @NonNull FilterChain chain) throws ServletException, IOException {
        RateLimitProperties.ClientSpec client = apiKeyResolver.resolve(request.getHeader("X-API-Key"));
        if (client == null) {
            metrics.unauthorized();
            problem(response, 401, "unauthorized", "missing or unknown api key", "UNAUTHORIZED", null);
            return;
        }
        EndpointClassifier.Endpoint endpoint =
                endpointClassifier.classify(request.getMethod(), pathWithinApplication(request));
        long nowMs = clock.millis();
        double limit = endpoint.limitPerMinute() / 60000.0 * properties.windowMs()
                * peakProfiles.multiplierAt(nowMs);
        RateLimiter.Reservation reservation;
        try {
            reservation = rateLimiter.reserve(client, endpoint, limit, nowMs);
        } catch (RateLimitOutageException e) {
            metrics.outage(client.name());
            if (properties.outageStrategy().equals("allow")) {
                chain.doFilter(request, response);
                return;
            } else {
                problem(response, 503, "service unavailable",
                        "rate limiter is temporarily unavailable, retry", "RATE_LIMITER_UNAVAILABLE",
                        properties.outageRetryAfterSeconds());
            }
            return;
        }
        if (!reservation.allowed()) {
            metrics.rejected(client.name(), endpoint.id());
            problem(response, 429, "rate limit exceeded",
                    "client " + client.name() + " exceeded the limit for " + endpoint.id(),
                    "RATE_LIMITED",
                    (int) Math.max(1, Math.ceil(reservation.retryAfterMs() / 1000.0)));
            return;
        }
        metrics.reservationStarted();
        try {
            chain.doFilter(request, response);
            switch (SlotOutcome.of(response.getStatus())) {
                case ACCEPTED -> metrics.accepted(client.name(), endpoint.id());
                case COUNTED -> metrics.counted(client.name(), endpoint.id());
                case REFUNDED -> {
                    rateLimiter.refund(reservation);
                    metrics.refunded(client.name(), endpoint.id());
                }
            }
        } catch (ServletException | IOException | RuntimeException e) {
            rateLimiter.refund(reservation);
            metrics.refunded(client.name(), endpoint.id());
            throw e;
        } finally {
            metrics.reservationClosed();
        }
    }

    private void problem(HttpServletResponse response,
                         int status,
                         String title,
                         String detail,
                         String code,
                         Integer retryAfterSeconds) throws IOException {
        response.setStatus(status);
        if (retryAfterSeconds != null) {
            response.setHeader("Retry-After", String.valueOf(retryAfterSeconds));
        }
        response.setContentType("application/problem+json;charset=UTF-8");
        ProblemDetail pd = ProblemDetail.forStatusAndDetail(HttpStatus.valueOf(status), detail);
        pd.setTitle(title);
        pd.setProperty("code", code);
        objectMapper.writeValue(response.getWriter(), pd);
    }
}
