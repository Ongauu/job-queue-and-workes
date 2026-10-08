package com.crimson.jobq.ratelimit;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.web.servlet.HandlerInterceptor;

public class RateLimiterInterceptor implements HandlerInterceptor {
    private final RateLimiter limiter;

    public RateLimiterInterceptor(RateLimiter limiter){
        this.limiter = limiter;
    }

    @Override
    public boolean preHandle(HttpServletRequest req, HttpServletResponse res, Object handler) throws Exception {
        if(!"POST".equals(req.getMethod())) return true;

        RateLimiter.Decision d = limiter.tryAcquire(ClientIds.resolve(req));
        res.setHeader("X-RateLimit-Limit", String.valueOf(d.limit()));
        res.setHeader("X-RateLimit-Remaining", String.valueOf(d.remaining()));

        if(d.allowed()) return true;
        long retryAfterSeconds = Math.max(1, (d.retryAfterMillis() + 999) / 1000);

        res.setStatus(429);
        res.setHeader("Retry-After", String.valueOf(retryAfterSeconds));
        res.setContentType("application/json");
        res.getWriter().write("{\"error\":\"rate_limited\", \"retryAfterMillis\":" + d.retryAfterMillis() + "}");

        return false;
    }
}
