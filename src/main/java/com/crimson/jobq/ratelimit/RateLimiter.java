package com.crimson.jobq.ratelimit;

public interface RateLimiter {
    Decision tryAcquire(String key);

    void evictIdle();

    record Decision(boolean allowed, long limit, long remaining, long retryAfterMillis){}
}
