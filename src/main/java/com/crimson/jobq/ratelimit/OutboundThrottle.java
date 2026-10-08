package com.crimson.jobq.ratelimit;

import java.util.function.LongSupplier;

public final class OutboundThrottle{
    private static final String KEY = "outbound";
    private final TokenBucketRateLimiter bucket;

    public OutboundThrottle(int burst, double ratePerSecond, LongSupplier nanoClock){
        this.bucket = new TokenBucketRateLimiter(burst, ratePerSecond, nanoClock);
    }

    public void acquire() throws InterruptedException{
        while(true) {
            RateLimiter.Decision d = bucket.tryAcquire(KEY);
            if(d.allowed()) return;

            Thread.sleep(Math.max(1, d.retryAfterMillis()));
        }
    }

    public void refund(){
        bucket.refund(KEY);
    }
}
