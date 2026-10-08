package com.crimson.jobq.ratelimit;


import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public final class TokenBucketRateLimiter implements RateLimiter{
    private static final class Bucket{
        double tokens;
        long lastRefillNanos;

        Bucket(double tokens, long lastRefillNanos){
            this.tokens = tokens;
            this.lastRefillNanos = lastRefillNanos;
        }
    }

    private final int capacity;
    private final double refillPerSecond;
    private final LongSupplier nanoClock;
    private final ConcurrentHashMap<String, Bucket> buckets = new ConcurrentHashMap<>();

    public TokenBucketRateLimiter(int capacity, double refillPerSecond, LongSupplier nanoClock){
        if(capacity <1 || refillPerSecond <= 0){
            throw new IllegalArgumentException("capacity must be >= 1, and refillPerSecond > 0");
        }
        this.capacity = capacity;
        this.refillPerSecond = refillPerSecond;
        this.nanoClock = nanoClock;
    }

    @Override
    public Decision tryAcquire(String key){
        Bucket b = buckets.computeIfAbsent(key, k -> new Bucket(capacity, nanoClock.getAsLong()));
        synchronized (b){
            refill(b, nanoClock.getAsLong());
            if(b.tokens >= 1.0){
                b.tokens -= 1.0;
                return new Decision(true, capacity, (long) Math.floor(b.tokens), 0);
            }
            long waitMillis = (long) Math.ceil((1.0 - b.tokens) / refillPerSecond * 1000.0);
            return new Decision(false, capacity, 0, waitMillis);
        }
    }

    public void refund(String key){
        Bucket b = buckets.get(key);
        if(b == null) return;
        synchronized (b){
            b.tokens = Math.min(capacity, b.tokens + 1.0);
        }
    }

    @Override
    public void evictIdle(){
        long now = nanoClock.getAsLong();
        for(String key : buckets.keySet()){
            buckets.computeIfPresent(key, (k, b) -> {
                synchronized (b){
                    refill(b, now);
                    return b.tokens >= capacity ? null : b;
                }
            });
        }
    }

    public int trackedKeys(){
        return buckets.size();
    }

    private void refill(Bucket b, long nowNanos){
        long elapsed = nowNanos - b.lastRefillNanos;
        if(elapsed > 0){
            b.tokens = Math.min(capacity, b.tokens + (elapsed / 1_000_000_000.0) * refillPerSecond);
            b.lastRefillNanos = nowNanos;
        }
    }
}
