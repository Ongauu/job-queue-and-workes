package com.crimson.jobq.ratelimit;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

public final class SlidingWindowLogRateLimiter implements RateLimiter{
    private final int limit;
    private final long windowNanos;
    private final LongSupplier nanoClock;
    private final ConcurrentHashMap<String, ArrayDeque<Long>> logs = new ConcurrentHashMap<>();

    public SlidingWindowLogRateLimiter(int limit, Duration window, LongSupplier nanoClock){
        if (limit < 1 || window.isZero() || window.isNegative()) {
            throw new IllegalArgumentException("limit must be >= 1 and window positive");
        }
        this.limit = limit;
        this.windowNanos = window.toNanos();
        this.nanoClock = nanoClock;
    }

    @Override
    public Decision tryAcquire(String key){
        ArrayDeque<Long> log = logs.computeIfAbsent(key, k -> new ArrayDeque<>());
        synchronized (log) {
            long now = nanoClock.getAsLong();
            prune(log, now);
            if(log.size() < limit){
                log.addLast(now);
                return new Decision(true, limit, limit - log.size(), 0);
            }
            long waitNanos = log.peekFirst() + windowNanos - now;
            return new Decision(false, limit, 0, (waitNanos + 999_999)/ 1_000_000);
        }
    }

    @Override
    public void evictIdle(){
        long now = nanoClock.getAsLong();
        for(String key : logs.keySet()){
            logs.computeIfPresent(key, (k, log) -> {
                synchronized (log){
                    prune(log, now);
                    return log.isEmpty() ? null : log;
                }
            });
        }
    }

    public int trackedKeys(){
        return logs.size();
    }

    private void prune(ArrayDeque<Long> log, long now){
        while(!log.isEmpty() && now - log.peekFirst() >= windowNanos){
            log.pollFirst();
        }
    }
}
