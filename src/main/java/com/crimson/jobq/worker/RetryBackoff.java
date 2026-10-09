package com.crimson.jobq.worker;


import lombok.RequiredArgsConstructor;

import java.util.function.DoubleSupplier;

@RequiredArgsConstructor
public final class RetryBackoff {
    private final long baseMs;
    private final long maxMs;
    private final DoubleSupplier random;

    public long ceilingMs(int attempt){
        int shift  = Math.min(Math.max(attempt, 1) - 1, 20);
        return Math.min(maxMs, baseMs * (1L << shift));
    }

    public long delayMs(int attempt){
        long ceiling = ceilingMs(attempt);
        long half = ceiling / 2;
        return half + (long) (random.getAsDouble() * (ceiling - half));
    }
}
