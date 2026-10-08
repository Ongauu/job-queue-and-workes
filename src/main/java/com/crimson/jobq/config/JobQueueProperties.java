package com.crimson.jobq.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

@ConfigurationProperties(prefix = "jobq")
public record JobQueueProperties(
    @DefaultValue Api api,
    @DefaultValue Backpressure backpressure,
    @DefaultValue Worker worker,
    @DefaultValue Retry retry
) {
    public record Api(
            @DefaultValue("token-bucket") String algorithm,
            @DefaultValue("10") int capacity,
            @DefaultValue("5") double refillPerSecond,
            @DefaultValue("10") int windowSeconds){}

    public record Backpressure(
            @DefaultValue("100") int maxPending,
            @DefaultValue("30") int maxPendingPerClient,
            @DefaultValue("5") int retryAfterSeconds) {}

    public record Worker(
            @DefaultValue("4") int threads,
            @DefaultValue("200") long pollIntervalMs,
            @DefaultValue("2000") long maxPollIntervalMs,
            @DefaultValue("30") int leaseSeconds,
            @DefaultValue("10") double outboundRatePerSecond,
            @DefaultValue("10") int outboundBurst) {}

    public record Retry(
            @DefaultValue("1000") long baseDelayMs,
            @DefaultValue("60000") long maxDelayMs) {}
}
