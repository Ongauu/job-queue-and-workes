package com.crimson.jobq.queue;

import java.time.Instant;
//info about a job to return to the client
public record JobView(
        long id,
        String clientId,
        String type,
        String payload,
        String status,
        int attempts,
        int maxAttempts,
        Instant runAt,
        String lastError,
        Instant createdAt,
        Instant finishedAt
) {}
