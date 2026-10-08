package com.crimson.jobq.queue;

//represent the information about a job that has been successfully claimed by a worker.

public record ClaimedJob(
        long id,
        String clientId,
        String type,
        String payload,
        int attempts,
        int maxAttempts
) {}
