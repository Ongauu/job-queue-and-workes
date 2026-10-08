package com.crimson.jobq.queue;

//represents the answer you get after trying to enqueue (add) a job.

public record EnqueueResult(Outcome outcome, Long id) {
    public enum Outcome { ACCEPTED, CLIENT_QUOTA, QUEUE_FULL}
}
