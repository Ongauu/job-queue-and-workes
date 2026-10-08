package com.crimson.jobq.worker;

import com.crimson.jobq.queue.ClaimedJob;

public interface JobHandler {
    String type();

    void handle(ClaimedJob job) throws Exception;
}
