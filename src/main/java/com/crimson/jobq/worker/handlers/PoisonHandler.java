package com.crimson.jobq.worker.handlers;

import com.crimson.jobq.queue.ClaimedJob;
import com.crimson.jobq.worker.JobHandler;
import com.crimson.jobq.worker.NonRetryableException;
import org.springframework.stereotype.Component;

//permanent failure that should not be retied

@Component
public class PoisonHandler implements JobHandler {
    @Override
    public String type(){
        return "poison";
    }

    @Override
    public void handle(ClaimedJob job) {
        throw new NonRetryableException("payload is invalid, retrying cannot help");
    }
}
