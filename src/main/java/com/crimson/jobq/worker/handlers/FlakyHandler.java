package com.crimson.jobq.worker.handlers;

import com.crimson.jobq.queue.ClaimedJob;
import com.crimson.jobq.worker.JobHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

//basically a test job handler to intentionally fail a few times then succeed.

@Component
public class FlakyHandler implements JobHandler{
    private final ObjectMapper mapper;

    public FlakyHandler(ObjectMapper mapper){
        this.mapper = mapper;
    }

    @Override
    public String type(){
        return "flaky";
    }

    @Override
    public void handle(ClaimedJob job) throws Exception{
        int succeedOn = mapper.readTree(job.payload()).path("succeedOnAttempt").asInt(3);
        if(job.attempts() < succeedOn){
            throw new IllegalArgumentException("simulated transient failure on attempt " + job.attempts());
        }
    }
}
