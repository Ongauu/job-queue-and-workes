package com.crimson.jobq.worker.handlers;

import com.crimson.jobq.queue.ClaimedJob;
import com.crimson.jobq.worker.JobHandler;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

@Component
public class SlowHandler implements JobHandler {
    private final ObjectMapper mapper;

    public SlowHandler(ObjectMapper mapper){
        this.mapper = mapper;
    }

    @Override
    public String type(){
        return "slow";
    }

    @Override
    public void handle(ClaimedJob job) throws Exception{
        Thread.sleep(mapper.readTree(job.payload()).path("ms").asLong(2000));
    }
}
