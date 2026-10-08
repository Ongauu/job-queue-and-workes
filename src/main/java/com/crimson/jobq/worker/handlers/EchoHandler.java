package com.crimson.jobq.worker.handlers;

import com.crimson.jobq.queue.ClaimedJob;
import com.crimson.jobq.worker.JobHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class EchoHandler implements JobHandler {
    private static final Logger log = LoggerFactory.getLogger(EchoHandler.class);

    @Override
    public String type(){
        return "echo";
    }

    @Override
    public void handle(ClaimedJob job){
        log.info("echo job {} payload = {}", job.id(), job.payload());
    }

}
