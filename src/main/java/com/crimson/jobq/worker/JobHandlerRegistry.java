package com.crimson.jobq.worker;

import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class JobHandlerRegistry {
    private final Map<String, JobHandler> byType = new HashMap<>();

    public JobHandlerRegistry(List<JobHandler> handlers){
        for(JobHandler h : handlers) byType.put(h.type(), h);
    }

    public JobHandler get(String type){
        return byType.get(type);
    }

    public boolean supports(String type){
        return byType.containsKey(type);
    }

    public Set<String> types(){
        return new TreeSet<>(byType.keySet());
    }
}
