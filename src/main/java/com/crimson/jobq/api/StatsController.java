package com.crimson.jobq.api;

import com.crimson.jobq.queue.JobRepository;
import com.crimson.jobq.worker.WorkerPool;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class StatsController {
    private final JobRepository repo;
    private final WorkerPool workers;

    public StatsController(JobRepository repo, WorkerPool workers){
        this.repo = repo;
        this.workers = workers;
    }

    @GetMapping("/stats")
    public Map<String, Object> stats(){
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("jobsByStatus", repo.countByStatus());
        out.put("queueLagSeconds", repo.oldestReadyAgeSeconds());
        out.put("workers", workers.snapshot());

        return out;
    }
}
