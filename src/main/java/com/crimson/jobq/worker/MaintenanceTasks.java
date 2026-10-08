package com.crimson.jobq.worker;

import com.crimson.jobq.queue.JobRepository;
import com.crimson.jobq.ratelimit.RateLimiter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class MaintenanceTasks {
    private static final Logger log = LoggerFactory.getLogger(MaintenanceTasks.class);

    private final JobRepository repo;
    private final RateLimiter limiter;

    public MaintenanceTasks(JobRepository repo, RateLimiter limiter){
        this.repo = repo;
        this.limiter = limiter;
    }

    @Scheduled(fixedDelay = 5000)
    public void reapExpiredLeases(){
        int n = repo.reclaimExpired();
        if(n > 0) log.warn("reclaimed {} job(s) with expired leases", n);
    }

    @Scheduled(fixedDelay = 6000)
    public void evictIdleLimiterState(){
        limiter.evictIdle();
    }
}
