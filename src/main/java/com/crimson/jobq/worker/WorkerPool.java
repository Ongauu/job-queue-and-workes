package com.crimson.jobq.worker;

import com.crimson.jobq.config.JobQueueProperties;
import com.crimson.jobq.queue.ClaimedJob;
import com.crimson.jobq.queue.JobRepository;
import com.crimson.jobq.ratelimit.OutboundThrottle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

@Component
public class WorkerPool implements SmartLifecycle {
    private static final Logger log = LoggerFactory.getLogger(WorkerPool.class);

    private final JobRepository repo;
    private final JobHandlerRegistry registry;
    private final OutboundThrottle throttle;
    private final RetryBackoff backoff;
    private final JobQueueProperties.Worker cfg;
    private final String instanceId = UUID.randomUUID().toString().substring(0, 8);

    private final AtomicInteger active = new AtomicInteger();
    private final AtomicLong succeeded = new AtomicLong();
    private final AtomicLong retried = new AtomicLong();
    private final AtomicLong buried = new AtomicLong();
    private final AtomicLong lostLeases = new AtomicLong();

    public WorkerPool(JobRepository repo, JobHandlerRegistry registry, OutboundThrottle throttle,
                      RetryBackoff backoff, JobQueueProperties props) {
        this.repo = repo;
        this.registry = registry;
        this.throttle = throttle;
        this.backoff = backoff;
        this.cfg = props.worker();
    }

    private volatile boolean running;
    private ExecutorService executor;



    @Override
    public void start() {
        running = true;
        executor = Executors.newFixedThreadPool(cfg.threads());
        for (int i = 1; i <= cfg.threads(); i++) {
            String workerId = instanceId + "-w" + i;
            executor.submit(() -> runLoop(workerId));
        }
        log.info("Started {} workers (instance {})", cfg.threads(), instanceId);
    }

    @Override
    public void stop() {
        running = false;
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    @Override public boolean isRunning() {
        return running;
    }

    private void runLoop(String workerId) {
        long idleMs = cfg.pollIntervalMs();
        while (running) {
            try {
                throttle.acquire();
                Optional<ClaimedJob> claimed = repo.claim(workerId, cfg.leaseSeconds());
                if (claimed.isEmpty()) {
                    throttle.refund();
                    sleepWithJitter(idleMs);
                    idleMs = Math.min(idleMs * 2, cfg.maxPollIntervalMs());
                    continue;
                }
                idleMs = cfg.pollIntervalMs();
                process(workerId, claimed.get());
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception e) {
                log.error("{} loop error: {}", workerId, e.toString());
                try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
            }
        }
    }

    private void process(String workerId, ClaimedJob job) {
        active.incrementAndGet();
        try {
            Exception failure = null;
            boolean retryable = true;
            try {
                JobHandler handler = registry.get(job.type());
                if (handler == null) throw new NonRetryableException("no handler for type " + job.type());
                handler.handle(job);
            } catch (NonRetryableException e) {
                failure = e;
                retryable = false;
            } catch (Exception e) {
                failure = e;
            }

            boolean recorded;
            if (failure == null) {
                recorded = repo.complete(job.id(), workerId);
                if (recorded) succeeded.incrementAndGet();
            } else if (!retryable || job.attempts() >= job.maxAttempts()) {
                recorded = repo.markDead(job.id(), workerId, describe(failure));
                if (recorded) buried.incrementAndGet();
                log.warn("job {} DEAD after {} attempt(s): {}", job.id(), job.attempts(), describe(failure));
            } else {
                long delay = backoff.delayMs(job.attempts());
                recorded = repo.scheduleRetry(job.id(), workerId, describe(failure), delay);
                if (recorded) retried.incrementAndGet();
                log.info("job {} failed (attempt {}/{}), retrying in {} ms", job.id(), job.attempts(), job.maxAttempts(), delay);
            }
            if (!recorded) {
                lostLeases.incrementAndGet();
                log.warn("job {} lost its lease before {} finished", job.id(), workerId);
            }
        } finally {
            active.decrementAndGet();
        }
    }

    private static String describe(Exception e) {
        String s = e.getClass().getSimpleName() + ": " + e.getMessage();
        return s.length() > 500 ? s.substring(0, 500) : s;
    }

    private static void sleepWithJitter(long ms) throws InterruptedException {
        long jitter = (long) (ms * 0.25 * (ThreadLocalRandom.current().nextDouble() * 2 - 1));
        Thread.sleep(Math.max(1, ms + jitter));
    }

    public Map<String, Object> snapshot() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("instance", instanceId);
        m.put("threads", cfg.threads());
        m.put("activeNow", active.get());
        m.put("succeeded", succeeded.get());
        m.put("retriesScheduled", retried.get());
        m.put("buriedDead", buried.get());
        m.put("lostLeases", lostLeases.get());
        return m;
    }
}
