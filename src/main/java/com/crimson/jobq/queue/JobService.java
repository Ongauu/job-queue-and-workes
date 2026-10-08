package com.crimson.jobq.queue;

import com.crimson.jobq.config.JobQueueProperties;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class JobService {
    private final JobRepository repo;
    private final JobQueueProperties.Backpressure limits;

    public JobService(JobRepository repo, JobQueueProperties props) {
        this.repo = repo;
        this.limits = props.backpressure();
    }

    public EnqueueResult enqueue(String clientId, String type, String payload, int maxAttempts, long delayMs) {
        Optional<Long> id = repo.enqueue(clientId, type, payload, maxAttempts, delayMs,
                limits.maxPending(), limits.maxPendingPerClient());
        if (id.isPresent())
            return new EnqueueResult(EnqueueResult.Outcome.ACCEPTED, id.get());

        boolean clientOverQuota = repo.countPendingForClient(clientId) >= limits.maxPendingPerClient();
        return new EnqueueResult(clientOverQuota ? EnqueueResult.Outcome.CLIENT_QUOTA : EnqueueResult.Outcome.QUEUE_FULL, null);
    }
}
