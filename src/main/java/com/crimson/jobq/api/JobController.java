package com.crimson.jobq.api;

import com.crimson.jobq.config.JobQueueProperties;
import com.crimson.jobq.queue.EnqueueResult;
import com.crimson.jobq.queue.JobRepository;
import com.crimson.jobq.queue.JobService;
import com.crimson.jobq.ratelimit.ClientIds;
import com.crimson.jobq.worker.JobHandlerRegistry;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.JsonNode;

import java.net.URI;
import java.util.Map;

@RestController
@RequestMapping("/api")
public class JobController {
    public record EnqueueRequest(
            @NotBlank String type,
            JsonNode payload,
            @Min(1) @Max(20) Integer maxAttempts,
            @Min(0) Long delayMs
    ) {}

    private final JobService service;
    private final JobRepository repo;
    private final JobHandlerRegistry registry;
    private final int retryAfterSeconds;

    public JobController(JobService service, JobRepository repo, JobHandlerRegistry registry, JobQueueProperties props) {
        this.service = service;
        this.repo = repo;
        this.registry = registry;
        this.retryAfterSeconds = props.backpressure().retryAfterSeconds();
    }

    @PostMapping("/jobs")
    public ResponseEntity<?> enqueue(@Valid @RequestBody EnqueueRequest req, HttpServletRequest http) {
        if (!registry.supports(req.type())) {
            return ResponseEntity.badRequest()
                    .body(Map.of("error", "unknown_job_type", "supportedTypes", registry.types()));
        }
        String payload = req.payload() == null ? "{}" : req.payload().toString();
        EnqueueResult r = service.enqueue(ClientIds.resolve(http), req.type(), payload,
                req.maxAttempts() == null ? 5 : req.maxAttempts(),
                req.delayMs() == null ? 0 : req.delayMs());

        return switch (r.outcome()) {
            case ACCEPTED -> ResponseEntity.accepted()
                    .location(URI.create("/api/jobs/" + r.id()))
                    .body(Map.of("id", r.id(), "status", "PENDING"));
            case CLIENT_QUOTA -> ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS)
                    .header("Retry-After", String.valueOf(retryAfterSeconds))
                    .body(Map.of("error", "client_queue_quota_exceeded"));
            case QUEUE_FULL -> ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                    .header("Retry-After", String.valueOf(retryAfterSeconds))
                    .body(Map.of("error", "queue_full"));
        };
    }

    @GetMapping("/jobs/{id}")
    public ResponseEntity<?> get(@PathVariable long id) {
        return repo.find(id).<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
