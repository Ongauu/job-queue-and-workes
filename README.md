 jobq - a request queue + rate limiter, built from scratch

Java  / Spring Boot  / PostgreSQL / Maven. No Redis, Kafka, RabbitMQ, Bucket4j, Resilience4j, etc.

Run it:

bash
mvn spring-boot:run

bash
curl -i -X POST localhost:8080/api/jobs -H 'Content-Type: application/json' \
     -H 'X-Client-Id: alice' -d '{"type":"echo","payload":{"hello":"world"}}'
curl localhost:8080/api/jobs/1
curl localhost:8080/api/stats


Job types: `echo` (instant), `slow` (`{"ms":3000}`), `flaky` (`{"succeedOnAttempt":3}`), `poison` (always dies).

## Where each concept lives

| Concept                | Code                                                                                                                  |
|------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Rate limiting (reject) | `ratelimit/TokenBucketRateLimiter`, `SlidingWindowLogRateLimiter`, `RateLimitInterceptor` -> HTTP 429 + `Retry-After` |
| Timing / pacing (wait) | `ratelimit/OutboundThrottle` used by `WorkerPool`                                                                     |
| Queue                  | `queue/JobRepository` - one table, one SQL statement per state change                                                 |
| Concurrency            | `claim()` with `FOR UPDATE SKIP LOCKED`; per-bucket `synchronized` in the limiters                                    |
| Workers                | `worker/WorkerPool` - N threads running throttle -> claim -> process                                                  |
| Backpressure           | `JobRepository.enqueue` (queue depth caps) -> 429/503; workers pull only when free                                    |
| Retries                | `RetryBackoff` (exponential + jitter), `scheduleRetry`, `markDead`, `NonRetryableException`                           |
| Crash recovery         | leases (`locked_until`) + `reclaimExpired()` run by `MaintenanceTasks`                                                |

Job lifecycle: `PENDING -> RUNNING -> DONE`, a failure goes back to `PENDING` with a future `run_at`,
and out of attempts (or non-retryable) goes to `DEAD`.

## Experiments

1. **Token bucket**: `scripts/burst.sh 30` -> about 10 x `202` (the burst), the rest `429`. Wait, repeat.
   Check `Retry-After` and `X-RateLimit-Remaining` with `curl -i`.
2. **Swap algorithms**: set `jobq.api.algorithm: sliding-window`, repeat. Notice there is no refill drip, a hard window.
3. **Worker pacing**: set `outbound-rate-per-second: 2`, `outbound-burst: 1`, enqueue 20 `echo` jobs, watch the logs
   tick at 2 jobs/s even with 4 workers. Look at `queueLagSeconds` in `/api/stats`.
4. **Backpressure**: set `max-pending: 20`, enqueue many `slow` jobs. First you see 429 (your client quota), raise
   `max-pending-per-client` and use several `X-Client-Id` values to see 503 `queue_full`.
5. **Retries**: enqueue `{"type":"flaky","payload":{"succeedOnAttempt":4}}`. Query the row in psql between attempts and
   watch `attempts`, `run_at`, `last_error`. Then `succeedOnAttempt: 9` with default 5 attempts -> `DEAD`.
6. **Crash recovery**: enqueue a `slow` job (`ms: 60000`), `kill -9` the app mid-job, restart. After the lease
   (30s) the job is reclaimed. `attempts` shows it still consumed one attempt.
7. **Two instances**: run a second copy with `--server.port=8081`. Both claim from one table without duplicates.

## Honest limitations (worth thinking about)

- The API limiter is **per instance** (in memory). Run 3 instances and each allows the full rate. A shared limiter
  needs shared state - the very thing Redis is usually used for. Try building one on a Postgres table and measure
  the cost of a DB round-trip per request.
- Queue depth limits are **soft** under concurrency (see comment in `enqueue`).
- Delivery is **at-least-once**: a job can run twice (lease expiry, crash after work but before `complete`).
  Handlers should be idempotent.
- Workers **poll**. Latency vs DB load is tuned by `poll-interval-ms` and idle backoff. `LISTEN/NOTIFY` is the next step.
- `DONE`/`DEAD` rows are never deleted; add a retention job.
- `X-Client-Id` is spoofable; real systems use authenticated identity.
