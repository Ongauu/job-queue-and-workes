package com.crimson.jobq.queue;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Repository
public class JobRepository {
    private final JdbcTemplate jdbc;

    public JobRepository(JdbcTemplate jdbc){
        this.jdbc = jdbc;
    }

    public Optional<Long> enqueue(String clientId, String type, String payload, int maxAttempts, long delayMs, int maxPending, int maxPendingPerClient){
        List<Long> ids = jdbc.queryForList("""
                INSERT INTO jobs (client_id, type, payload, max_attempts, run_at)
                SELECT ?, ?, ?, ?::integer, now() + make_interval(secs => ?::double precision)
                WHERE (SELECT count(*) FROM jobs WHERE status = 'PENDING') < ?
                  AND (SELECT count(*) FROM jobs WHERE status = 'PENDING' AND client_id = ?) < ?
                RETURNING id
                """, Long.class,
                clientId, type, payload, maxAttempts, delayMs / 1000.0,
                maxPending, clientId, maxPendingPerClient);
        return ids.stream().findFirst();
    }

    public Optional<ClaimedJob> claim(String workerId, int leaseSeconds){
        List<ClaimedJob> rows = jdbc.query("""
                UPDATE jobs
                   SET status = 'RUNNING',
                       attempts = attempts + 1,
                       locked_by = ?,
                       locked_until = now() + make_interval(secs => ?::double precision),
                       updated_at = now()
                 WHERE id = (SELECT id FROM jobs
                              WHERE status = 'PENDING' AND run_at <= now()
                              ORDER BY run_at, id
                              LIMIT 1
                              FOR UPDATE SKIP LOCKED)
                RETURNING id, client_id, type, payload, attempts, max_attempts
                """,
                (rs, i) -> new ClaimedJob(rs.getLong("id"), rs.getString("client_id"), rs.getString("type"),
                        rs.getString("payload"), rs.getInt("attempts"), rs.getInt("max_attempts")),
                workerId, (double) leaseSeconds);
        return rows.stream().findFirst();
    }

    public boolean complete(long id, String workerId) {
        return jdbc.update("""
                UPDATE jobs SET status = 'DONE', finished_at = now(), updated_at = now(),
                                locked_by = NULL, locked_until = NULL, last_error = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ?
                """, id, workerId) == 1;
    }

    public boolean scheduleRetry(long id, String workerId, String error, long delayMs) {
        return jdbc.update("""
                UPDATE jobs SET status = 'PENDING', run_at = now() + make_interval(secs => ?::double precision),
                                last_error = ?, updated_at = now(), locked_by = NULL, locked_until = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ?
                """, delayMs / 1000.0, error, id, workerId) == 1;
    }

    public boolean markDead(long id, String workerId, String error) {
        return jdbc.update("""
                UPDATE jobs SET status = 'DEAD', last_error = ?, finished_at = now(), updated_at = now(),
                                locked_by = NULL, locked_until = NULL
                 WHERE id = ? AND status = 'RUNNING' AND locked_by = ?
                """, error, id, workerId) == 1;
    }

    public int reclaimExpired() {
        return jdbc.update("""
                UPDATE jobs
                   SET status = CASE WHEN attempts >= max_attempts THEN 'DEAD' ELSE 'PENDING' END,
                       finished_at = CASE WHEN attempts >= max_attempts THEN now() ELSE NULL END,
                       run_at = now(), last_error = 'lease expired (worker crashed or too slow)',
                       locked_by = NULL, locked_until = NULL, updated_at = now()
                 WHERE status = 'RUNNING' AND locked_until < now()
                """);
    }

    public int countPendingForClient(String clientId) {
        Integer n = jdbc.queryForObject(
                "SELECT count(*) FROM jobs WHERE status = 'PENDING' AND client_id = ?", Integer.class, clientId);
        return n == null ? 0 : n;
    }

    public Optional<JobView> find(long id) {
        return jdbc.query("SELECT * FROM jobs WHERE id = ?", this::mapView, id).stream().findFirst();
    }

    public Map<String, Long> countByStatus() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (String s : List.of("PENDING", "RUNNING", "DONE", "DEAD")) out.put(s, 0L);
        jdbc.query("SELECT status, count(*) AS n FROM jobs GROUP BY status",
                rs -> { out.put(rs.getString("status"), rs.getLong("n")); });
        return out;
    }

    public double oldestReadyAgeSeconds() {
        Double v = jdbc.queryForObject("""
                SELECT COALESCE(EXTRACT(EPOCH FROM now() - min(run_at)), 0)::double precision
                  FROM jobs WHERE status = 'PENDING' AND run_at <= now()
                """, Double.class);
        return v == null ? 0 : v;
    }

    private JobView mapView(ResultSet rs, int i) throws SQLException {
        return new JobView(rs.getLong("id"), rs.getString("client_id"), rs.getString("type"),
                rs.getString("payload"), rs.getString("status"), rs.getInt("attempts"), rs.getInt("max_attempts"),
                instant(rs.getTimestamp("run_at")), rs.getString("last_error"),
                instant(rs.getTimestamp("created_at")), instant(rs.getTimestamp("finished_at")));
    }

    private static Instant instant(Timestamp t) {
        return t == null ? null : t.toInstant();
    }
}
