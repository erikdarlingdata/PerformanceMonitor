/*
#5794 load test, step (a), part 2: the read times the service recorded, beside the read budgets. The service writes
collect.read_latency once an hour, so run this after the hour that held the reads has closed. :since is the UTC
start of the reads, as measure.ps1 prints it.
Budgets: a Custom Views panel (surface Compose) has 45 s, 75% of its 60 s statement timeout (ComposeSpec).
Every other read has 5 s, the time at which the service records it as a slow read (SlowReadLog.DefaultThresholdMs).
#5506 part 3: the rows that are not a read running (queue_wait, busy, cache_hit, cancelled) are counted in their own
columns and kept out of runs, avg_ms, max_ms, the verdict and wait_total_ms. wait_total_ms is the wait for a store
connection only; the time a read waited for a long-read slot is queue_wait_ms, from the queue_wait rows (one per read
that waited, its elapsed being that wait). A run passes with ok, fallback_raw, gate_failed, queue_wait and cache_hit rows;
any busy or cancelled row fails it.
*/
SELECT
    r.surface,
    r.route,
    r.window_band,
    r.outcome,
    sum(r.run_count) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')) AS runs,
    round(sum(r.total_ms) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled'))::numeric
        / nullif(sum(r.run_count) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')), 0)) AS avg_ms,
    max(r.max_ms) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')) AS max_ms,
    CASE WHEN r.surface ILIKE 'compose%' THEN 45000 ELSE 5000 END AS budget_ms,
    CASE WHEN r.outcome IN ('busy', 'cancelled') THEN 'FAILED'
         WHEN r.outcome IN ('queue_wait', 'cache_hit') THEN 'not a run'
         WHEN max(r.max_ms) <= CASE WHEN r.surface ILIKE 'compose%' THEN 45000 ELSE 5000 END
         THEN 'within' ELSE 'OVER' END AS verdict,
    max(r.max_in_flight) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')) AS max_in_flight,
    sum(r.wait_total_ms) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')) AS wait_total_ms,
    sum(r.run_count) FILTER (WHERE r.outcome = 'queue_wait') AS queued,
    sum(r.total_ms) FILTER (WHERE r.outcome = 'queue_wait') AS queue_wait_ms,
    sum(r.run_count) FILTER (WHERE r.outcome IN ('busy', 'cache_hit', 'cancelled')) AS not_run
FROM collect.read_latency AS r
WHERE r.metric_time >= date_trunc('hour', :'since'::timestamp)
GROUP BY r.surface, r.route, r.window_band, r.outcome
ORDER BY max(r.max_ms) FILTER (WHERE r.outcome NOT IN ('queue_wait', 'busy', 'cache_hit', 'cancelled')) DESC NULLS LAST,
         r.surface, r.route, r.window_band, r.outcome;
