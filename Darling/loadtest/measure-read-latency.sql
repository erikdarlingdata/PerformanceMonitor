/*
#5794 load test, step (a), part 2: the read times the service recorded, beside the read budgets. The service writes
collect.read_latency once an hour, so run this after the hour that held the reads has closed. :since is the UTC
start of the reads, as measure.ps1 prints it.
Budgets: a Custom Views panel (surface Compose) has 45 s, 75% of its 60 s statement timeout (ComposeSpec).
Every other read has 5 s, the time at which the service records it as a slow read (SlowReadLog.DefaultThresholdMs).
*/
SELECT
    r.surface,
    r.route,
    r.window_band,
    r.outcome,
    sum(r.run_count) AS runs,
    round(sum(r.total_ms)::numeric / nullif(sum(r.run_count), 0)) AS avg_ms,
    max(r.max_ms) AS max_ms,
    CASE WHEN r.surface ILIKE 'compose%' THEN 45000 ELSE 5000 END AS budget_ms,
    CASE WHEN max(r.max_ms) <= CASE WHEN r.surface ILIKE 'compose%' THEN 45000 ELSE 5000 END
         THEN 'within' ELSE 'OVER' END AS verdict,
    max(r.max_in_flight) AS max_in_flight,
    sum(r.wait_total_ms) AS wait_total_ms
FROM collect.read_latency AS r
WHERE r.metric_time >= date_trunc('hour', :'since'::timestamp)
GROUP BY r.surface, r.route, r.window_band, r.outcome
ORDER BY max(r.max_ms) DESC;
