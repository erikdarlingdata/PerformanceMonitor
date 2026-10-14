/*
#5794 load test, step (b), last table: the TimescaleDB background jobs. One row per continuous aggregate refresh job
and per compression job, slowest first. These jobs do not read the Query Store interval tables, which are plain
partitioned tables, so their times should not change with N: they are here to show that. The hourly work that does
grow with N is the service's own Query Store builders, which the Jobs step reads from the service log.
hourly_total is the sum of last_run_duration over the jobs that run every hour.
*/
SELECT
    j.job_id,
    j.proc_name,
    coalesce(ca.view_schema || '.' || ca.view_name, j.hypertable_schema || '.' || j.hypertable_name) AS relation,
    j.schedule_interval,
    s.last_run_status,
    s.last_run_duration,
    s.total_runs,
    s.total_failures,
    sum(s.last_run_duration) FILTER (WHERE j.schedule_interval = interval '1 hour') OVER () AS hourly_total
FROM timescaledb_information.jobs AS j
JOIN timescaledb_information.job_stats AS s
  ON s.job_id = j.job_id
LEFT JOIN timescaledb_information.continuous_aggregates AS ca
  ON  ca.materialization_hypertable_schema = j.hypertable_schema
  AND ca.materialization_hypertable_name = j.hypertable_name
WHERE j.proc_name IN ('policy_refresh_continuous_aggregate', 'policy_compression')
ORDER BY s.last_run_duration DESC NULLS LAST, j.job_id;
