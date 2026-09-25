:pre
SELECT
    database_name, query_id, plan_id, replica_role,
    any_value(query_plan_hash) AS query_plan_hash,
    SUM(execution_count) AS execs,
    SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
    SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
    MAX(last_execution_time) AS last_exec,
    bool_or(is_forced_plan) AS is_forced_plan,
    MAX(force_failure_count) AS force_failure_count
FROM :srctbl
WHERE server_id = 1
:etypefilter
AND   last_execution_time >= :lastexec
AND   collection_time >= :collbound
AND   first_execution_time >= :collbound
GROUP BY database_name, query_id, plan_id, replica_role;
