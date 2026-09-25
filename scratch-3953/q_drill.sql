:pre
SELECT
    database_name, query_id, replica_role, query_plan_hash,
    MAX(plan_id) AS plan_id,
    any_value(query_hash) AS query_hash,
    SUM(execution_count) AS execs,
    SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
    SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
    MAX(last_execution_time) AS last_exec
FROM :srctbl
WHERE server_id = 1
:etypefilter
AND   last_execution_time >= :lastexec
AND   collection_time >= :collbound
AND   first_execution_time >= :collbound
GROUP BY database_name, query_id, replica_role, query_plan_hash
HAVING SUM(execution_count) >= 25;
