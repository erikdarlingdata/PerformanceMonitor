:pre
WITH ranked AS (
    SELECT
        database_name, query_id, plan_id, query_hash, execution_type_desc,
        MAX(module_name) AS module_name,
        replica_role,
        CAST(SUM(execution_count) AS bigint) AS total_executions,
        AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_reads,
        AVG(CAST(avg_logical_io_writes AS double precision)) AS avg_logical_writes,
        AVG(CAST(avg_physical_io_reads AS double precision)) AS avg_physical_reads,
        AVG(CAST(avg_rowcount AS double precision)) AS avg_rowcount,
        MAX(last_execution_time) AS last_execution_time,
        MAX(query_plan_hash) AS query_plan_hash
    FROM :src
    GROUP BY database_name, query_id, plan_id, query_hash, execution_type_desc, replica_role
    ORDER BY SUM(execution_count) * AVG(CAST(avg_duration_us AS double precision)) DESC
    LIMIT :top + 5
)
SELECT r.*, t.query_text
FROM ranked AS r
LEFT JOIN LATERAL (
    SELECT COALESCE(
        (SELECT x.query_sql_text FROM collect.query_store_text AS x WHERE x.server_id = 1 AND x.database_name = r.database_name AND x.query_id = r.query_id),
        (SELECT s.query_text FROM collect.query_store_stats AS s WHERE s.server_id = 1 AND s.query_id = r.query_id AND s.database_name = r.database_name AND s.query_text IS NOT NULL ORDER BY s.collection_time DESC LIMIT 1)
    ) AS query_text
) AS t ON TRUE
WHERE t.query_text IS NULL OR t.query_text NOT LIKE 'WAITFOR%'
ORDER BY r.total_executions * r.avg_duration_ms DESC
LIMIT :top;
