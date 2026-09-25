:pre
WITH ranked AS (
    SELECT
        database_name, query_id, plan_id, query_hash, replica_role, execution_type_desc,
        MAX(module_name) AS module_name,
        CAST(SUM(execution_count) AS bigint) AS total_executions,
        AVG(CAST(avg_duration_us AS double precision)) / 1000.0 AS avg_duration_ms,
        AVG(CAST(avg_cpu_time_us AS double precision)) / 1000.0 AS avg_cpu_time_ms,
        AVG(CAST(avg_logical_io_reads AS double precision)) AS avg_logical_reads,
        AVG(CAST(avg_logical_io_writes AS double precision)) AS avg_logical_writes,
        AVG(CAST(avg_physical_io_reads AS double precision)) AS avg_physical_reads,
        AVG(CAST(avg_rowcount AS double precision)) AS avg_rowcount,
        MIN(min_dop) AS min_dop,
        MAX(max_dop) AS max_dop,
        MAX(last_execution_time) AS last_execution_time,
        MAX(query_plan_hash) AS query_plan_hash,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(plan_forcing_type) AS plan_forcing_type,
        MIN(first_execution_time) AS first_execution_time,
        AVG(CAST(avg_clr_time_us AS double precision)) / 1000.0 AS avg_clr_time_ms,
        AVG(CAST(avg_tempdb_space_used AS double precision)) AS avg_tempdb_space_used,
        AVG(CAST(avg_log_bytes_used AS double precision)) AS avg_log_bytes_used,
        MAX(plan_type) AS plan_type,
        MAX(force_failure_count) AS force_failure_count,
        MAX(last_force_failure_reason) AS last_force_failure_reason,
        MAX(compatibility_level) AS compatibility_level,
        MIN(CAST(min_duration_us AS double precision)) / 1000.0 AS min_duration_ms,
        MAX(CAST(max_duration_us AS double precision)) / 1000.0 AS max_duration_ms,
        MIN(CAST(min_cpu_time_us AS double precision)) / 1000.0 AS min_cpu_time_ms,
        MAX(CAST(max_cpu_time_us AS double precision)) / 1000.0 AS max_cpu_time_ms,
        MIN(CAST(min_logical_io_reads AS double precision)) AS min_logical_reads,
        MAX(CAST(max_logical_io_reads AS double precision)) AS max_logical_reads,
        MIN(CAST(min_logical_io_writes AS double precision)) AS min_logical_writes,
        MAX(CAST(max_logical_io_writes AS double precision)) AS max_logical_writes,
        MIN(CAST(min_physical_io_reads AS double precision)) AS min_physical_reads,
        MAX(CAST(max_physical_io_reads AS double precision)) AS max_physical_reads,
        MIN(CAST(min_clr_time_us AS double precision)) / 1000.0 AS min_clr_time_ms,
        MAX(CAST(max_clr_time_us AS double precision)) / 1000.0 AS max_clr_time_ms,
        MIN(CAST(min_rowcount AS double precision)) AS min_rowcount,
        MAX(CAST(max_rowcount AS double precision)) AS max_rowcount,
        MIN(CAST(min_log_bytes_used AS double precision)) AS min_log_bytes_used,
        MAX(CAST(max_log_bytes_used AS double precision)) AS max_log_bytes_used,
        MIN(CAST(min_tempdb_space_used AS double precision)) AS min_tempdb_space_used,
        MAX(CAST(max_tempdb_space_used AS double precision)) AS max_tempdb_space_used,
        AVG(CAST(avg_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS avg_memory_mb,
        MIN(CAST(min_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS min_memory_mb,
        MAX(CAST(max_query_max_used_memory AS double precision)) * 8.0 / 1024.0 AS max_memory_mb,
        AVG(CAST(avg_num_physical_io_reads AS double precision)) AS avg_num_physical_io_reads,
        MIN(CAST(min_num_physical_io_reads AS double precision)) AS min_num_physical_io_reads,
        MAX(CAST(max_num_physical_io_reads AS double precision)) AS max_num_physical_io_reads
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
