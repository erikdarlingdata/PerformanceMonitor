:pre
WITH deduped AS (
    SELECT collection_time, interval_start_time_utc, query_id, execution_count, avg_cpu_time_us, avg_duration_us,
           avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                              ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM collect.query_store_stats
    WHERE server_id = 1
    AND   COALESCE(interval_start_time_utc, collection_time) >= :ws
    AND   COALESCE(interval_start_time_utc, collection_time) <= :wend
    AND   collection_time >= :ws::timestamp - interval '1 hour'
    AND   collection_time <= :wend::timestamp + interval '30 days'
)
SELECT date_trunc('hour', COALESCE(interval_start_time_utc, collection_time)) AS bucket,
       COUNT(DISTINCT query_id) AS query_count,
       COALESCE(SUM(CAST(avg_cpu_time_us AS double precision) * execution_count), 0) / 1000.0 AS total_cpu_ms,
       COALESCE(SUM(CAST(avg_duration_us AS double precision) * execution_count), 0) / 1000.0 AS total_duration_ms,
       COALESCE(SUM(CAST(avg_logical_io_reads AS double precision) * execution_count), 0) AS total_reads,
       COALESCE(SUM(CAST(avg_logical_io_writes AS double precision) * execution_count), 0) AS total_writes,
       COALESCE(SUM(CAST(avg_physical_io_reads AS double precision) * execution_count), 0) AS total_physical_reads
FROM deduped
WHERE rn = 1
GROUP BY date_trunc('hour', COALESCE(interval_start_time_utc, collection_time))
ORDER BY bucket;
