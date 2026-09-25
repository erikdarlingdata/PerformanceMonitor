:pre
SELECT date_trunc('hour', COALESCE(interval_start_time_utc, collection_time)) AS bucket,
       COUNT(DISTINCT query_id) AS query_count,
       COALESCE(SUM(CAST(avg_cpu_time_us AS double precision) * execution_count), 0) / 1000.0 AS total_cpu_ms,
       COALESCE(SUM(CAST(avg_duration_us AS double precision) * execution_count), 0) / 1000.0 AS total_duration_ms,
       COALESCE(SUM(CAST(avg_logical_io_reads AS double precision) * execution_count), 0) AS total_reads,
       COALESCE(SUM(CAST(avg_logical_io_writes AS double precision) * execution_count), 0) AS total_writes,
       COALESCE(SUM(CAST(avg_physical_io_reads AS double precision) * execution_count), 0) AS total_physical_reads
FROM :srctbl
WHERE server_id = 1
AND   COALESCE(interval_start_time_utc, collection_time) >= :ws
AND   COALESCE(interval_start_time_utc, collection_time) <= :wend
GROUP BY date_trunc('hour', COALESCE(interval_start_time_utc, collection_time))
ORDER BY bucket;
