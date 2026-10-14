/*
#5794 load test, step (c): the write path. One row per Query Store collector over the last :hours hours (default 2)
of the collection log. Start the service on the inflated copy first and let it run at least one cycle.
duration_ms is the whole run, sql_ms the read from the monitored server, and store_ms (the duckdb_duration_ms
column, kept under its old name) the write to this store.
*/
\if :{?hours}
\else
    \set hours 2
\endif
SELECT
    l.collector_name,
    count(*) AS runs,
    count(*) FILTER (WHERE lower(l.status) <> 'success') AS not_success,
    round(avg(l.rows_collected)) AS avg_rows,
    round(avg(l.duration_ms)) AS avg_duration_ms,
    max(l.duration_ms) AS max_duration_ms,
    round(avg(l.sql_duration_ms)) AS avg_sql_ms,
    round(avg(l.duckdb_duration_ms)) AS avg_store_ms,
    max(l.duckdb_duration_ms) AS max_store_ms
FROM collect.collection_log AS l
WHERE l.collector_name LIKE 'query_store%'
AND   l.collection_time >= (now() AT TIME ZONE 'UTC') - make_interval(hours => :hours)
GROUP BY l.collector_name
ORDER BY l.collector_name;
