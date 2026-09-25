\timing on
SET search_path = collect, public;
TRUNCATE query_store_stats;
TRUNCATE query_store_text;

/* 8 days of hourly Query Store intervals, one server, 4 databases, 15,000 (query, plan) pairs, ~30% active per
   hour, Regular + ~4% Aborted + ~2% Exception outcomes. Each interval identity is stored k = 1..4 times (mean 2.0):
   k-1 open-interval re-fetches plus the closing fetch at start + 65 min. 0.5% of closing fetches carry a #1907
   in-memory sliver (same key, same collection_time, smaller execution_count). 0.1% NULL first_execution_time.
   Hours 24-29 (within the newest 8 days) are legacy (NULL runtime_stats_interval_id / interval_start_time_utc). db3 is replica_role
   'Primary', the rest NULL. */
WITH params AS
(
    SELECT date_trunc('hour', now() AT TIME ZONE 'UTC') - interval '360 hours' AS t0,
           now() AT TIME ZONE 'UTC' AS t_now
),
ident AS
(
    SELECT
        p, h, e,
        p % 4 AS d,
        pr.t0 + make_interval(hours => h) AS s,
        pr.t_now,
        CASE e WHEN 0 THEN 'Regular' WHEN 1 THEN 'Aborted' ELSE 'Exception' END AS etype,
        1 + CASE WHEN ((p::bigint * 2654435761 + h * 40503 + e * 977) % 100) < 35 THEN 0
                 WHEN ((p::bigint * 2654435761 + h * 40503 + e * 977) % 100) < 75 THEN 1
                 WHEN ((p::bigint * 2654435761 + h * 40503 + e * 977) % 100) < 90 THEN 2
                 ELSE 3 END AS k,
        1 + ((p::bigint * 37 + h * 11 + e * 5) % 500) AS base_ec
    FROM params AS pr
    CROSS JOIN generate_series(0, 359) AS h
    CROSS JOIN generate_series(0, 14999) AS p
    CROSS JOIN generate_series(0, 2) AS e
    WHERE ((p::bigint * 7919 + h * 104729) % 100) < 30
    AND   (e = 0
           OR (e = 1 AND ((p::bigint * 31 + h * 7) % 100) < 4)
           OR (e = 2 AND ((p::bigint * 17 + h * 3) % 100) < 2))
),
snap AS
(
    SELECT i.*, j,
        CASE WHEN j = i.k THEN i.s + interval '65 minutes'
             ELSE i.s + make_interval(mins => 5 * ((j * 11) / i.k)) END
          + make_interval(secs => i.d * 2) AS ct,
        GREATEST(1, (i.base_ec * j) / i.k) AS ec,
        false AS sliver
    FROM ident AS i
    CROSS JOIN LATERAL generate_series(1, i.k) AS j
    UNION ALL
    SELECT i.*, i.k,
        i.s + interval '65 minutes' + make_interval(secs => i.d * 2),
        i.base_ec / 10,
        true
    FROM ident AS i
    WHERE ((i.p::bigint * 3 + i.h) % 200) = 0
)
INSERT INTO query_store_stats
(
    collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc,
    first_execution_time, last_execution_time, module_name, query_text, query_hash, execution_count,
    avg_duration_us, min_duration_us, max_duration_us, avg_cpu_time_us, min_cpu_time_us, max_cpu_time_us,
    avg_logical_io_reads, min_logical_io_reads, max_logical_io_reads, avg_logical_io_writes, min_logical_io_writes, max_logical_io_writes,
    avg_physical_io_reads, min_physical_io_reads, max_physical_io_reads, avg_clr_time_us, min_clr_time_us, max_clr_time_us,
    min_dop, max_dop, avg_query_max_used_memory, min_query_max_used_memory, max_query_max_used_memory,
    avg_rowcount, min_rowcount, max_rowcount, avg_num_physical_io_reads, min_num_physical_io_reads, max_num_physical_io_reads,
    avg_log_bytes_used, min_log_bytes_used, max_log_bytes_used, avg_tempdb_space_used, min_tempdb_space_used, max_tempdb_space_used,
    plan_type, plan_forcing_type, is_forced_plan, force_failure_count, last_force_failure_reason, compatibility_level,
    query_plan_text, query_plan_hash, replica_role, runtime_stats_interval_id, interval_start_time_utc
)
SELECT
    extract(epoch FROM x.ct)::bigint,
    x.ct,
    1,
    'SQLPROD01',
    'db' || x.d,
    1000 + ((x.p / 4) * 10) / 13,
    50000 + x.p,
    x.etype,
    CASE WHEN ((x.p::bigint * 7 + x.h) % 1000) = 0 THEN NULL ELSE x.s + make_interval(mins => (x.p * 13 + x.h) % 5) END,
    x.ct - interval '30 seconds',
    CASE WHEN x.p % 3 = 0 THEN 'dbo.usp_proc_' || (x.p / 3) % 400 END,
    NULL,
    '0x' || lpad(to_hex(1000 + ((x.p / 4) * 10) / 13), 16, '0'),
    x.ec,
    m.dur, m.dur / 3, m.dur * 4, m.cpu, m.cpu / 3, m.cpu * 4,
    m.rd, m.rd / 2, m.rd * 3, m.wr, m.wr / 2, m.wr * 3,
    m.pr, 0, m.pr * 5, m.clr, 0, m.clr * 2,
    1, 1 + (x.p % 8), m.mem, m.mem / 2, m.mem * 2,
    m.rc, 0, m.rc * 10, m.pr, 0, m.pr * 2,
    m.log, 0, m.log * 3, m.tmp, 0, m.tmp * 2,
    'Compiled Plan', 'NONE', (x.p % 97 = 0), (x.p % 97 = 0)::int * (x.h % 3), CASE WHEN x.p % 97 = 0 THEN 'NONE' END, 160,
    NULL,
    '0x' || lpad(to_hex(50000 + x.p), 16, '0'),
    CASE WHEN x.d = 3 THEN 'Primary' END,
    CASE WHEN x.h BETWEEN 24 AND 29 THEN NULL ELSE 10000 + x.h END,
    CASE WHEN x.h BETWEEN 24 AND 29 THEN NULL ELSE x.s END
FROM snap AS x
CROSS JOIN LATERAL
(
    SELECT
        1000 + ((x.p::bigint * 97 + x.h * 13 + x.j * 7 + x.e * 3) % 500000) AS dur,
        500 + ((x.p::bigint * 89 + x.h * 17 + x.j * 5) % 300000) AS cpu,
        10 + ((x.p::bigint * 53 + x.h * 19 + x.j) % 90000) AS rd,
        ((x.p::bigint * 29 + x.h * 23 + x.j) % 900) AS wr,
        ((x.p::bigint * 31 + x.h + x.j) % 5000) AS pr,
        ((x.p::bigint * 11 + x.j) % 50) AS clr,
        ((x.p::bigint * 41 + x.h + x.j) % 2000) AS mem,
        ((x.p::bigint * 43 + x.h * 3 + x.j) % 10000) AS rc,
        ((x.p::bigint * 47 + x.h + x.j) % 100000) AS log,
        ((x.p::bigint * 59 + x.h + x.j) % 800) AS tmp
) AS m
WHERE x.ct <= x.t_now;

INSERT INTO query_store_text (server_id, database_name, query_id, query_sql_text, last_seen, query_hash)
SELECT DISTINCT ON (database_name, query_id)
    1, database_name, query_id,
    'SELECT /* q' || query_id || ' */ ' || repeat('c.column_name, ', 25) || ' FROM dbo.some_table AS c WHERE c.id = @p1',
    now() AT TIME ZONE 'UTC', query_hash
FROM query_store_stats
ORDER BY database_name, query_id;

ANALYZE query_store_stats;
ANALYZE query_store_text;

SELECT count(*) AS raw_rows,
       count(DISTINCT (database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role)) AS identities
FROM query_store_stats
WHERE collection_time >= (now() AT TIME ZONE 'UTC') - interval '24 hours';
