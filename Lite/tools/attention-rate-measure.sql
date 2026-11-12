-- attention-rate-measure.sql (#5736): the per-minute client attention rate, per server and for the fleet. The DuckDB twin of
-- Darling/tools/attention-rate-measure.sql: same buckets, same columns, same rule. The two are hand-written copies of one
-- definition; change one and change the other in the same commit (nothing fails if only one moves, and the bars are set from both).
--
-- Why: the Client Timeouts alert (#5640) first shipped OFF with provisional bars of 10 (Warning) and 60 (Critical) attentions
-- per minute, a guess. #5736 used this script on a week of field capture, made each bar the higher of its floor and a
-- multiple of the server's own baseline, and turned the alert on. Run it to see a store's measured normal rate. The bar-setting
-- rule is: take each server's p99.9 of the per-minute rate, then read the fleet spread of those p99.9 values (the fleet
-- row: min, median, max) and set the defaults from that.
--
-- How to run (the DuckDB CLI, on a read-only copy of the store, on the machine that ran Lite):
--
--   1. Close Lite first, so the store is checkpointed. A monitor.duckdb.wal file left beside the original means it is not: a
--      copy of the .duckdb alone is missing what the .wal holds (the newest minutes). Start Lite and close it again until no
--      .wal remains, then copy.
--   2. Copy only the monitor.duckdb file to a scratch folder. Do not copy the .wal. Do not copy the archive folder.
--   3. Run it on the same machine. The v_collection_log and v_attention_events views hold the ORIGINAL store's absolute archive
--      path (a parquet glob baked in when the store was opened), so a copy still reads the archive from the original store's
--      folder, wherever the copy sits; -readonly cannot repoint it. Moving the copy to another machine loses the archived weeks.
--
--   duckdb -readonly <copy of monitor.duckdb> -c "SET VARIABLE range_from = '2026-10-13 00:00:00'; SET VARIABLE range_to = '2026-10-20 00:00:00';" -f attention-rate-measure.sql
--
-- range_from and range_to are naive UTC timestamps (the clock of collection_time), range_from inclusive, range_to exclusive.
-- Both are required. Add -csv for a file the next change can read. The query reads the v_collection_log and v_attention_events
-- archive views, the same relations the alert reads, so a week that straddles an archive-and-reset still reads in full (with
-- no parquet files the views are the plain tables).
--
-- What one row means: per server, a bucket per calendar minute that had a successful collector run (v_collection_log,
-- collector_name = 'attention', status = 'SUCCESS'; the minute is date_trunc of collection_time). The bucket holds the sum of
-- the attention_events count rows in that minute (event_kind = 'count', the exact attentions since the prior run; the sample
-- rows are detail and never the total), and is zero when the run counted nothing. The attention collector runs once a minute
-- by default, so a minute holds one run. A minute with NO successful run has no bucket: the collector did not look, so it is
-- not a quiet minute, and inventing a zero for it would dilute the percentiles. A hole in collection therefore shows in the
-- three coverage columns below, not as quiet minutes. A count row in a minute with no successful run is not read (the run that
-- wrote it is not logged as a success, so the minute is not observed). The first run after a app restart stages the
-- counter and writes no count row, so that minute reads as quiet, the same known edge the alert has.
--
-- Columns:
--   scope                 server, or fleet for the last row
--   server_id, server_name
--   minutes_in_span       minutes from the first to the last run, inclusive (the window the collector could have covered);
--                         on the fleet row the sum over servers
--   minutes_with_a_run    buckets: minutes that had a successful run; every percentile below is computed over these;
--                         on the fleet row the sum over servers
--   longest_gap_minutes   the longest stretch of consecutive minutes inside the span with no run (0 when there is none);
--                         on the fleet row the largest over servers. Read it before trusting a p999: a long gap means the
--                         rate was not measured for that long
--   attentions            the sum of the count rows in the buckets; on the fleet row the sum over servers
--   p50, p99, p999        percentiles (continuous) of attentions per minute over the buckets
--   max_per_minute        the busiest minute
--   p999_3min_avg         the p99.9 of the 3-minute rolling average of the per-minute rate, the quantity the alert compares to
--                         its bars (it averages over about three minutes); only three consecutive run minutes make a window,
--                         so a hole breaks it; NULL when no such window exists
--   top_minutes           the three busiest minutes with a nonzero count, as "UTC minute=attentions; ..." (NULL when all are zero)
--   fleet_p999_min, fleet_p999_median, fleet_p999_max
--                         on the fleet row only: the spread of the per-server p999 across the servers above
--
-- Collected text is not read: no statement text, no application names, only counts and times.
-- #7061: this script sums the EXACT count rows and does not read sample rows, so it still counts events under 1 ms and Azure Data Studio metadata
-- queries. The alert's own baseline (AttentionWindowSql.BaselineMinutes) scales each minute by the share of that minute's sampled events that
-- are not left out, so on a store with that noise the alert's baseline is lower than the p999 and p999_3min_avg this script prints.

WITH
bounds AS (
    SELECT
        CAST(getvariable('range_from') AS TIMESTAMP) AS t_from,
        CAST(getvariable('range_to') AS TIMESTAMP) AS t_to
),
runs AS (
    SELECT
        l.server_id,
        max(l.server_name) AS server_name,
        date_trunc('minute', l.collection_time) AS minute
    FROM v_collection_log AS l
    CROSS JOIN bounds AS b
    WHERE l.collector_name = 'attention'
    AND   l.status = 'SUCCESS'
    AND   l.collection_time >= b.t_from
    AND   l.collection_time < b.t_to
    GROUP BY l.server_id, date_trunc('minute', l.collection_time)
),
counts AS (
    SELECT
        e.server_id,
        max(e.server_name) AS server_name,
        date_trunc('minute', e.collection_time) AS minute,
        CAST(sum(e.attention_count) AS BIGINT) AS attentions
    FROM v_attention_events AS e
    CROSS JOIN bounds AS b
    WHERE e.event_kind = 'count'
    AND   e.collection_time >= b.t_from
    AND   e.collection_time < b.t_to
    GROUP BY e.server_id, date_trunc('minute', e.collection_time)
),
per_minute AS (
    SELECT
        r.server_id,
        r.server_name,
        r.minute,
        coalesce(c.attentions, 0) AS attentions,
        lag(r.minute) OVER (PARTITION BY r.server_id ORDER BY r.minute) AS prior_run_minute
    FROM runs AS r
    LEFT JOIN counts AS c
        ON  c.server_id = r.server_id
        AND c.minute = r.minute
),
rolled AS (
    SELECT
        pm.*,
        CASE
            WHEN count(*) OVER w = 3
            AND  date_diff('minute', min(pm.minute) OVER w, pm.minute) = 2
            THEN avg(CAST(pm.attentions AS DOUBLE)) OVER w
        END AS avg_3min,
        row_number() OVER (PARTITION BY pm.server_id ORDER BY pm.attentions DESC, pm.minute) AS busiest
    FROM per_minute AS pm
    WINDOW w AS (PARTITION BY pm.server_id ORDER BY pm.minute ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
per_server AS (
    SELECT
        r.server_id,
        max(r.server_name) AS server_name,
        date_diff('minute', min(r.minute), max(r.minute)) + 1 AS minutes_in_span,
        count(*) AS minutes_with_a_run,
        coalesce(max(date_diff('minute', r.prior_run_minute, r.minute) - 1), 0) AS longest_gap_minutes,
        CAST(sum(r.attentions) AS BIGINT) AS attentions,
        quantile_cont(r.attentions, 0.5) AS p50,
        quantile_cont(r.attentions, 0.99) AS p99,
        quantile_cont(r.attentions, 0.999) AS p999,
        max(r.attentions) AS max_per_minute,
        quantile_cont(r.avg_3min, 0.999) AS p999_3min_avg,
        string_agg(
            strftime(r.minute, '%Y-%m-%d %H:%M') || '=' || CAST(r.attentions AS VARCHAR),
            '; ' ORDER BY r.busiest) FILTER (WHERE r.busiest <= 3 AND r.attentions > 0) AS top_minutes
    FROM rolled AS r
    GROUP BY r.server_id
)
SELECT
    'server' AS scope,
    p.server_id,
    p.server_name,
    p.minutes_in_span,
    p.minutes_with_a_run,
    p.longest_gap_minutes,
    p.attentions,
    round(CAST(p.p50 AS DECIMAL(18, 4)), 2) AS p50,
    round(CAST(p.p99 AS DECIMAL(18, 4)), 2) AS p99,
    round(CAST(p.p999 AS DECIMAL(18, 4)), 2) AS p999,
    p.max_per_minute,
    round(CAST(p.p999_3min_avg AS DECIMAL(18, 4)), 2) AS p999_3min_avg,
    p.top_minutes,
    CAST(NULL AS DECIMAL(18, 2)) AS fleet_p999_min,
    CAST(NULL AS DECIMAL(18, 2)) AS fleet_p999_median,
    CAST(NULL AS DECIMAL(18, 2)) AS fleet_p999_max
FROM per_server AS p
UNION ALL
SELECT
    'fleet',
    NULL,
    '(' || CAST(count(*) AS VARCHAR) || ' servers)',
    CAST(sum(p.minutes_in_span) AS BIGINT),
    CAST(sum(p.minutes_with_a_run) AS BIGINT),
    max(p.longest_gap_minutes),
    CAST(sum(p.attentions) AS BIGINT),
    NULL,
    NULL,
    NULL,
    max(p.max_per_minute),
    NULL,
    NULL,
    round(CAST(min(p.p999) AS DECIMAL(18, 4)), 2),
    round(CAST(quantile_cont(p.p999, 0.5) AS DECIMAL(18, 4)), 2),
    round(CAST(max(p.p999) AS DECIMAL(18, 4)), 2)
FROM per_server AS p
HAVING count(*) > 0
ORDER BY scope DESC, server_id;
