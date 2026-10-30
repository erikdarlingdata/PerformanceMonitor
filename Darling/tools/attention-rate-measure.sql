-- attention-rate-measure.sql (#5736): the per-minute client attention rate, per server and for the fleet.
--
-- Twin: Lite/tools/attention-rate-measure.sql is the DuckDB copy of this definition. The two are hand-written; change one and
-- change the other in the same commit (nothing fails if only one moves, and the bars are set from both).
--
-- Why: the Client Timeouts alert (#5640) first shipped OFF with provisional bars of 10 (Warning) and 60 (Critical) attentions
-- per minute, a guess. #5736 used this script on a week of field capture, made each bar the higher of its floor and a
-- multiple of the server's own baseline, and turned the alert on. Run it to see a store's measured normal rate. The bar-setting
-- rule is: take each server's p99.9 of the per-minute rate, then read the fleet spread of those p99.9 values (the fleet
-- row: min, median, max) and set the defaults from that.
--
-- How to run (read-only; any role that can SELECT from the collect schema, for example the viewer or the owner login):
--
--   psql -h <host> -p <port> -U <user> -d <store database> \
--        -v from='2026-10-13 00:00:00' -v to='2026-10-20 00:00:00' -f attention-rate-measure.sql
--
-- from and to are naive UTC timestamps (the clock of collection_time), from inclusive, to exclusive. Both are required.
-- Add -A -F, (or --csv) for a file the next change can read.
--
-- What one row means: per server, a bucket per calendar minute that had a successful collector run (collect.collection_log,
-- collector_name = 'attention', status = 'SUCCESS'; the minute is date_trunc of collection_time). The bucket holds the sum of
-- the attention_events count rows in that minute (event_kind = 'count', the exact attentions since the prior run; the sample
-- rows are detail and never the total), and is zero when the run counted nothing. The attention collector runs once a minute
-- by default, so a minute holds one run. A minute with NO successful run has no bucket: the collector did not look, so it is
-- not a quiet minute, and inventing a zero for it would dilute the percentiles. A hole in collection therefore shows in the
-- three coverage columns below, not as quiet minutes. A count row in a minute with no successful run is not read (the run that
-- wrote it is not logged as a success, so the minute is not observed). The first run after a service restart stages the
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

WITH
bounds AS (
    SELECT
        CAST(:'from' AS timestamp) AS t_from,
        CAST(:'to' AS timestamp) AS t_to
),
runs AS (
    SELECT
        l.server_id,
        max(l.server_name) AS server_name,
        date_trunc('minute', l.collection_time) AS minute
    FROM collect.collection_log AS l
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
        CAST(sum(e.attention_count) AS bigint) AS attentions
    FROM collect.attention_events AS e
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
        r.minute - lag(r.minute) OVER (PARTITION BY r.server_id ORDER BY r.minute) AS since_prior_run
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
            AND  pm.minute - min(pm.minute) OVER w = interval '2 minutes'
            THEN avg(CAST(pm.attentions AS double precision)) OVER w
        END AS avg_3min,
        row_number() OVER (PARTITION BY pm.server_id ORDER BY pm.attentions DESC, pm.minute) AS busiest
    FROM per_minute AS pm
    WINDOW w AS (PARTITION BY pm.server_id ORDER BY pm.minute ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
per_server AS (
    SELECT
        r.server_id,
        max(r.server_name) AS server_name,
        CAST(extract(epoch FROM max(r.minute) - min(r.minute)) / 60 AS bigint) + 1 AS minutes_in_span,
        count(*) AS minutes_with_a_run,
        CAST(coalesce(max(extract(epoch FROM r.since_prior_run) / 60 - 1), 0) AS bigint) AS longest_gap_minutes,
        sum(r.attentions) AS attentions,
        percentile_cont(0.5) WITHIN GROUP (ORDER BY r.attentions) AS p50,
        percentile_cont(0.99) WITHIN GROUP (ORDER BY r.attentions) AS p99,
        percentile_cont(0.999) WITHIN GROUP (ORDER BY r.attentions) AS p999,
        max(r.attentions) AS max_per_minute,
        percentile_cont(0.999) WITHIN GROUP (ORDER BY r.avg_3min) AS p999_3min_avg,
        string_agg(
            to_char(r.minute, 'YYYY-MM-DD HH24:MI') || '=' || r.attentions,
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
    round(CAST(p.p50 AS numeric), 2) AS p50,
    round(CAST(p.p99 AS numeric), 2) AS p99,
    round(CAST(p.p999 AS numeric), 2) AS p999,
    p.max_per_minute,
    round(CAST(p.p999_3min_avg AS numeric), 2) AS p999_3min_avg,
    p.top_minutes,
    CAST(NULL AS numeric) AS fleet_p999_min,
    CAST(NULL AS numeric) AS fleet_p999_median,
    CAST(NULL AS numeric) AS fleet_p999_max
FROM per_server AS p
UNION ALL
SELECT
    'fleet',
    NULL,
    '(' || count(*) || ' servers)',
    CAST(sum(p.minutes_in_span) AS bigint),
    CAST(sum(p.minutes_with_a_run) AS bigint),
    max(p.longest_gap_minutes),
    sum(p.attentions),
    NULL,
    NULL,
    NULL,
    max(p.max_per_minute),
    NULL,
    NULL,
    round(CAST(min(p.p999) AS numeric), 2),
    round(CAST(percentile_cont(0.5) WITHIN GROUP (ORDER BY p.p999) AS numeric), 2),
    round(CAST(max(p.p999) AS numeric), 2)
FROM per_server AS p
HAVING count(*) > 0
ORDER BY scope DESC, server_id;
