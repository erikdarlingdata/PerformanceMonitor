-- attention-rate-measure.sql (#5736): the per-minute client attention rate, per server and for the fleet.
--
-- Why: the Client Timeouts alert (#5640) ships OFF with provisional bars of 10 (Warning) and 60 (Critical) attentions
-- per minute, a guess. After a week of capture on a field store, run this to see the measured normal rate. The bar-setting
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
-- What one row means: per server, a bucket per calendar minute (date_trunc of collection_time) holding the sum of the
-- attention_events count rows (event_kind = 'count', the exact attentions since the prior run; the sample rows are detail and
-- never the total). The attention collector runs once a minute by default, so a minute holds one run. The server's observed
-- span is the first to the last minute that has a successful collector run in collect.collection_log (collector_name =
-- 'attention', status = 'SUCCESS') or a count row. Every minute inside that span is a bucket, zero where nothing was counted: a
-- p99.9 over the non-empty minutes alone would overstate the rate, because quiet minutes write no row. Minutes outside the
-- span are not observed and are not counted. The first run after a service restart stages the counter and writes no count row,
-- so that span reads as quiet, the same known edge the alert has.
--
-- Columns:
--   scope                 server, or fleet for the last row
--   server_id, server_name
--   minutes_observed      buckets in the span (zero-filled minutes included); on the fleet row the sum over servers
--   attentions            the sum of the count rows; on the fleet row the sum over servers
--   p50, p99, p999        percentiles (continuous) of attentions per minute over the buckets
--   max_per_minute        the busiest minute
--   p999_3min_avg         the p99.9 of the 3-minute rolling average of the per-minute rate, the quantity the alert compares to
--                         its bars (it averages over about three minutes); NULL when the span is under 3 minutes
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
seen AS (
    SELECT server_id, server_name, minute FROM runs
    UNION ALL
    SELECT server_id, server_name, minute FROM counts
),
span AS (
    SELECT
        server_id,
        max(server_name) AS server_name,
        min(minute) AS first_minute,
        max(minute) AS last_minute
    FROM seen
    GROUP BY server_id
),
per_minute AS (
    SELECT
        s.server_id,
        s.server_name,
        g.minute,
        coalesce(c.attentions, 0) AS attentions
    FROM span AS s
    CROSS JOIN LATERAL generate_series(s.first_minute, s.last_minute, interval '1 minute') AS g(minute)
    LEFT JOIN counts AS c
        ON  c.server_id = s.server_id
        AND c.minute = g.minute
),
rolled AS (
    SELECT
        pm.*,
        CASE
            WHEN count(*) OVER w = 3
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
        count(*) AS minutes_observed,
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
    p.minutes_observed,
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
    sum(p.minutes_observed),
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
ORDER BY 1 DESC, 2;
