/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The per-server 7-day collection-health statements (#5810), moved out of the service's reader (#6384) so the viewer's
/// Sweep Pressure and Heaviest Collectors panels run the SAME statement the web's <c>get_collection_health</c> does, not
/// a second copy. The service's <c>DarlingDataReader.CollectionHealthComposedSql</c> and the viewer's sweep read both
/// point here.
/// </summary>
public static class CollectionHealthStatements
{
    /// <summary><c>CollectionHealthSql</c>'s final SELECT: the columns, in their positional order, over the CTEs
    /// <c>health</c> and <c>p95</c>, and the keyed LATERAL lookups on the raw log (#4955). Its own constant so
    /// <c>CollectionHealthComposedSql</c> runs the same text over its own <c>health</c> and <c>p95</c> (#5810).</summary>
    public const string FinalSelectSql = $"""
        SELECT
            h.collector_name,
            h.total_runs,
            h.success_count,
            h.error_count,
            h.avg_duration_ms,
            h.max_duration_ms,
            p95.p95_duration_ms,
            h.last_success_time,
            h.last_run_time,
            -- #1855: the message from the NEWEST failing run, not MAX()'s lexicographically greatest
            -- one. The lookup is filtered to the failing class rather than to the instant alone, which is
            -- load-bearing rather than belt-and-braces: a run of ANY other class at the same instant (a
            -- SUCCESS row's note) must not surface here as a fake last error, and when no failing run in
            -- the window carried text the lookup finds nothing and this is NULL. error_message DESC only
            -- breaks an exact-timestamp tie, the way the rank it replaces broke it, which a binary-vs-
            -- locale collation would not.
            -- #3240: EXTENSION_MISSING is in the exemplar set because its stored sentence IS the remedy
            -- (it names the extension and the database to create it in).
            failed.error_message AS last_error,
            h.last_error_time,
            h.permission_denied_count,
            h.yield_count,
            -- #1837: the note a SUCCEEDING run can leave behind (an enumeration that yielded 0 items,
            -- items whose enumeration probe failed) and how many runs carried one. Gated on SUCCESS
            -- specifically — the runners attach a note only to the SUCCESS write. Informational: it
            -- feeds no band, and a legitimately empty target stays HEALTHY. The NEWEST run that carried
            -- one (#1855), so a later clean run no longer blanks a note the window still holds; text does
            -- not sort like the number #1837's probe note carries (12 item(s) sorts below 9 item(s)).
            noted.error_message AS last_note,
            h.note_count,
            h.has_user_databases,
            -- #2472: the per-database fan-out, described for ONE run — the dearest one in the window.
            -- Five collectors run once per database and the run writes a single blended duration_ms, so
            -- "eight databases at 10.1s" and "one at 62s beside seven at 2.7s" are the same 80,900 ms and
            -- want opposite fixes. These four are that one run's parts, and their ratio
            -- (slowest_item_ms * fanout_items / slowest_run_duration_ms) is 1.0 for an even fan-out and
            -- 6.1 for the dominated example. All four come from the SAME row (the dearest lookup below)
            -- rather than four independent aggregates, because a slowest item taken from one run and a
            -- width taken from another compose into a ratio describing no run that ever happened — the
            -- same defect PERCENTILE_CONT was rejected for above. Ranked on slowest_item_ms rather than
            -- duration_ms because the question is which database is expensive, not which cycle was: a run
            -- whose total is the largest only because every database was busy is exactly the shape a
            -- per-database override should NOT be aimed at.
            --
            -- A collector that never fans out has no dearest item, and the lookup then reads the NEWEST
            -- run, which is where the rank this replaces fell through: fanout_items and slowest_item are
            -- that run's own (NULL for a collector that never fans out, which is most of them),
            -- slowest_item_ms is NULL, and slowest_run_duration_ms is that run's duration.
            dearest.fanout_item_count AS fanout_items,
            dearest.slowest_item AS slowest_item,
            dearest.slowest_item_ms AS slowest_item_ms,
            dearest.duration_ms AS slowest_run_duration_ms,
            h.abandoned_count,
            h.last_denied_time,
            h.rows_stored,
            h.runs_with_rows,
            h.extension_missing_count,
            h.session_missing_count,
            -- #3819: current_status is what the collector is reporting NOW, for the finding's prose. Taken
            -- from the NEWEST run rather than as a MAX over the skip rows: MAX is lexicographic, so on a
            -- streak whose status changed it would name whichever word sorts highest instead of the one
            -- being reported. status DESC only breaks an exact-timestamp tie, and breaks it identically
            -- on DuckDB and Postgres -- the same reason the lookups above tie-break on error_message.
            newest.status AS current_status,
            h.last_non_skip_time,
            h.last_productive_time,
            -- #3885: how many runs, counting back from the NEWEST, were SUCCESS with zero rows and nothing
            -- else. The column that makes the #3819 regression arm able to see the class it could not:
            -- job_history recorded SUCCESS / 0 rows run after run on 41 of 43 servers of the largest
            -- production store for up to a fortnight -- 1,900+ consecutive such runs on one of them -- and
            -- banded HEALTHY throughout, because #3819 keys on a skip STATUS and this collector's status was
            -- the most reassuring word the vocabulary has.
            --
            -- Exact. The newest run that BREAKS the streak is found by the aggregate above
            -- (last_streak_break_time); the lookup ranks only the runs from that instant on, newest first,
            -- and takes the rank of the first breaking run, minus one, which is the count of runs ahead of
            -- it. NULL (no run breaks it -- every run in the window is a zero-row success) falls back to
            -- the window's run count, which is that same count. A streak broken by an error, a denial, a
            -- skip or a productive run therefore reads 0 rather than reaching past it, because what this
            -- measures is the collector's CURRENT state and any of those is a different current state.
            --
            -- The abandonment exclusion is the one success_count above uses, for the same #2926 reason: a
            -- pre-#2803 abandoned cycle is stored as SUCCESS with zero rows and the budget note, which is
            -- data LOSS rather than a source that went quiet, and counting it here would attribute an
            -- abandonment to a regression. APPENDED, like every column since #2472, because this result set
            -- is read positionally.
            COALESCE(streak.runs_ahead_of_break, h.total_runs) AS trailing_zero_row_success_runs,
            -- #4748: the note the collector's NEWEST run left, which is not last_note above. last_note is the
            -- newest run that CARRIED a note, so a clean run after a partial-failure cycle still
            -- shows the older cycle's note there; the band must not read that, because the loss it names
            -- is not the collector's current state. The newest run of any status, and the SUCCESS
            -- gate matches last_note's (only the SUCCESS write carries a note). APPENDED like every
            -- column since #2472, because this result set is read positionally and Lite's DuckDB read
            -- mirrors the ordinals.
            CASE WHEN newest.status = 'SUCCESS' THEN newest.error_message END AS latest_run_note,
            -- #5646: how many of the NEWEST runs in a row succeeded, counted to 3 and no further (the recovery
            -- rule's input). Newest first on the FULL timestamp (collection_time keeps microseconds), then
            -- log_id DESC, the unique id, so two runs at the same instant are ordered the same way every time.
            -- The success test is success_count's own (SUCCESS, not an abandoned cycle) over the same window
            -- rows total_runs counts. APPENDED, read positionally; Lite's DuckDB read carries it at the same
            -- ordinal.
            COALESCE(recent.trailing_success_runs, 0) AS trailing_success_runs
        FROM health h
        LEFT JOIN p95
          ON p95.collector_name = h.collector_name
        -- #4955: the keyed lookups. Each reads ONE collector's rows through
        -- idx_collection_log_watermark (server_id, collector_name, collection_time DESC), at an instant the
        -- aggregate above already found, instead of ranking every run of the window to reach them. The
        -- window bound ($2) is repeated on each so a chunk older than the window is excluded at plan time.
        --
        -- The newest run: the rows at the window's newest instant, the greater status first (the order
        -- the rank this replaces broke an exact tie in; PostgreSQL's DESC puts a NULL status first).
        LEFT JOIN LATERAL
        (
            SELECT n.status,
                   n.error_message
            FROM v_collection_log n
            WHERE n.server_id = $1
            AND   n.collector_name = h.collector_name
            AND   n.collection_time >= $2
            AND   n.collection_time = h.last_run_time
            ORDER BY n.status DESC
            LIMIT 1
        ) newest ON TRUE
        -- The newest failing run that carried text. No such run: no row, and last_error is NULL.
        LEFT JOIN LATERAL
        (
            SELECT f.error_message
            FROM v_collection_log f
            WHERE f.server_id = $1
            AND   f.collector_name = h.collector_name
            AND   f.collection_time >= $2
            AND   f.collection_time = h.last_failure_text_time
            AND   f.status IN ('ERROR', 'PERMISSIONS', 'EXTENSION_MISSING')
            AND   f.error_message IS NOT NULL
            ORDER BY f.error_message DESC
            LIMIT 1
        ) failed ON TRUE
        -- The newest SUCCESS run that carried a note.
        LEFT JOIN LATERAL
        (
            SELECT t.error_message
            FROM v_collection_log t
            WHERE t.server_id = $1
            AND   t.collector_name = h.collector_name
            AND   t.collection_time >= $2
            AND   t.collection_time = h.last_note_time
            AND   t.status = 'SUCCESS'
            AND   t.error_message IS NOT NULL
            ORDER BY t.error_message DESC
            LIMIT 1
        ) noted ON TRUE
        -- The run that carried the window's dearest item, the newest of any tied on it. With no dearest
        -- item the filter keeps every run, and the newest one is the row.
        LEFT JOIN LATERAL
        (
            SELECT d.fanout_item_count,
                   d.slowest_item,
                   d.slowest_item_ms,
                   d.duration_ms
            FROM v_collection_log d
            WHERE d.server_id = $1
            AND   d.collector_name = h.collector_name
            AND   d.collection_time >= $2
            AND   (h.dearest_item_ms IS NULL OR d.slowest_item_ms = h.dearest_item_ms)
            ORDER BY d.collection_time DESC
            LIMIT 1
        ) dearest ON TRUE
        -- The runs from the newest streak break on, ranked newest first (the greater status first at an
        -- exact tie). Never the window: with no break the aggregate's instant is NULL and nothing is read.
        LEFT JOIN LATERAL
        (
            SELECT MIN(CASE WHEN NOT (status = 'SUCCESS'
                                      AND COALESCE(rows_collected, 0) = 0
                                      AND NOT {EnumeratedCollectorDriver.AbandonedByNotePredicateSql})
                            THEN run_rank END) - 1 AS runs_ahead_of_break
            FROM
            (
                SELECT s.status,
                       s.rows_collected,
                       s.error_message,
                       ROW_NUMBER() OVER
                       (
                           ORDER BY s.collection_time DESC,
                                    s.status DESC
                       ) AS run_rank
                FROM v_collection_log s
                WHERE s.server_id = $1
                AND   s.collector_name = h.collector_name
                AND   s.collection_time >= $2
                AND   s.collection_time >= h.last_streak_break_time
            ) ranked
        ) streak ON TRUE
        -- #5646: the newest three runs, newest first, then the highest log_id. A plain index walk of
        -- idx_collection_log_watermark that stops after three rows.
        LEFT JOIN LATERAL
        (
            SELECT {CollectorTrailingRuns.StreakFromFlagsSql} AS trailing_success_runs
            FROM
            (
                -- An ordered aggregate over the three rows, not a window function: the keyed lookups
                -- above rank nothing over the window.
                SELECT array_agg(x.ok ORDER BY x.collection_time DESC, x.log_id DESC) AS flags
                FROM
                (
                    SELECT {CollectorTrailingRuns.RunOkSql} AS ok,
                           v.collection_time,
                           v.log_id
                    FROM v_collection_log v
                    WHERE v.server_id = $1
                    AND   v.collector_name = h.collector_name
                    AND   v.collection_time >= $2
                    ORDER BY v.collection_time DESC, v.log_id DESC
                    LIMIT 3
                ) x
            ) r
        ) recent ON TRUE
        ORDER BY h.collector_name
        """;

    /// <summary>
    /// The 7-day health read served from the hourly rollup (V186, #5810): the production statement behind
    /// <c>GetCollectionHealthAsync</c>. Same parameters, columns, order and types as <c>CollectionHealthSql</c>;
    /// every column but <c>p95_duration_ms</c> has the same value.
    ///
    /// <para><b>Where each hour comes from.</b> A closed hour the rollup holds for this server, from the window's first
    /// whole hour on, is read from <c>collect.collection_log_hourly</c> (about 168 rows per collector for the week). Every
    /// other part of the window is read from the raw log: the partial first hour, any closed hour the table does not hold
    /// (the first passes after the upgrade, which backfill a day at a time, or passes that failed), and everything after
    /// the newest rolled hour, the open hour included. Those slices are found as ranges of consecutive hours, so the raw
    /// read is a few range scans, not a filter over the week, and an empty table reads the whole window raw. The raw
    /// slices are aggregated with <c>CollectionLogHourly.AggregatesSql</c>, the expressions the writer uses, and the
    /// parts are merged per collector: counts and sums add, maxima and instants take the MAX, and the average is
    /// <c>sum::numeric / count</c>, which is what <c>AVG(integer)</c> computes. The keyed LATERAL lookups (#4955) and
    /// the final column list are <c>FinalSelectSql</c>, the text <c>CollectionHealthSql</c> ends with, so they stay on the raw log.</para>
    ///
    /// <para><b>p95 is bucketed.</b> An exact percentile cannot be rebuilt from hourly rows, so the read sums the
    /// duration histograms and reports the upper bound of the slot that holds the run at position <c>ceil(0.95 n)</c>
    /// (<c>CollectionLogHourly.DurationBucketBounds</c>, within 47% of the exact value), capped at the window's
    /// maximum duration so p95 never exceeds the max. The slot above an hour has no upper bound and reads the maximum.
    /// NULL for a collector with no timed run, as before.</para>
    /// </summary>
    public static readonly string ComposedSql = ComposeCollectionHealthSql();

    private static string ComposeCollectionHealthSql()
    {
        var values = CollectionLogHourly.ValueColumns;
        return $"""
        WITH rolled AS MATERIALIZED
        (
            SELECT
                hr.collector_name,
                hr.hour_start,
                {values}
            FROM collect.collection_log_hourly hr
            WHERE hr.server_id = $1
            AND   hr.hour_start >= $2
        ),
        rolled_hours AS
        (
            SELECT DISTINCT rh.hour_start
            FROM rolled rh
        ),
        raw_ranges AS
        (
            -- The hours the table does not hold, as islands of consecutive hours: the partial first hour, and any
            -- missing closed hour up to the newest rolled one.
            SELECT GREATEST(MIN(gap.hour_start), $2) AS range_start,
                   MAX(gap.hour_start) + interval '1 hour' AS range_end
            FROM
            (
                SELECT g.hour_start,
                       g.hour_start - ROW_NUMBER() OVER (ORDER BY g.hour_start) * interval '1 hour' AS island
                FROM generate_series(date_trunc('hour', $2),
                                     (SELECT MAX(m.hour_start) FROM rolled_hours m),
                                     interval '1 hour') AS g(hour_start)
                WHERE NOT EXISTS (SELECT 1 FROM rolled_hours r WHERE r.hour_start = g.hour_start)
            ) gap
            GROUP BY gap.island
            UNION ALL
            -- Everything after the newest rolled hour, the open hour included; the whole window when nothing is rolled.
            SELECT COALESCE(MAX(t.hour_start) + interval '1 hour', $2),
                   'infinity'::timestamp
            FROM rolled_hours t
        ),
        raw (collector_name, {values}) AS
        (
            SELECT
                w.collector_name,
                {CollectionLogHourly.AggregatesSql}
            FROM
            (
                SELECT
                    l.collector_name,
                    l.collection_time,
                    l.status,
                    l.error_message,
                    l.rows_collected,
                    l.duration_ms,
                    l.slowest_item_ms,
                    width_bucket(l.duration_ms - 1, {CollectionLogHourly.BoundsArraySql}) AS slot
                FROM raw_ranges rr
                -- #5506: one bounded probe per raw range. A plain join read every row of the server for the whole
                -- window (about 223k rows, keeping 2,670 through a join filter), mostly from the two uncompressed
                -- chunks that hold every server's rows on shared pages: 10.5 s and 185 MB read cold on the large
                -- production store. This form took 0.73 s and 27 MB cold with identical output. OFFSET 0 must stay:
                -- without it the planner flattens the LATERAL back into the plain join (measured 7.7 s), the same
                -- fence CollectionLogHourly.UpsertSql uses. The select list must hold every column read above.
                CROSS JOIN LATERAL
                (
                    SELECT x.collector_name, x.collection_time, x.status, x.error_message, x.rows_collected, x.duration_ms, x.slowest_item_ms
                    FROM v_collection_log x
                    WHERE x.server_id = $1
                    AND   x.collection_time >= $2
                    AND   x.collection_time >= rr.range_start
                    AND   x.collection_time < rr.range_end
                    AND   x.{CollectionCycleRow.NotCycleRowSql}
                    OFFSET 0
                ) l
            ) w
            GROUP BY w.collector_name
        ),
        parts AS
        (
            SELECT collector_name, {values} FROM rolled
            UNION ALL
            SELECT collector_name, {values} FROM raw
        ),
        slots AS
        (
            SELECT p.collector_name,
                   s.slot,
                   SUM(s.runs) AS runs
            FROM parts p
            CROSS JOIN LATERAL unnest(p.duration_histogram) WITH ORDINALITY AS s(runs, slot)
            GROUP BY p.collector_name, s.slot
        ),
        p95_slot AS
        (
            -- The slot (1-based, so slot s's upper bound is bounds[s]) holding the run at position ceil(0.95 n), in the
            -- same integer test as CollectionHealthSql's exact p95.
            SELECT collector_name,
                   MIN(slot) FILTER (WHERE timed_runs > 0 AND running_runs * 20 >= timed_runs * 19) AS slot
            FROM
            (
                SELECT collector_name,
                       slot,
                       SUM(runs) OVER (PARTITION BY collector_name ORDER BY slot) AS running_runs,
                       SUM(runs) OVER (PARTITION BY collector_name) AS timed_runs
                FROM slots
            ) running
            GROUP BY collector_name
        ),
        health AS
        (
            SELECT
                collector_name,
                SUM(total_runs)::bigint AS total_runs,
                SUM(success_count)::bigint AS success_count,
                SUM(error_count)::bigint AS error_count,
                SUM(duration_sum)::numeric / NULLIF(SUM(duration_count), 0)::numeric AS avg_duration_ms,
                MAX(max_duration_ms) AS max_duration_ms,
                MAX(last_success_time) AS last_success_time,
                MAX(last_run_time) AS last_run_time,
                MAX(last_error_time) AS last_error_time,
                MAX(last_failure_text_time) AS last_failure_text_time,
                SUM(permission_denied_count)::bigint AS permission_denied_count,
                SUM(yield_count)::bigint AS yield_count,
                MAX(last_note_time) AS last_note_time,
                SUM(note_count)::bigint AS note_count,
                CASE
                    WHEN EXISTS
                         (
                             SELECT 1
                             FROM v_database_size_stats
                             WHERE server_id = $1
                             AND   collection_time >= $2
                             AND   (database_id > 4 OR database_id IS NULL)
                         )
                    THEN 1
                    ELSE 0
                END AS has_user_databases,
                MAX(max_slowest_item_ms) AS dearest_item_ms,
                SUM(abandoned_count)::bigint AS abandoned_count,
                MAX(last_denied_time) AS last_denied_time,
                SUM(rows_stored)::bigint AS rows_stored,
                SUM(runs_with_rows)::bigint AS runs_with_rows,
                SUM(extension_missing_count)::bigint AS extension_missing_count,
                SUM(session_missing_count)::bigint AS session_missing_count,
                MAX(last_non_skip_time) AS last_non_skip_time,
                MAX(last_productive_time) AS last_productive_time,
                MAX(last_streak_break_time) AS last_streak_break_time
            FROM parts
            GROUP BY collector_name
        ),
        p95 AS
        (
            SELECT ps.collector_name,
                   CASE WHEN ps.slot IS NOT NULL
                        THEN LEAST(({CollectionLogHourly.BoundsArraySql})[ps.slot], hb.max_duration_ms)
                   END AS p95_duration_ms
            FROM p95_slot ps
            JOIN health hb
              ON hb.collector_name = ps.collector_name
        )
        """ + "\n" + FinalSelectSql;
    }
}
