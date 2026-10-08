/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Where an unkeyed duration trend (query-stats, procedure-stats) reads from, and what it says about what it
/// served — the tier decision, the hourly-tier SQL, the raw-tier SQL (<see cref="BuildBucketedRawTrendSql"/>, #3653
/// A11), and the coverage description that the MCP reader
/// (<c>DarlingTrendReader</c>, #3590) and the desktop viewer's Performance Trends tab
/// (<c>ViewerDataService.QueryTrends</c>, #3653) share, so the two apps cannot disagree about which relation
/// answers a window or about what a served series covers.
///
/// <para><b>Why this lives in Storage.</b> The viewer does not reference the service assembly (the #1661 /
/// #2530 arrangement: reads both apps run live here, beside <see cref="QueryStoreTrendRouting"/>, which is
/// this file's model), so a decision that has to be the SAME in both cannot live in the MCP reader alone.
/// Before #3653 the viewer's chart read the raw tier only — the exact shape #3590 removed from the MCP trio —
/// so a seven-day chart on a TimescaleDB store plotted the four days raw still held, under an axis that said
/// seven, with nothing on the chart marking the difference. The MCP reader keeps its own named constants for
/// its tests and its payload; DarlingMcpTrendToolsTests pins them equal to what this file produces, so a
/// change to either side that is not made to both fails CI rather than drifting.</para>
///
/// <para><b>Not <see cref="RetentionTierRouter"/>, and the difference is deliberate.</b> The router is the
/// built-in tabs' three-tier ladder with a one-day margin under every horizon; this is get_query_trend's
/// (#2353) two-tier ladder with a one-HOUR margin under raw, because the trends' widest accepted window
/// (seven days) sits far inside the hourly horizon so the daily tier is never reached, and because widening
/// the raw margin to a day would push every three-to-four-day window onto the hourly tier for nothing (the
/// #2353 trade of resolution for purge-independence is one hour, stated there). Both ladders are built from
/// the same <see cref="TierCoverage"/> primitives and degrade the same way.</para>
/// </summary>
public static class DurationTrendRouting
{
    /// <summary>
    /// The seconds in one hourly-rollup bucket, as the SQL literal the hourly-tier duration trends divide by.
    /// A string because a constant interpolated string admits only string constants; pinned equal to
    /// <see cref="TimescaleSupport.HourlyBucket"/> by DarlingMcpTrendToolsTests so the two cannot drift.
    /// </summary>
    public const string HourlyBucketSecondsSql = "3600.0";

    /// <summary>
    /// Extra room inside the raw horizon before a window is handed to the aggregate (#2353). The purge is
    /// periodic, so a window whose oldest point sits exactly on the four-day line may or may not still find its
    /// rows depending on when the purge last ran; preferring the aggregate there trades resolution for an answer
    /// that does not change with the purge schedule. Exposed so a test pins the boundary rather than restating it.
    /// </summary>
    public static readonly TimeSpan RawTierMargin = TimeSpan.FromHours(1);

    /// <summary>
    /// How far past the requested start the first served point may sit before the answer calls itself
    /// <c>window_truncated</c> (the wire key since #3653 item 17). Ninety minutes: an hourly bucket can begin up to an hour after a window start that
    /// falls mid-hour (<c>bucket &gt;= $start</c> excludes the bucket the start falls inside), and a raw series
    /// legitimately opens a collection cadence or two late; anything past that means the tier did not hold the
    /// window's head. Extracted from #2353's inline literal so every tiered read shares one boundary.
    /// </summary>
    public static readonly TimeSpan TruncationSlack = TimeSpan.FromMinutes(90);

    /// <summary>
    /// Whether the raw per-collection table can serve a window, decided by the age of its OLDEST point measured
    /// from WALL CLOCK (#2353).
    ///
    /// <para><b>The second parameter is <c>now</c>, not the window's end, and the difference is not cosmetic.</b>
    /// Retention drops chunks by actual elapsed time, so what decides whether raw still holds a row is how long
    /// ago that row happened — never where it sits inside the requested window. Measuring the start against the
    /// END would call a two-hour window from ten days ago "recent", because it is recent relative to its own
    /// end, and route it to a tier that dropped those rows six days earlier.</para>
    ///
    /// <para>Pure so the boundary is unit-testable without a store. Width is deliberately not consulted: a
    /// narrow window sitting entirely in last week is exactly as unservable from raw as a wide one.</para>
    /// </summary>
    public static bool ShouldUseRawTier(DateTime startUtc, DateTime nowUtc) =>
        startUtc >= nowUtc - TimescaleSupport.RawRetentionSpan + RawTierMargin;

    /// <summary>
    /// The one tier decision every unkeyed duration trend makes (#3541 A2): get_query_trend's age rule
    /// (<see cref="ShouldUseRawTier"/>), degraded to what the store HAS and to what it has MATERIALIZED, in that
    /// order.
    ///
    /// <para><b>Availability (#1664).</b> A plain-PostgreSQL store has no continuous aggregates, and a relation
    /// named in a statement is resolved at PARSE time — so an age-only rule sent a 168-hour window on such a
    /// store to a view that does not exist and the read failed with 42P01. Raw is the right answer there anyway:
    /// without the extension nothing ever drops raw, so it holds the complete window.</para>
    ///
    /// <para><b>Coverage (#1759).</b> A rollup created <c>WITH NO DATA</c> serves only what a refresh or a
    /// backfill has materialized, so on a store that pre-existed its rollups and was never backfilled the hourly
    /// tier is SHALLOWER than raw, whose purge the arming gate holds paused until the rollup covers it. The rule
    /// is comparative and only ever moves DOWN on a positive measurement: raw wins only when it is measured to
    /// reach further back than the rollup's floor (<see cref="TierCoverage.ReachesFurtherBack"/>); unmeasured
    /// coverage (nulls) is inert and the age + availability answer stands. Hourly is never abandoned merely
    /// because the window starts below its floor — on a healthy store raw keeps four days against the rollup's
    /// ninety, and dropping to raw there would return LESS. The part of the window the rollup has not reached is
    /// the caller's job to disclose (<see cref="DescribeCoverage"/>), not routing's job to hide.</para>
    /// </summary>
    public static RetentionTier ResolveTier(DateTime startUtc, DateTime nowUtc, bool hourlyAvailable, TierCoverage coverage)
    {
        if (ShouldUseRawTier(startUtc, nowUtc) || !hourlyAvailable)
        {
            return RetentionTier.Raw;
        }

        if (TierCoverage.Covers(coverage.HourlyFloorUtc, startUtc))
        {
            return RetentionTier.Hourly;
        }

        return TierCoverage.ReachesFurtherBack(coverage.RawOldestUtc, coverage.HourlyFloorUtc)
            ? RetentionTier.Raw
            : RetentionTier.Hourly;
    }

    /// <summary>
    /// What a series actually covers, which is what the caller gets told (#2353): the first served point, or
    /// the requested start when nothing came back — an empty result says nothing about coverage, so the
    /// requested start stands rather than being narrowed to a window nobody can describe. <c>Truncated</c> is
    /// true only on a NON-empty series whose head sits more than <see cref="TruncationSlack"/> after the
    /// requested start; an empty series reports its coverage through the caller's empty branch instead.
    /// </summary>
    public static (DateTime EffectiveStartUtc, bool Truncated) DescribeCoverage(DateTime? firstPointUtc, DateTime startUtc)
        => firstPointUtc is DateTime first
            ? (first, first > startUtc + TruncationSlack)
            : (startUtc, false);

    /// <summary>
    /// The one word both apps use for the tier a trend was served from — get_query_trend's payload vocabulary
    /// (<c>source</c>), reused by the viewer's chart subtitle so a user reading the chart and an agent reading
    /// the tool describe the same series with the same word. The daily tier is not on this ladder (see the
    /// type remarks), so it has no word here.
    /// </summary>
    public static string SourceWord(RetentionTier tier) => tier == RetentionTier.Raw ? "raw" : "hourly";

    /// <summary>
    /// The hourly-tier duration trend over <paramref name="hourlyView"/> (#3541 A2): the same two per-second
    /// rates the raw reads compute, from the continuous aggregate, for windows whose oldest point the raw tier
    /// no longer holds. The rollup carries <c>elapsed_time_sum</c> and <c>execution_count_sum</c> per
    /// (server, database, identity, hour); summing those across the identity columns per bucket is exactly what
    /// the raw read's <c>GROUP BY collection_time</c> does one grain finer, so the two tiers answer the same
    /// question at different resolution. <c>bucket</c> is projected as <c>collection_time</c> so the point shape
    /// does not change underneath a caller that got a raw answer last time.
    ///
    /// <para><b>The denominator is the bucket width, not a LAG.</b> The raw read has to recompute its interval
    /// from the gap to the previous collection because a per-sweep row carries no shared interval; an hour
    /// bucket's width is KNOWN, so this read divides by <see cref="HourlyBucketSecondsSql"/> and every bucket has
    /// a real denominator — the LAG idiom's first collection, whose interval is NULL and whose rate is therefore
    /// unknowable (published as NULL by the raw reads since #3541 A12; as a fabricated 0 before it), does not
    /// exist here. The trade is stated rather than hidden: an hour the collector covered only partly (a service
    /// restart mid-hour, a gap past the delta policy) reads LOW, never high, because its summed work is still
    /// spread over the full 3,600 seconds.</para>
    ///
    /// <para><b>Materialized-only</b>, like every rollup here (#1759): the current hour is never materialized
    /// and the previous one lands on the next refresh, so this series trails the clock by up to two hours.</para>
    ///
    /// <para>With <paramref name="withDatabaseFilter"/>, $4 is the viewer's guarded <c>text[]</c> database
    /// filter (#1319) — both hourly rollups group by <c>database_name</c>, so the filter survives the routing.
    /// Without it the text is the MCP reader's constant byte for byte, which is what its pin asserts.
    /// $1 server_id, $2/$3 window (naive UTC; $3 is EXCLUSIVE — a bucket is stamped at its START, so the hour
    /// that begins at $3 lies after the window and is not read).</para>
    /// </summary>
    public static string BuildHourlyTrendSql(string hourlyView, bool withDatabaseFilter, bool coverIdleHours = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hourlyView);

        if (coverIdleHours)
        {
            return BuildHourlyTrendWithIdleHoursSql(hourlyView, withDatabaseFilter);
        }

        if (withDatabaseFilter)
        {
            /* #5414 round 2: the same rule as the bucketed read. The filter sits inside the sums, so an hour the chosen
               databases had no rollup row in is a measured 0 (the store held the hour), not a missing point, and the
               chart's data-start banner is not pushed to the databases' first hour. Whether they had any row in the
               window is one window-level test, which keeps the answer empty when they had none. */
            const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
            return $"""
                WITH hourly AS
                (
                    SELECT
                        bucket,
                        COALESCE(SUM(elapsed_time_sum) FILTER (WHERE {match}), 0) AS elapsed_time_sum,
                        COALESCE(SUM(execution_count_sum) FILTER (WHERE {match}), 0) AS execution_count_sum,
                        COUNT(*) FILTER (WHERE {match}) AS matched_rows
                    FROM {hourlyView}
                    WHERE server_id = $1
                    AND   bucket >= $2
                    AND   bucket < $3
                    GROUP BY bucket
                )
                SELECT
                    bucket AS collection_time,
                    elapsed_time_sum / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
                    CAST(execution_count_sum AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
                FROM hourly
                WHERE EXISTS (SELECT 1 FROM hourly WHERE matched_rows > 0)
                ORDER BY bucket
                """;
        }

        return $"""
            SELECT
                bucket AS collection_time,
                SUM(elapsed_time_sum) / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
                CAST(SUM(execution_count_sum) AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
            FROM {hourlyView}
            WHERE server_id = $1
            AND   bucket >= $2
            AND   bucket < $3
            GROUP BY bucket
            ORDER BY bucket
            """;
    }

    /// <summary>
    /// #5449: the procedure rollups hold no row for an hour in which no procedure did work (the collector stores no row for a
    /// procedure that did none, where an older store kept zero rows), so a fully idle hour is a gap in the rollup where an older
    /// store has a measured 0. <see cref="BuildHourlyTrendSql"/>'s read with the idle hours (<see cref="ProcedureIdleHoursSql"/>)
    /// added to the rollup's hours as zero work over the same <see cref="HourlyBucketSecondsSql"/>. Read time only, no change to
    /// the rollup. $1..$4 as <see cref="BuildHourlyTrendSql"/>.
    /// </summary>
    private static string BuildHourlyTrendWithIdleHoursSql(string hourlyView, bool withDatabaseFilter)
    {
        const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
        var sums = withDatabaseFilter
            ? $"COALESCE(SUM(elapsed_time_sum) FILTER (WHERE {match}), 0) AS elapsed_time_sum,\n"
              + $"                COALESCE(SUM(execution_count_sum) FILTER (WHERE {match}), 0) AS execution_count_sum,\n"
              + $"                COUNT(*) FILTER (WHERE {match}) AS matched_rows"
            : "SUM(elapsed_time_sum) AS elapsed_time_sum,\n"
              + "                SUM(execution_count_sum) AS execution_count_sum";
        var windowHasRows = withDatabaseFilter
            ? "\n            WHERE EXISTS (SELECT 1 FROM hourly WHERE matched_rows > 0)"
            : "";

        return $"""
            WITH hourly AS
            (
                SELECT
                    bucket,
                    {sums}
                FROM {hourlyView}
                WHERE server_id = $1
                AND   bucket >= $2
                AND   bucket < $3
                GROUP BY bucket
            ),
            {ProcedureIdleHoursSql()},
            filled AS
            (
                SELECT bucket, elapsed_time_sum, execution_count_sum
                FROM hourly
                UNION ALL
                SELECT idle_hours.bucket, 0, 0
                FROM idle_hours
                WHERE NOT EXISTS (SELECT 1 FROM hourly WHERE hourly.bucket = idle_hours.bucket)
            )
            SELECT
                bucket AS collection_time,
                elapsed_time_sum / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
                CAST(execution_count_sum AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
            FROM filled{windowHasRows}
            ORDER BY bucket
            """;
    }

    /// <summary>
    /// #5449: the CTEs (<c>run_hours</c> and <c>idle_hours</c>, the hours the rollup has no row for) that add the idle hours to the
    /// hourly read. An hour is in the series when the rollup has a row, OR a SUCCESS run of the collector falls in it AND the raw
    /// <c>procedure_stats</c> table holds no row in that hour: a collector that ran and stored nothing measured zero work over the
    /// 3,600 seconds. The runs come from <c>collect.collection_log</c> (a run is logged after it finishes, so its point is the log
    /// time less its <c>duration_ms</c>, the same instant the raw tier plots an idle run at).
    ///
    /// <para>The raw-row test is what keeps the one or two trailing hours the continuous aggregate has not materialized yet a gap on
    /// a busy server instead of a false 0: those hours have raw rows. An outage hour (no rollup row, no run) stays a gap, so a
    /// bucket's seconds do not count time the collector was down. Hours older than the log (it keeps 60 days) have no runs, and a
    /// store that never logged runs has none at all: both read as the rollup alone, which is the rule before this fill existed. An
    /// old materialization hole past raw retention, with runs, reads 0. Uses the log's watermark index; the raw probe per hour
    /// is a no-op once the raw rows are past retention. Only idle runs (<c>rows_collected = 0</c>) make an hour: a busy run that starts at
    /// H:59:58 plots in the next hour, and counting it there would fill an outage hour with a 0 instead of a gap. $1 server_id, $2/$3 window (naive UTC, $3 exclusive, as the rollup read).</para>
    /// </summary>
    private static string ProcedureIdleHoursSql()
        => $"""
            run_hours AS
            (
                SELECT DISTINCT date_trunc('hour', collection_time - COALESCE(duration_ms, 0) * INTERVAL '1 millisecond') AS bucket
                FROM {PgSchemaGenerator.CollectSchema}.collection_log
                WHERE server_id = $1
                AND   collector_name = 'procedure_stats'
                AND   status = 'SUCCESS'
                AND   rows_collected = 0
                AND   collection_time >= $2
                AND   collection_time < $3 + INTERVAL '1 hour'
            ),
            idle_hours AS
            (
                SELECT run_hours.bucket
                FROM run_hours
                WHERE run_hours.bucket >= $2
                AND   run_hours.bucket < $3
                AND   NOT EXISTS
                (
                    SELECT 1
                    FROM procedure_stats
                    WHERE procedure_stats.server_id = $1
                    AND   procedure_stats.collection_time >= run_hours.bucket
                    AND   procedure_stats.collection_time < run_hours.bucket + INTERVAL '1 hour'
                )
            )
            """;

    /// <summary>The query-stats hourly trend — <see cref="BuildHourlyTrendSql"/> over
    /// <see cref="TimescaleSupport.QueryStatsHourlyView"/>.</summary>
    public static string QueryDurationTrendHourlySql(bool withDatabaseFilter)
        => BuildHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView, withDatabaseFilter);

    /// <summary>
    /// The per-collection CTE both raw-tier bucketed statements read (<see cref="BuildBucketedRawTrendSql"/>: the MCP
    /// tools, #3897, and the viewer's and desktop's charts, #4234): per collection, the summed
    /// <c>delta_elapsed_time</c> (to ms) and <c>delta_execution_count</c> and the interval the store HAS (#3653,
    /// measurement A11), the gap-to-previous-collection LAG only where the store never recorded one. (The per-collection
    /// builder this CTE was factored out of had no production caller after #4234 and is gone; its rate rule is stated here.)
    ///
    /// <para><b>The interval is read, not recomputed.</b> Both tables have carried <c>sample_interval_seconds</c>
    /// since their first rung, stamped per row by the shared delta calculator from the SAME two collection instants a
    /// LAG over <c>collection_time</c> would see; reading it keeps a restart honest. A restart zeroes the delta, and
    /// a LAG divides that fabricated 0 by the real elapsed seconds into a confident <c>0.00 ms/sec</c> at exactly the
    /// instant nothing was knowable.</para>
    ///
    /// <para><b>Three states per collection, read distinctly</b> (the #2234 / #3540 contract): <c>MAX</c> over
    /// the collection's rows, because a plan first seen in an otherwise steady pass (a TOP (150) readmission)
    /// stores 0 beside its siblings' real interval and contributes 0 to the sums, so MAX is the collection's
    /// measured interval and is 0 only when EVERY row was unknowable (a restart). That 0 becomes NULL through
    /// <c>NULLIF</c>, so the rates are NULL: an UNRATED collection, never <c>0.00</c>. A NULL MAX is a pre-V128
    /// collection that never recorded an interval, and only there does the LAG stand in, so history renders
    /// exactly as it did. <c>COALESCE(NULLIF(sample_interval_seconds, 0), LAG)</c> would be the WRONG spelling:
    /// it falls back to a fabricated interval on precisely the restart row the marker exists to flag. No
    /// <c>ELSE 0</c> anywhere: a collection with no rate keeps its row with NULL rate columns (#3541 A12: the
    /// collection happened, and <c>effective_start</c> is truthfully its instant).</para>
    ///
    /// <para>With <paramref name="withDatabaseFilter"/>, $4 is the viewer's database clause, applied INSIDE the
    /// aggregates (#5414 M1) and not in the WHERE: the collection's interval is the collection's, whichever databases
    /// were asked about, so a collection where the chosen database had no rows (the query-stats collector keeps only
    /// plans that ran in the last ten minutes, so a quiet database is absent from most collections) stays in the bucket
    /// as zero work over its real seconds instead of vanishing from the denominator and reading the rate high.
    /// <c>matched_rows</c> counts the rows that passed the filter; it decides ONE thing, whether the chosen databases had
    /// any row in the WHOLE window (the tool's <c>empty</c>), never which buckets exist (#5414 round 2: a per-bucket test
    /// dropped the buckets the databases were quiet in, so they read missing instead of 0 and the window read
    /// truncated). Unfiltered, the text carries no FILTER and no <c>matched_rows</c>. Reads the BASE table (no text
    /// columns are projected, so the payload-resolving <c>v_*</c> view is not needed). $1 server_id, $2/$3 window
    /// (naive UTC).</para>
    /// </summary>
    private static string RawCollectionsCte(string rawTable, bool withDatabaseFilter)
    {
        const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
        var sums = withDatabaseFilter
            ? $"COALESCE(SUM(delta_elapsed_time) FILTER (WHERE {match}), 0) / 1000.0 AS total_elapsed_ms,\n"
              + $"        COALESCE(SUM(delta_execution_count) FILTER (WHERE {match}), 0) AS total_executions,\n"
              + $"        COUNT(*) FILTER (WHERE {match}) AS matched_rows,"
            : "SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,\n"
              + "        SUM(delta_execution_count) AS total_executions,";

        return $"""
        raw AS
        (
            SELECT
                collection_time,
                {sums}
                CASE WHEN MAX(sample_interval_seconds) IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                     ELSE NULLIF(MAX(sample_interval_seconds), 0)
                END AS interval_seconds
            FROM {rawTable}
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time
        )
        """;
    }

    /// <summary>
    /// #5449: the per-collection CTE the bucketed raw statement reads for <c>procedure_stats</c>, in place of
    /// <see cref="RawCollectionsCte"/>. The collector stores no row for a procedure that did no work in a cycle, so the stored
    /// collections alone are not the time axis: a minute in which nothing ran is a measured 0, not missing data.
    ///
    /// <para><b>The axis is the collector's SUCCESS runs.</b> A run that stored rows is a point at its stored
    /// <c>collection_time</c> (rows are stamped at the run's start). A run that stored none (<c>collect.collection_log</c> logs it
    /// after it finishes, so its row is later than the run's start) is an idle point carrying work 0 at the instant it began,
    /// the log time less its <c>duration_ms</c>. <c>duration_ms</c> is the SQL and storage time, never more than the run's wall
    /// clock, so the point never lands before the real start. A run owns the rows stamped after the previous SUCCESS run's log
    /// time up to its own; it is idle when it owns none. (The stored collections and the runs are merged into one ordered list,
    /// and a run whose predecessor in that list is another run, or nothing, owns no row: one sort, never a probe of the
    /// stored collections per run.) A run that failed stores nothing and is no point: the next point's gap
    /// covers it. Without a log (imported or old data) the axis is the stored collections alone.</para>
    ///
    /// <para><b>A point's seconds</b> are the gap to the previous point, or NULL (an unrated point, counted by the bucket) when the
    /// gap passes <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>, the collector's own limit: a longer outage is
    /// out of the bucket's seconds, and a shorter one sits in the next point's gap, whose deltas measured it. A stored
    /// collection whose rows all carry interval 0 is a restart (the three-state contract, #3540) and stays unrated. The first
    /// point of the series has no previous point and uses the stored interval it carries (an idle one has none: unrated). A
    /// returning procedure's whole delta lands in the run that stored it and divides by that run's gap, never by the 10-50 minutes
    /// the row itself was last seen: the bucket's total work over its seconds is exact, and that run's own rate and peak read
    /// high.</para>
    ///
    /// <para>Both tables are read from <c>$2 - 1 hour</c> so the first point in the window has its previous point; points before
    /// <c>$2</c> are dropped after the previous point is found. The runs are read to <c>$3 + 1 hour</c>, because a run is logged
    /// after it finishes: an idle run that began inside the window but was logged just past <c>$3</c> still plots its 0. Stored
    /// rows stop at <c>$3</c> and the points do too (a run past <c>$3</c> whose rows were not read is dropped by that final bound,
    /// its point is never earlier than its own start). With <paramref name="withDatabaseFilter"/> the database clause is
    /// applied inside the sums as in <see cref="RawCollectionsCte"/>, and an idle point carries <c>matched_rows</c> 0. Ends in a
    /// CTE named <c>raw</c> with the query grain's columns. $1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    private static string ProcedureCollectionsCte(bool withDatabaseFilter)
    {
        const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
        var maxGap = CollectorDeltaCalculator.DefaultMaxGapSeconds;
        var sums = withDatabaseFilter
            ? $"COALESCE(SUM(delta_elapsed_time) FILTER (WHERE {match}), 0) / 1000.0 AS total_elapsed_ms,\n"
              + $"        COALESCE(SUM(delta_execution_count) FILTER (WHERE {match}), 0) AS total_executions,\n"
              + $"        COUNT(*) FILTER (WHERE {match}) AS matched_rows,"
            : "SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,\n"
              + "        SUM(delta_execution_count) AS total_executions,";
        var matched = withDatabaseFilter ? ", matched_rows" : "";
        var idleMatched = withDatabaseFilter ? ", CAST(0 AS bigint)" : "";

        return $"""
        stored AS
        (
            SELECT
                collection_time,
                {sums}
                MAX(sample_interval_seconds) AS max_interval_seconds
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2 - INTERVAL '{maxGap} seconds'
            AND   collection_time <= $3
            GROUP BY collection_time
        ),
        runs AS
        (
            SELECT
                collection_time AS logged_at,
                COALESCE(duration_ms, 0) AS duration_ms
            FROM {PgSchemaGenerator.CollectSchema}.collection_log
            WHERE server_id = $1
            AND   collector_name = 'procedure_stats'
            AND   status = 'SUCCESS'
            AND   collection_time >= $2 - INTERVAL '{maxGap} seconds'
            AND   collection_time <= $3 + INTERVAL '{maxGap} seconds'
        ),
        events AS
        (
            SELECT collection_time AS event_time, 0 AS is_run, CAST(0 AS bigint) AS duration_ms
            FROM stored
            UNION ALL
            SELECT logged_at, 1, duration_ms
            FROM runs
        ),
        sequenced AS
        (
            SELECT
                event_time,
                is_run,
                duration_ms,
                LAG(is_run) OVER (ORDER BY event_time, is_run) AS previous_is_run
            FROM events
        ),
        points AS
        (
            SELECT collection_time, total_elapsed_ms, total_executions{matched}, max_interval_seconds, TRUE AS is_stored
            FROM stored
            UNION ALL
            SELECT
                q.event_time - q.duration_ms * INTERVAL '1 millisecond',
                CAST(0 AS numeric),
                CAST(0 AS numeric){idleMatched},
                CAST(NULL AS bigint),
                FALSE
            FROM sequenced AS q
            WHERE q.is_run = 1
            AND   COALESCE(q.previous_is_run, 1) = 1
        ),
        gapped AS
        (
            SELECT
                collection_time, total_elapsed_ms, total_executions{matched}, max_interval_seconds, is_stored,
                extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS gap_seconds
            FROM points
        ),
        raw AS
        (
            SELECT
                collection_time,
                total_elapsed_ms,
                total_executions,
                -- #5449: a stored collection whose rows ALL carry interval 0 reads as a restart and stays unrated, its seconds out of
                -- the bucket. This is the chosen three-state rule. It also catches a collection of only 0/0 rows (a quiet server whose
                -- procedure returns at or past the 3600 s gap policy): SQL cannot tell that from a collector restart, so the minute is
                -- unrated and counted in unrated_collections rather than rated 0.
                CASE WHEN is_stored AND max_interval_seconds = 0 THEN NULL
                     WHEN gap_seconds IS NOT NULL THEN CASE WHEN gap_seconds <= {maxGap} THEN gap_seconds END
                     WHEN is_stored THEN NULLIF(max_interval_seconds, 0)
                END AS interval_seconds{matched}
            FROM gapped
            WHERE collection_time >= $2
            AND   collection_time <= $3
        )
        """;
    }

    /// <summary>
    /// The raw-tier duration trend BUCKETED (#3897): the per-collection CTE (see <c>RawCollectionsCte</c>: the same
    /// three-state interval, the same no-ELSE rate arms), with every collection then counted in the bucket of <c>$4</c>
    /// minutes its collection time falls in.
    ///
    /// <para><b>A bucket's rate is its summed work over its summed seconds</b> — time-weighted, never an average of
    /// the per-collection rates, which would weight a 30-second collection the same as a 300-second one (fleet
    /// gaps run p50 299 s, p99 830 s). Only RATED collections enter either sum: a collection whose interval was
    /// unknowable (a restart's stored 0, or the window's first pre-V128 collection with no LAG) carries fabricated
    /// zeros, so it is left out of the numerator AND the denominator rather than diluting the bucket toward 0. A
    /// bucket that held nothing rated has NULL rates — the unrated point #3541 A12 keeps — and
    /// <c>unrated_collections</c> counts every collection left out, so a restart inside a rated bucket is still
    /// reported. The peak is the bucket's worst single collection's rate, so a one-minute regression survives an
    /// hour-wide bucket.</para>
    ///
    /// <para><c>first_collection_time</c> is the bucket's first collection, rated or not, which is what
    /// <c>effective_start</c> reports: the point is stamped at its bucket's start (the first at the window's start,
    /// <c>GREATEST</c>), and a bucket start is not a collection the store held. With <paramref name="withDatabaseFilter"/>,
    /// $4 is the viewer's guarded <c>text[]</c> database filter (#1319), and
    /// the bucket width moves to $5 so the filter's own numbering never shifts; every MCP caller passes <c>false</c>,
    /// so the text and its $4 width are exactly what #3897 always ran. <c>collection_count</c> (#4234) is
    /// <c>COUNT(*)</c> over the same population as <c>first_collection_time</c> — every collection in the bucket,
    /// rated or not — trailing so the MCP tools, which read this statement positionally and stop at
    /// <c>unrated_collections</c>, are unaffected; the viewer's chart uses it to tell a true singleton bucket from
    /// one the bucketing actually merged (see <c>ViewerDataService.QueryTrends.cs</c>'s bucketed reader).
    /// $1 server_id, $2/$3 window (naive UTC), [$4 database filter], the last $ the bucket width in minutes.</para>
    /// </summary>
    public static string BuildBucketedRawTrendSql(string rawTable, bool withDatabaseFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rawTable);

        var widthParam = withDatabaseFilter ? "$5" : "$4";
        /* #5449: procedure_stats stores no row for a cycle in which nothing ran, so its collections are read through the
           collector's runs (ProcedureCollectionsCte); every other table stores every cycle and reads its own rows. */
        var collections = rawTable == "procedure_stats" ? ProcedureCollectionsCte(withDatabaseFilter) : RawCollectionsCte(rawTable, withDatabaseFilter);
        /* #5414 M1: every bucket is read over EVERY collection in it. Round 2: whether the chosen databases had any
           row at all is decided once, for the whole window (the tool's empty), and never per bucket: a bucket the
           databases were quiet in is a measured 0 and a bucket the store covered, and dropping it made it read missing
           and the window read truncated. */
        var matchedColumn = withDatabaseFilter ? ",\n                    matched_rows" : "";
        var windowHasRows = withDatabaseFilter
            ? "\n            WHERE EXISTS (SELECT 1 FROM rated WHERE matched_rows > 0)"
            : "";

        return $"""
            WITH {collections},
            rated AS
            (
                SELECT
                    collection_time,
                    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms END AS rated_elapsed_ms,
                    CASE WHEN interval_seconds > 0 THEN total_executions END AS rated_executions,
                    CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
                    CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second,
                    CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second{matchedColumn}
                FROM raw
            )
            SELECT
                GREATEST(date_bin(CAST({widthParam} AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
                SUM(rated_elapsed_ms) / SUM(rated_seconds) AS elapsed_ms_per_second,
                CAST(SUM(rated_executions) AS DOUBLE PRECISION) / SUM(rated_seconds) AS executions_per_second,
                MAX(elapsed_ms_per_second) AS peak_elapsed_ms_per_second,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) - COUNT(rated_seconds) AS unrated_collections,
                COUNT(*) AS collection_count
            FROM rated{windowHasRows}
            GROUP BY 1
            ORDER BY 1
            """;
    }

    /// <summary>
    /// The hourly-tier duration trend BUCKETED (#3897): <see cref="BuildHourlyTrendSql"/>'s rollup read, with the
    /// rollup's hours then gathered into buckets of <c>$4</c> minutes (a whole number of hours; the tool refuses
    /// any other width on this tier). At 60 minutes each bucket is one hour and every figure is the unbucketed
    /// read's, divided by the same <see cref="HourlyBucketSecondsSql"/>. Wider, a bucket's rate is its hours'
    /// summed work over <see cref="HourlyBucketSecondsSql"/> per hour the rollup holds in it — the per-hour rates
    /// time-weighted, so a bucket the window cuts short, or one whose newest hour has not materialized yet, is
    /// not read low for hours it does not contain. The rollup keeps hourly sums, not the collections inside them,
    /// so the peak on this tier is the bucket's busiest HOUR — the finest grain the rollup holds, and at 60 minutes
    /// the point's own rate; nor is any hour unrated, because its denominator is known. $1 server_id, $2/$3 window
    /// (naive UTC; $3 is EXCLUSIVE — a bucket is stamped at its START, so the hour that begins at $3 lies after
    /// the window and is not read), $4 the bucket width in minutes. With <paramref name="withDatabaseFilter"/> (#5244:
    /// every hourly rollup and the interval successors group by <c>database_name</c>), $4 is the guarded
    /// <c>text[]</c> database filter (<see cref="DatabaseFilter.Clause"/>'s shape) and the width moves to $5, as
    /// <see cref="BuildBucketedRawTrendSql"/> does; off, the text is the one the MCP reader's constants pin.
    /// With <paramref name="coverIdleHours"/> (#5449, the procedure rollups only) the hours <see cref="ProcedureIdleHoursSql"/> finds
    /// (a SUCCESS run of the collector, no raw row) count as zero work, so a quiet hour is in the bucket's seconds as it is on a
    /// store that kept the idle rows; off, the text is unchanged.
    /// </summary>
    public static string BuildBucketedHourlyTrendSql(string hourlyView, bool withDatabaseFilter = false, bool coverIdleHours = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hourlyView);

        /* #5414 M1: the filter goes inside the aggregates, so an hour where the chosen databases had no rollup row
           still counts (zero work) in the bucket's hour count; matched_rows decides empty, once for the whole window
           (round 2: never per bucket, so a quiet bucket reads 0 and not missing). */
        const string match = "$4::text[] IS NULL OR database_name = ANY($4)";
        var sums = withDatabaseFilter
            ? $"COALESCE(SUM(elapsed_time_sum) FILTER (WHERE {match}), 0) / 1000.0 AS elapsed_ms,\n"
              + $"                    COALESCE(SUM(execution_count_sum) FILTER (WHERE {match}), 0) AS executions,\n"
              + $"                    COUNT(*) FILTER (WHERE {match}) AS matched_rows"
            : "SUM(elapsed_time_sum) / 1000.0 AS elapsed_ms,\n"
              + "                    SUM(execution_count_sum) AS executions";
        var windowHasRows = withDatabaseFilter
            ? "\n            WHERE EXISTS (SELECT 1 FROM hourly WHERE matched_rows > 0)"
            : "";
        var widthParam = withDatabaseFilter ? "$5" : "$4";

        /* #5449: procedure rollups hold no row for an hour in which no procedure worked, so the hours the bucket divides by (COUNT(*)
           of the rollup's hours in it) leave out the idle ones and a wider bucket reads high where an older store, which kept the
           idle rows, reads lower. ProcedureIdleHoursSql adds the hours a SUCCESS run of the collector falls in with no raw row in
           them as zero work; an outage hour (no rollup row, no run) stays out of the bucket's seconds. */
        var filled = coverIdleHours
            ? $"""
            ,
            {ProcedureIdleHoursSql()},
            filled AS
            (
                SELECT bucket, elapsed_ms, executions
                FROM hourly
                UNION ALL
                SELECT idle_hours.bucket, 0, 0
                FROM idle_hours
                WHERE NOT EXISTS (SELECT 1 FROM hourly WHERE hourly.bucket = idle_hours.bucket)
            )
            """
            : "";
        var source = coverIdleHours ? "filled" : "hourly";

        return $"""
            WITH hourly AS
            (
                SELECT
                    bucket,
                    {sums}
                FROM {hourlyView}
                WHERE server_id = $1
                AND   bucket >= $2
                AND   bucket < $3
                GROUP BY bucket
            ){filled}
            SELECT
                GREATEST(date_bin(CAST({widthParam} AS integer) * INTERVAL '1 minute', bucket, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
                SUM(elapsed_ms) / (COUNT(*) * {HourlyBucketSecondsSql}) AS elapsed_ms_per_second,
                CAST(SUM(executions) AS DOUBLE PRECISION) / (COUNT(*) * {HourlyBucketSecondsSql}) AS executions_per_second,
                MAX(elapsed_ms / {HourlyBucketSecondsSql}) AS peak_elapsed_ms_per_second,
                MIN(bucket) AS first_collection_time,
                0 AS unrated_collections
            FROM {source}{windowHasRows}
            GROUP BY 1
            ORDER BY 1
            """;
    }

    /// <summary>The procedure-stats hourly trend — <see cref="BuildHourlyTrendSql"/> over
    /// <see cref="TimescaleSupport.ProcedureStatsHourlyView"/>.</summary>
    public static string ProcedureDurationTrendHourlySql(bool withDatabaseFilter)
        => BuildHourlyTrendSql(TimescaleSupport.ProcedureStatsHourlyView, withDatabaseFilter, coverIdleHours: true);
}
