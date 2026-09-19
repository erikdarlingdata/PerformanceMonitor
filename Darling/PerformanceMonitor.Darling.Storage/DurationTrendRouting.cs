/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Where an unkeyed duration trend (query-stats, procedure-stats) reads from, and what it says about what it
/// served — the tier decision, the hourly-tier SQL, the raw-tier SQL (<see cref="BuildRawTrendSql"/>, #3653
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
    /// <c>truncated</c>. Ninety minutes: an hourly bucket can begin up to an hour after a window start that
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
    /// $1 server_id, $2/$3 window (naive UTC).</para>
    /// </summary>
    public static string BuildHourlyTrendSql(string hourlyView, bool withDatabaseFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(hourlyView);

        var filter = withDatabaseFilter
            ? "\nAND   ($4::text[] IS NULL OR database_name = ANY($4))"
            : "";

        return $"""
            SELECT
                bucket AS collection_time,
                SUM(elapsed_time_sum) / 1000.0 / {HourlyBucketSecondsSql} AS elapsed_ms_per_second,
                CAST(SUM(execution_count_sum) AS DOUBLE PRECISION) / {HourlyBucketSecondsSql} AS executions_per_second
            FROM {hourlyView}
            WHERE server_id = $1
            AND   bucket >= $2
            AND   bucket <= $3{filter}
            GROUP BY bucket
            ORDER BY bucket
            """;
    }

    /// <summary>The query-stats hourly trend — <see cref="BuildHourlyTrendSql"/> over
    /// <see cref="TimescaleSupport.QueryStatsHourlyView"/>.</summary>
    public static string QueryDurationTrendHourlySql(bool withDatabaseFilter)
        => BuildHourlyTrendSql(TimescaleSupport.QueryStatsHourlyView, withDatabaseFilter);

    /// <summary>
    /// The raw-tier (per-collection) duration trend over <paramref name="rawTable"/> — <c>query_stats</c> or
    /// <c>procedure_stats</c>, the two plan-cache delta families — with the interval the store HAS (#3653,
    /// measurement A11): per collection, the summed <c>delta_elapsed_time</c> (→ ms) and
    /// <c>delta_execution_count</c> divided by that collection's <c>sample_interval_seconds</c>, and the
    /// gap-to-previous-collection LAG only where the store never recorded one. Projects the same three
    /// columns under the same aliases as <see cref="BuildHourlyTrendSql"/> so one mapper serves both tiers.
    ///
    /// <para><b>The interval is read, not recomputed.</b> Both tables have carried <c>sample_interval_seconds</c>
    /// since their first rung (the #3540 keystone's own words: "perfmon/query_stats already have the column"),
    /// stamped per row by the shared delta calculator from the SAME two collection instants a LAG over
    /// <c>collection_time</c> re-derives — so on a steady series the two agree to the second, and the
    /// difference is exactly the rows where they must not: a row the calculator could not difference (first
    /// sighting, counter reset, a gap past the delta policy — in practice a restart) stores <c>0</c> beside a
    /// <c>0</c> delta, and the LAG happily divided that fabricated 0 by the real elapsed seconds into a confident
    /// <c>0.00 ms/sec</c> at exactly the instant nothing was knowable. The query-stats copies of this read
    /// (the MCP reader's raw const, the viewer's, Lite's two) kept the LAG-only shape after the procedure
    /// copies moved to the stored interval in V128 — the A11 residual #3540 reported rather than rewrote —
    /// and this builder is where it is rewritten once for both apps.</para>
    ///
    /// <para><b>Three states per collection, read distinctly</b> (the #2234 / #3540 contract): <c>MAX</c> over
    /// the collection's rows, because a plan first seen in an otherwise steady pass (a TOP (150) readmission)
    /// stores 0 beside its siblings' real interval and contributes 0 to the sums, so MAX is the collection's
    /// measured interval and is 0 only when EVERY row was unknowable (a restart). That 0 becomes NULL through
    /// <c>NULLIF</c>, so the rates are NULL — an UNRATED point, never <c>0.00</c>. A NULL MAX is a pre-V128
    /// collection that never recorded an interval, and only there does the LAG stand in, so history renders
    /// exactly as it did. <c>COALESCE(NULLIF(sample_interval_seconds, 0), LAG)</c> would be the WRONG spelling:
    /// it falls back to a fabricated interval on precisely the restart row the marker exists to flag.</para>
    ///
    /// <para>No <c>ELSE 0</c>: the first collection of a pre-V128 stretch has a NULL LAG and no rate, and a
    /// restart collection has no rate — both rows are KEPT with NULL rate columns (#3541 A12: the collection
    /// happened, <c>effective_start</c> is truthfully its instant, and a lone collection is "no rate yet", not
    /// an empty window). The MCP reader publishes such a point as null with the reason; the viewer's chart
    /// reader skips it. With <paramref name="withDatabaseFilter"/>, $4 is the viewer's guarded <c>text[]</c>
    /// database filter (#1319); without it the text is what the MCP reader runs. $1 server_id, $2/$3 window
    /// (naive UTC). Reads the BASE table (no text columns are projected, so the payload-resolving
    /// <c>v_*</c> view is not needed).</para>
    /// </summary>
    public static string BuildRawTrendSql(string rawTable, bool withDatabaseFilter)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rawTable);

        var filter = withDatabaseFilter
            ? "\n    AND   ($4::text[] IS NULL OR database_name = ANY($4))"
            : "";

        return $"""
            WITH raw AS
            (
                SELECT
                    collection_time,
                    SUM(delta_elapsed_time) / 1000.0 AS total_elapsed_ms,
                    SUM(delta_execution_count) AS total_executions,
                    CASE WHEN MAX(sample_interval_seconds) IS NULL
                         THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                         ELSE NULLIF(MAX(sample_interval_seconds), 0)
                    END AS interval_seconds
                FROM {rawTable}
                WHERE server_id = $1
                AND   collection_time >= $2
                AND   collection_time <= $3{filter}
                GROUP BY collection_time
            )
            SELECT
                collection_time,
                CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second,
                CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
            FROM raw
            ORDER BY collection_time
            """;
    }

    /// <summary>The query-stats raw trend — <see cref="BuildRawTrendSql"/> over <c>query_stats</c>. The
    /// viewer's <c>QueryDurationTrendSql</c> is this text with the filter; the MCP reader's
    /// <c>DarlingTrendReader.QueryDurationTrendSql</c> is this text without it, by alias since #3653 (it was
    /// the LAG-only copy between #3695 and that alias; see the builder remarks).</summary>
    public static string QueryDurationTrendRawSql(bool withDatabaseFilter)
        => BuildRawTrendSql("query_stats", withDatabaseFilter);

    /// <summary>The procedure-stats raw trend — <see cref="BuildRawTrendSql"/> over <c>procedure_stats</c>:
    /// the V128 idiom the two procedure consts carried by hand, produced by the builder so a test could pin
    /// that the builder IS that idiom before both consts became its aliases (#3653; the viewer's with the
    /// filter, the MCP reader's without) — a provable no-op, because the pin passed first.</summary>
    public static string ProcedureDurationTrendRawSql(bool withDatabaseFilter)
        => BuildRawTrendSql("procedure_stats", withDatabaseFilter);

    /// <summary>The procedure-stats hourly trend — <see cref="BuildHourlyTrendSql"/> over
    /// <see cref="TimescaleSupport.ProcedureStatsHourlyView"/>.</summary>
    public static string ProcedureDurationTrendHourlySql(bool withDatabaseFilter)
        => BuildHourlyTrendSql(TimescaleSupport.ProcedureStatsHourlyView, withDatabaseFilter);
}
