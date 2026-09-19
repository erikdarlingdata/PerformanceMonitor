/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One point on a Performance-Trends chart — the viewer copy of Lite's <c>QueryTrendPoint</c>
/// (LocalDataService.QueryStats.cs). <see cref="Value"/> is the per-second rate the chart plots
/// (elapsed ms/sec for the duration trends, executions/sec for the execution-count trend);
/// <see cref="ExecutionCount"/> carries the executions/sec rate the duration trends also compute
/// (unused by the execution-count trend). CollectionTime is naive UTC — the chart converts it
/// through <see cref="ViewerTimeHelper.ForDisplay"/>.
/// </summary>
public sealed class QueryTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public double Value { get; set; }
    public long ExecutionCount { get; set; }
}

/// <summary>
/// One routed Performance-Trends series and what it actually covers (#3653): the points, the tier they were
/// read from, and the coverage description — the MCP trio's <c>DurationTrendResult</c> shape (#3590), so the
/// chart can SAY what it served the way the tool's payload does (<c>source</c>, <c>effective_start</c>,
/// <c>truncated</c>) rather than plotting four days under an axis that says seven.
/// </summary>
/// <param name="Points">The plotted points, oldest first; a NULL-rate row is not among them (see the reader).</param>
/// <param name="Tier">The tier <see cref="DurationTrendRouting.ResolveTier"/> picked: raw per-collection rows, or the hourly rollup.</param>
/// <param name="EffectiveStartUtc">The first served point, or the requested start when nothing came back — <see cref="DurationTrendRouting.DescribeCoverage"/>.</param>
/// <param name="Truncated">True when the series' head sits more than <see cref="DurationTrendRouting.TruncationSlack"/> after the requested start: the tier did not hold the window's head.</param>
public sealed record QueryTrendSeries(List<QueryTrendPoint> Points, RetentionTier Tier, DateTime EffectiveStartUtc, bool Truncated)
{
    /// <summary>The payload word for the tier — <see cref="DurationTrendRouting.SourceWord"/>, so the chart and the tool use one vocabulary.</summary>
    public string Source => DurationTrendRouting.SourceWord(Tier);

    /// <summary>An empty raw series with nothing to describe — the shape the untouched reads return so every caller handles one type.</summary>
    public static QueryTrendSeries Empty(DateTime startUtc) => new(new List<QueryTrendPoint>(), RetentionTier.Raw, startUtc, false);
}

/// <summary>
/// The Query Store duration trend and what it actually covers (#3653): the points, the #2736 route that
/// produced them, and the coverage facts get_query_store_duration_trend publishes beside its points — so the
/// chart can say what it served the way the tool's payload does. Not a <see cref="QueryTrendSeries"/>, and the
/// difference is the point: that record's tier is <see cref="RetentionTier"/> (raw or hourly, one relation per
/// read) because its ladder picks ONE tier; this trend's route is a materialization WATERMARK — the corrected
/// hourly serves the window below <see cref="QueryStoreTrendRouting.QueryStoreTrendRoute.RawStartUtc"/> and
/// the raw arms serve from it — so "which tier" is not a question it can answer with one enum value, and
/// forcing it into one (Hourly for a rollup+raw read) would have the chart lie in the payload's own vocabulary.
/// </summary>
/// <param name="Points">The plotted points, oldest first; the window's first united point (NULL rate, #3541 A12) is not among them.</param>
/// <param name="Route">The #2736 route the read took: raw-only, or the rollup below <c>RawStartUtc</c> and raw from it.</param>
/// <param name="EffectiveStartUtc">The first served point, or the requested start when nothing came back — <see cref="DurationTrendRouting.DescribeCoverage"/>, the MCP payload's <c>effective_start</c>.</param>
/// <param name="UnservedBeforeUtc">
/// The rollup's materialized floor when it sits ABOVE the requested start — <see cref="QueryStoreTrendRouting.UnservedBefore"/>,
/// the payload's <c>routing.unserved_before</c> — or null when the route reached the window's start. This, not the
/// sibling series' <c>Truncated</c>, is what the chart discloses on: a floor above the start is a MEASURED fact
/// that the head was not served (missing, not zero; <c>--backfill-rollups</c> reaches it), where a late first
/// point on a route that DID reach the start is a quiet server, which is data and not a defect in the axis.
/// </param>
public sealed record QueryStoreTrendSeries(
    List<QueryTrendPoint> Points, QueryStoreTrendRouting.QueryStoreTrendRoute Route, DateTime EffectiveStartUtc, DateTime? UnservedBeforeUtc)
{
    /// <summary>The payload word for the route — <see cref="QueryStoreTrendRouting.SourceWord"/> (<c>rollup+raw</c> / <c>raw</c>), so the chart and the tool use one vocabulary.</summary>
    public string Source => QueryStoreTrendRouting.SourceWord(Route);

    /// <summary>Whether the window's head went unserved — <see cref="UnservedBeforeUtc"/> is set. The chart's "data begins" clause hangs on this.</summary>
    public bool HeadUnserved => UnservedBeforeUtc is not null;
}

public sealed partial class ViewerDataService
{
    /* The four Performance-Trends reads — Lite's Get{Query,Procedure,ExecutionCount}DurationTrendAsync
       (LocalDataService.QueryStats.cs:1029-1168) + GetQueryStoreDurationTrendAsync
       (LocalDataService.QueryStore.cs:582) ported to Postgres. The SQL is byte-identical to Lite's
       apart from the table name (Lite reads the v_* views; the viewer reads the base tables, matching
       W1f-1's Top-Queries read) — every operator Lite uses (date_trunc('second', …), the
       LAG() OVER (ORDER BY collection_time) fallback interval, extract(epoch FROM interval),
       CAST(… AS double precision), the positional $1/$2/$3 placeholders) is native Postgres, so no
       dialect rewrite is needed here (unlike the heatmap's time_bucket/ARG_MAX). The per-snapshot
       rate = summed delta over the collection's interval: the STORED sample_interval_seconds where the
       rows have one (#3653 A11 — the three delta-family trends read the interval the store has, with
       0 meaning unknowable and NULL meaning pre-V128; the LAG over collection_time stands in only for
       NULL), and a NULL interval_seconds — a restart pass, or the first row of a pre-V128 stretch — makes
       the CASE yield NULL: the rate is unknowable, and the reader skips the point rather than plotting
       the fabricated 0 it used to (#3653; the MCP readers stopped in #3642, the procedure copy in
       #3630). Summed bigint deltas come back as Postgres numeric, so the reads go through Convert
       tolerantly.
       $1 server_id, $2 window start, $3 window end (naive UTC), $4 the #1319 database filter.

       #3653: the query-stats and procedure-stats trends are ROUTED (DurationTrendRouting, the ladder the
       MCP trio took in #3590): raw for a window raw can serve, the hourly rollup otherwise, and the
       series carries what it served (QueryTrendSeries) so the chart can say so. Before this the tab read
       raw only, and a 7-day chart on a TimescaleDB store plotted the 4 days raw still held under an axis
       that said 7. The execution-count trend routes the same way and, on the hourly tier, reads the
       duration statement's second column (it is that read drawn on its own chart). The Query Store trend
       keeps its own routing (#2736, QueryStoreTrendRouting): its rollup is the corrected hourly and its
       seam is a materialization watermark rather than a tier choice, so it is not on this ladder; its
       chart-side disclosure (the floor the rollup has materialized to, which the MCP tool has published as
       routing.unserved_before since #2736) rides its own series type, QueryStoreTrendSeries. */

    /// <summary>
    /// Query-stats duration trend: elapsed ms/sec + executions/sec per collection snapshot —
    /// <see cref="DurationTrendRouting.QueryDurationTrendRawSql"/> with the viewer's $4 database filter (#3653
    /// A11). The denominator is the collection's STORED <c>sample_interval_seconds</c> (MAX over its rows;
    /// 0 → NULL, so a restart pass is unrated and the reader skips it rather than plotting 0.00 ms/sec), and
    /// the LAG over <c>collection_time</c> this read used to recompute stands in only for a pre-V128 collection
    /// that never recorded one — the same three-state read <see cref="ProcedureDurationTrendSql"/> has carried
    /// since V128; the builder's remarks state the rule once. A static readonly rather than a const because
    /// the Storage side is a builder (one text with and one without the filter), as for the hourly twin below.
    /// </summary>
    public static readonly string QueryDurationTrendSql =
        DurationTrendRouting.QueryDurationTrendRawSql(withDatabaseFilter: true);

    /// <summary>
    /// The hourly-tier twin of <see cref="QueryDurationTrendSql"/> (#3653): the SAME text the MCP reader's
    /// <c>DarlingTrendReader.QueryDurationTrendHourlySql</c> runs, built by
    /// <see cref="DurationTrendRouting.QueryDurationTrendHourlySql"/> with the viewer's $4 database filter
    /// (the rollup groups by <c>database_name</c>, so the #1319 filter survives the routing). Chosen per read
    /// by <see cref="DurationTrendRouting.ResolveTier"/>; the bucket-width denominator means it has no
    /// unrated first point. See the builder for the trade it states.
    /// </summary>
    public static readonly string QueryDurationTrendHourlySql =
        DurationTrendRouting.QueryDurationTrendHourlySql(withDatabaseFilter: true);

    /// <summary>
    /// Procedure-stats duration trend: elapsed ms/sec + executions/sec per collection snapshot.
    ///
    /// <para>#3540 (V128): the interval is the collection's STORED one where the rows have it — <c>MAX</c>
    /// over the collection's rows, because a plan first seen in an otherwise steady pass (a TOP (150)
    /// readmission) carries 0 beside its siblings' real interval and contributes 0 to the sums; MAX is 0
    /// only when EVERY row was unknowable (a restart), and that 0 becomes NULL through <c>NULLIF</c> so the
    /// rates are NULL and the reader drops the point rather than rendering 0.00 ms/sec. NULL (a pre-V128
    /// collection that never recorded one) falls back to the LAG over collection_time this read always
    /// used, so history renders exactly as it did. No <c>ELSE 0</c>: the first row of a pre-V128 series is
    /// absent rather than a fabricated 0.0, the same correction V127 made for the wait trends.</para>
    /// </summary>
    public const string ProcedureDurationTrendSql = """
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
            FROM procedure_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN total_elapsed_ms / interval_seconds END AS elapsed_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>The hourly-tier twin of <see cref="ProcedureDurationTrendSql"/> (#3653) over
    /// <c>procedure_stats_hourly</c> — <see cref="DurationTrendRouting.ProcedureDurationTrendHourlySql"/> with
    /// the $4 filter; see <see cref="QueryDurationTrendHourlySql"/>.</summary>
    public static readonly string ProcedureDurationTrendHourlySql =
        DurationTrendRouting.ProcedureDurationTrendHourlySql(withDatabaseFilter: true);

    /// <summary>
    /// Query Store duration trend: each interval's work, placed at the time that work RAN.
    ///
    /// <para>#1841 tier 2 corrected this, and the fix is a different shape from the other four Query
    /// Store reads. The three trends above sum per-cycle DELTA columns; Query Store has none, so its
    /// cumulative per-interval snapshots — re-fetched every cycle while the interval stays open — used to
    /// make an interval that reached 40 executions charge 10, then 25, then 40 to three successive
    /// points. Tier 1 deliberately left that in, because dedup ALONE makes this chart worse: it keeps ONE
    /// row per interval at the collection where the interval closed, and Query Store's default
    /// INTERVAL_LENGTH_MINUTES is 60 against a 5-minute cadence, so twelve snapshots collapse onto one
    /// collection_time and a 1-hour window renders a SINGLE point valued 0.</para>
    ///
    /// <para>What unlocks it is the x-axis, not the dedup: <c>interval_start_time_utc</c> is the
    /// interval's own start boundary, converted to UTC AT COLLECTION, so it shares the clock with
    /// collection_time. (The premise this was blocked on — that the interval clock is server-LOCAL — was
    /// wrong on both halves: Query Store's interval start_time and first_execution_time are both
    /// <c>datetimeoffset</c>, and the collector already normalized them through
    /// <c>DateTimeOffset.UtcDateTime</c> before storing.) Deduped and placed at its start, each interval
    /// contributes its true total exactly once at the hour it ran, and the series resolution becomes
    /// Query Store's OWN interval length — the honest resolution of this source.</para>
    ///
    /// <para><b>The legacy boundary.</b> Rows collected before tier 2 carry no interval start and none can
    /// be reconstructed, so they keep the pre-tier-2 treatment exactly: un-deduped, at collection_time,
    /// still overstating. The arms split on <c>interval_start_time_utc IS NULL</c>, which partitions the
    /// rows with no overlap and no gap. Mirrors Lite's GetQueryStoreDurationTrendAsync.</para>
    /// <para><b>#2736: this is now the FALLBACK, not the read.</b> The rank-over-raw below costs the whole
    /// slab regardless of the window; on stores with a materialized
    /// <c>query_store_stats_corrected_hourly</c> the read routes through
    /// <see cref="QueryStoreDurationTrendRollupSql"/> and this shape runs only where it is affordable
    /// (no rollup: plain PostgreSQL, or nothing materialized yet). Its ±slab stays untouched on purpose —
    /// see <see cref="QueryStoreTrendRouting"/>.</para>
    /// <para>The first placed interval in the window carries NULL rates, not 0 (#3653; #3642 on the MCP copy):
    /// its LAG has nothing to difference against, so its rate is unknowable and the reader skips it. The
    /// rollup route's builder has applied the same rule to its first bucket since #3642.</para>
    /// </summary>
    public const string QueryStoreDurationTrendSql = """
        WITH placed AS
        (
            /* Arm 1 (#1841 tier 2) — rows carrying the interval identity. Dedup to the interval's FINAL
               cumulative snapshot, then place it at interval_start_time_utc: the hour the work ran, not
               the cycle that last fetched it. Both halves are needed; see the remarks for why dedup alone
               collapses this series and placement alone leaves it inflated. */
            SELECT
                interval_start_time_utc AS point_time,
                execution_count,
                avg_duration_us
            FROM
            (
                SELECT
                    interval_start_time_utc,
                    execution_count,
                    avg_duration_us,
                    ROW_NUMBER() OVER
                    (
                        PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                        ORDER BY collection_time DESC, execution_count DESC
                    ) AS rn
                FROM query_store_stats
                WHERE server_id = $1
                /* Windowed on the column this arm PLACES its points at (#1892), which inside the IS NOT NULL
                   guard below is what COALESCE(interval_start_time_utc, collection_time) resolves to anyway.
                   Filtering on collection_time here put a point outside the range the caller asked for, and
                   dropped the range's final interval because its closing fetch had not happened yet. */
                AND   interval_start_time_utc >= $2
                AND   interval_start_time_utc <= $3
                /* Chunk-exclusion bounds only; see the slicer for why the floor is free and why the ceiling
                   is a month rather than tight. */
                AND   collection_time >= $2 - interval '1 day'
                AND   collection_time <= $3 + interval '30 days'
                AND   interval_start_time_utc IS NOT NULL
                AND   ($4::text[] IS NULL OR database_name = ANY($4))
            ) AS identified
            WHERE rn = 1

            UNION ALL

            /* Arm 2 — rows collected before tier 2. No interval start exists and none can be
               reconstructed, so these keep the pre-tier-2 treatment byte for byte: un-deduped, placed at
               collection_time, still overstating. The split is on IS NULL / IS NOT NULL, so the two arms
               partition the rows exactly — nothing counted twice, nothing dropped. */
            SELECT
                collection_time AS point_time,
                execution_count,
                avg_duration_us
            FROM query_store_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   interval_start_time_utc IS NULL
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ),
        raw AS
        (
            SELECT
                point_time,
                SUM(execution_count * avg_duration_us / 1000.0) AS total_duration_ms,
                SUM(execution_count) AS total_executions,
                extract(epoch FROM (date_trunc('second', point_time) - date_trunc('second', LAG(point_time) OVER (ORDER BY point_time)))) AS interval_seconds
            FROM placed
            GROUP BY point_time
        )
        SELECT
            point_time AS collection_time,
            CASE WHEN interval_seconds > 0 THEN total_duration_ms / interval_seconds END AS duration_ms_per_second,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
        FROM raw
        ORDER BY point_time
        """;

    /// <summary>
    /// The rollup-routed Query Store duration trend (#2736): the materialized window portion served from
    /// <c>query_store_stats_corrected_hourly</c> as a rollup scan, the unmaterialized tail from the raw arms
    /// with tail-tight bounds instead of the fixed ±slab. Built by
    /// <see cref="QueryStoreTrendRouting.BuildRollupTrendSql"/> — the SAME builder the MCP reader's twin
    /// uses, so the browser and the desktop viewer cannot disagree about the same hour; this copy carries
    /// the #1319 database filter as $5 (the corrected hourly groups by database_name, so the filter
    /// survives the routing). Chosen per read by <see cref="QueryStoreTrendRouting.ResolveAsync"/>; a store
    /// without the rollup (plain PostgreSQL, or nothing materialized yet) keeps
    /// <see cref="QueryStoreDurationTrendSql"/> byte for byte.
    /// $1 server_id, $2/$3 window, $4 the raw boundary (naive UTC), $5 database filter.
    /// </summary>
    public static readonly string QueryStoreDurationTrendRollupSql =
        QueryStoreTrendRouting.BuildRollupTrendSql(withDatabaseFilter: true);

    /// <summary>Execution-count trend: executions/sec per collection snapshot from query_stats — the
    /// executions column of <see cref="QueryDurationTrendSql"/> on its own, so it reads the interval the same
    /// way (#3653 A11): the collection's stored <c>sample_interval_seconds</c>, 0 → NULL (unrated, skipped),
    /// and the LAG over <c>collection_time</c> only for a pre-V128 collection. The first collection of such a
    /// stretch has a NULL rate, not 0 (#3541 A12), and the reader skips it. Kept as its own text rather than
    /// reading the duration statement's second column (as the hourly route does) because the raw tier's
    /// elapsed sum is a cost this chart does not need paid.</summary>
    public const string ExecutionCountTrendSql = """
        WITH raw AS
        (
            SELECT
                collection_time,
                SUM(delta_execution_count) AS total_executions,
                CASE WHEN MAX(sample_interval_seconds) IS NULL
                     THEN extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time))))
                     ELSE NULLIF(MAX(sample_interval_seconds), 0)
                END AS interval_seconds
            FROM query_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   ($4::text[] IS NULL OR database_name = ANY($4))
            GROUP BY collection_time
        )
        SELECT
            collection_time,
            CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
        FROM raw
        ORDER BY collection_time
        """;

    /// <summary>
    /// Query-stats duration trend over [<paramref name="startUtc"/>, <paramref name="endUtc"/>], routed by
    /// <see cref="DurationTrendRouting.ResolveTier"/> (#3653): raw <c>query_stats</c> for a window raw can
    /// serve, <c>query_stats_hourly</c> otherwise. <paramref name="nowUtc"/> is the WALL CLOCK the age rule
    /// measures against — never the window's end (a two-hour window from ten days ago is not "recent"
    /// relative to now, whatever it is relative to its own end) — and defaults to the real clock; a test
    /// passes a fixed instant to pin the boundary.
    /// </summary>
    public Task<QueryTrendSeries> GetQueryDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        DateTime? nowUtc = null, CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            QueryDurationTrendSql, QueryDurationTrendHourlySql, TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView,
            static rollups => rollups.QueryGrainHourly, serverId, startUtc, endUtc, databaseNames, nowUtc, cancellationToken);

    /// <summary>Procedure-stats duration trend over the window — the procedure twin of
    /// <see cref="GetQueryDurationTrendAsync"/>, routed the same way over <c>procedure_stats_hourly</c>.</summary>
    public Task<QueryTrendSeries> GetProcedureDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        DateTime? nowUtc = null, CancellationToken cancellationToken = default)
        => ReadRoutedDurationTrendAsync(
            ProcedureDurationTrendSql, ProcedureDurationTrendHourlySql, TimescaleSupport.ProcedureStatsHourlyView, TimescaleSupport.ProcedureStatsDailyView,
            static rollups => rollups.ProcedureGrainHourly, serverId, startUtc, endUtc, databaseNames, nowUtc, cancellationToken);

    /// <summary>
    /// The shared body of the two routed reads (#3653): probe what the store has and has materialized (the
    /// viewer's cached <see cref="GetRollupAvailabilityAsync"/>), resolve the tier, run that tier's SQL, and
    /// describe what came back. One method so the query and procedure trends cannot drift in how they route,
    /// read, or describe coverage — the MCP reader's <c>ReadRoutedDurationTrendAsync</c> arrangement.
    /// <paramref name="hourlyAvailable"/> picks the GRAIN's own availability flag off the probe: a store with
    /// the query rollup but not the procedure one (a failed ensure sweep, #1664's failure isolation) must
    /// route the procedure trend to raw, whatever the query grain is doing.
    /// </summary>
    private async Task<QueryTrendSeries> ReadRoutedDurationTrendAsync(
        string rawSql, string hourlySql, string hourlyView, string dailyView, Func<RollupAvailability, bool> hourlyAvailable,
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, DateTime? nowUtc,
        CancellationToken cancellationToken)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = DurationTrendRouting.ResolveTier(
            startUtc, nowUtc ?? DateTime.UtcNow, hourlyAvailable(rollups), coverage.For(hourlyView, dailyView));

        var points = await ReadDurationTrendAsync(
            tier == RetentionTier.Raw ? rawSql : hourlySql, serverId, startUtc, endUtc, databaseNames, cancellationToken);
        var (effectiveStart, truncated) = DurationTrendRouting.DescribeCoverage(points.Count > 0 ? points[0].CollectionTime : null, startUtc);
        return new QueryTrendSeries(points, tier, effectiveStart, truncated);
    }

    /// <summary>
    /// Query Store duration trend over the window — routed through the corrected rollup where the store has
    /// one (#2736; see <see cref="QueryStoreDurationTrendRollupSql"/>). The raw-only read is kept unchanged
    /// as the fallback for stores without a materialized rollup, where it is affordable.
    /// <para>#3653: returns the series WITH its route and coverage (<see cref="QueryStoreTrendSeries"/>) rather
    /// than bare points, so the chart can disclose what it served. Before this the read already knew the
    /// rollup's floor (<see cref="QueryStoreTrendRouting.QueryStoreTrendRoute.RollupFloorUtc"/>, resolved for
    /// the seam) and threw it away, and a seven-day chart on a store whose rollup was never backfilled plotted
    /// the days it had under an axis that said seven — the sibling charts' #3666 defect, on the one chart that
    /// PR left out because its routing is a watermark and not the tier ladder. The MCP tool has disclosed
    /// this floor as <c>routing.unserved_before</c> since #2736; the chart now reads the same rule.</para>
    /// </summary>
    public async Task<QueryStoreTrendSeries> GetQueryStoreDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var route = await QueryStoreTrendRouting.ResolveAsync(_dataSource, cancellationToken);
        var points = route.UseRollup
            ? await ReadQueryStoreRollupTrendAsync(route, serverId, startUtc, endUtc, databaseNames, cancellationToken)
            : await ReadDurationTrendAsync(QueryStoreDurationTrendSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);

        /* Coverage the MCP tool's way: effective_start from the first served point (the shared rule, so the
           chart and the payload name the same instant), the unserved head from the route's measured floor. */
        var (effectiveStart, _) = DurationTrendRouting.DescribeCoverage(points.Count > 0 ? points[0].CollectionTime : null, startUtc);
        return new QueryStoreTrendSeries(points, route, effectiveStart, QueryStoreTrendRouting.UnservedBefore(route, startUtc));
    }

    /// <summary>The rollup-routed read (#2736): <see cref="QueryStoreDurationTrendRollupSql"/> with the route's
    /// raw boundary as $4 and the database filter as $5.</summary>
    private async Task<List<QueryTrendPoint>> ReadQueryStoreRollupTrendAsync(
        QueryStoreTrendRouting.QueryStoreTrendRoute route, int serverId, DateTime startUtc, DateTime endUtc,
        IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var items = new List<QueryTrendPoint>();

        await using var command = _dataSource.CreateCommand(QueryStoreDurationTrendRollupSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(route.RawStartUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* #3541 A12: the shared builder returns NULL rates for a raw-arm point that opens the window (its
               LAG has nothing to difference against); a rollup point is always rated over its bucket width
               (#3653 A8). A chart has nowhere to draw "unknown", so the point is skipped here rather than
               coerced to the 0 it used to be plotted as. */
            if (reader.IsDBNull(1))
                continue;

            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = Convert.ToDouble(reader.GetValue(1)),
                ExecutionCount = reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2)),
            });
        }

        return items;
    }

    /// <summary>
    /// Shared reader for the three duration trends (same column shape: collection_time,
    /// value/sec, executions/sec). Value = column 1; ExecutionCount = column 2 truncated to long
    /// (Lite's <c>(long)ToDouble(…)</c>).
    /// </summary>
    private Task<List<QueryTrendPoint>> ReadDurationTrendAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
        => ReadTrendPointsAsync(sql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 1, executionsOrdinal: 2, cancellationToken);

    /// <summary>
    /// The one loop every trend read here goes through. <paramref name="valueOrdinal"/> is the column plotted
    /// as <see cref="QueryTrendPoint.Value"/>; <paramref name="executionsOrdinal"/> the column carried as
    /// <see cref="QueryTrendPoint.ExecutionCount"/>, or null for a series that has none. The execution-count
    /// trend's hourly route reads the SAME rollup statement the duration trend does and plots its second
    /// column, which is why the ordinal is a parameter rather than a fourth SQL text.
    /// </summary>
    private async Task<List<QueryTrendPoint>> ReadTrendPointsAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames,
        int valueOrdinal, int? executionsOrdinal, CancellationToken cancellationToken)
    {
        var items = new List<QueryTrendPoint>();

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is an unknowable interval — a collection whose stored sample_interval_seconds is 0 (a
               restart pass, #3540 V128; every raw trend here reads the stored interval since #3653 A11), or the
               first collection of a pre-V128 stretch, whose LAG has nothing to difference against (#3653, every
               raw trend here since the ELSE 0 came out). The point is SKIPPED, not read as 0
               and not interpolated across: a chart has nowhere to draw "unknown", and the fabricated quiet
               instant it used to plot dragged every series' opening toward zero. The hourly tier never
               produces one (its denominator is the bucket width), so this is a no-op on that route. */
            if (reader.IsDBNull(valueOrdinal))
            {
                continue;
            }

            items.Add(new QueryTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                Value = Convert.ToDouble(reader.GetValue(valueOrdinal)),
                ExecutionCount = executionsOrdinal is int executions && !reader.IsDBNull(executions)
                    ? (long)Convert.ToDouble(reader.GetValue(executions))
                    : 0,
            });
        }

        return items;
    }

    /// <summary>
    /// Execution-count trend over the window (single value column; no ExecutionCount), routed like its
    /// duration sibling (#3653): raw <see cref="ExecutionCountTrendSql"/> for a window raw can serve; for an
    /// older window the SAME <see cref="QueryDurationTrendHourlySql"/> statement the duration trend runs,
    /// plotting its <c>executions_per_second</c> column — the two charts are one read's two columns drawn
    /// apart, and on the hourly tier they literally are.
    /// </summary>
    public async Task<QueryTrendSeries> GetExecutionCountTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = DurationTrendRouting.ResolveTier(
            startUtc, nowUtc ?? DateTime.UtcNow, rollups.QueryGrainHourly,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        var points = tier == RetentionTier.Raw
            ? await ReadTrendPointsAsync(ExecutionCountTrendSql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 1, executionsOrdinal: null, cancellationToken)
            : await ReadTrendPointsAsync(QueryDurationTrendHourlySql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 2, executionsOrdinal: null, cancellationToken);
        var (effectiveStart, truncated) = DurationTrendRouting.DescribeCoverage(points.Count > 0 ? points[0].CollectionTime : null, startUtc);
        return new QueryTrendSeries(points, tier, effectiveStart, truncated);
    }
}
