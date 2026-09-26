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
using PerformanceMonitor.Common;
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
/// <c>window_truncated</c>) rather than plotting four days under an axis that says seven.
/// </summary>
/// <param name="Points">The plotted points, oldest first; a NULL-rate row is not among them (see the reader).</param>
/// <param name="Tier">The tier <see cref="DurationTrendRouting.ResolveTier"/> picked: raw (bucketed, #4234) or the hourly rollup.</param>
/// <param name="EffectiveStartUtc">The first served point, or the requested start when nothing came back — <see cref="DurationTrendRouting.DescribeCoverage"/>.</param>
/// <param name="Truncated">True when the series' head sits more than <see cref="DurationTrendRouting.TruncationSlack"/> after the requested start: the tier did not hold the window's head.</param>
/// <param name="BucketMinutes">The width the raw tier gathered its points at (#4234), or 0 when every point is one collection (every bucket held one, or the tier is hourly).</param>
public sealed record QueryTrendSeries(List<QueryTrendPoint> Points, RetentionTier Tier, DateTime EffectiveStartUtc, bool Truncated, int BucketMinutes = 0)
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
    /// Query-stats duration trend: elapsed ms/sec + executions/sec per BUCKET —
    /// <see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> over <c>query_stats</c> with the viewer's $4
    /// database filter (#4234; #3653 A11 before it). Until #4234 this read was the per-collection builder's
    /// output (<see cref="DurationTrendRouting.QueryDurationTrendRawSql"/>), and a 7-day chart could hold as
    /// many rows as the window had collections — the issue's measured number for the sibling wait/perfmon
    /// charts this same fix applied to. The per-collection CTE and its three-state interval (the collection's
    /// STORED <c>sample_interval_seconds</c>, MAX over its rows, 0 → NULL so a restart pass is unrated, NULL →
    /// the LAG over <c>collection_time</c> for a pre-V128 collection that never recorded one) are unchanged —
    /// only the aggregation into buckets is new; the builder's remarks state the rate rule once. The reader
    /// (<c>ReadBucketedDurationTrendAsync</c>) stamps a bucket at its own <c>first_collection_time</c>, not
    /// <c>bucket_start</c>, when every bucket the call returned held exactly one collection — the PR #4304
    /// pattern <see cref="ViewerDataService.WaitTrendsSql"/> established. A static readonly rather than a const
    /// because the Storage side is a builder (one text with and one without the filter), as for the hourly
    /// twin below.
    /// </summary>
    public static readonly string QueryDurationTrendSql =
        DurationTrendRouting.BuildBucketedRawTrendSql("query_stats", withDatabaseFilter: true);

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
    /// Procedure-stats duration trend: elapsed ms/sec + executions/sec per BUCKET —
    /// <see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> over <c>procedure_stats</c> with the
    /// viewer's $4 database filter (#4234). Until #4234 this read was the per-collection builder's output
    /// (<see cref="DurationTrendRouting.ProcedureDurationTrendRawSql"/>, itself an alias since #3653); see
    /// <see cref="QueryDurationTrendSql"/> for why the read is bucketed now and what stays the same.
    ///
    /// <para>#3540 (V128): the interval is the collection's STORED one where the rows have it — <c>MAX</c>
    /// over the collection's rows, because a plan first seen in an otherwise steady pass (a TOP (150)
    /// readmission) carries 0 beside its siblings' real interval and contributes 0 to the sums; MAX is 0
    /// only when EVERY row was unknowable (a restart), and that 0 becomes NULL through <c>NULLIF</c> so the
    /// rates are NULL and the reader drops the point rather than rendering 0.00 ms/sec. NULL (a pre-V128
    /// collection that never recorded one) falls back to the LAG over collection_time this read always
    /// used, so history renders exactly as it did. No <c>ELSE 0</c>: the first row of a pre-V128 series is
    /// absent rather than a fabricated 0.0, the same correction V127 made for the wait trends. The rule is
    /// stated once, on the builder; <see cref="QueryDurationTrendSql"/> reads it the same way.</para>
    /// </summary>
    public static readonly string ProcedureDurationTrendSql =
        DurationTrendRouting.BuildBucketedRawTrendSql("procedure_stats", withDatabaseFilter: true);

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

    /// <summary>
    /// The table-routed twin of <see cref="QueryStoreDurationTrendSql"/> (#4310 site 3): reads
    /// <c>query_store_interval_wide</c> directly instead of the raw arms' interval dedupe — the table already
    /// holds the latest snapshot per interval, every outcome (<see cref="QueryStoreIntervalWide.UpsertSql"/>'s
    /// running-maximum guard) — so this drops arm 1's identity subquery/ROW_NUMBER entirely and keeps arm 2
    /// (the legacy, un-deduped rows with no <c>interval_start_time_utc</c>) reading the SAME base table, exactly
    /// as <see cref="ViewerDataService.QueryStoreTopTablePrefix"/> does for the grid's top read. $1 server_id,
    /// $2 the gate's own clamp (<see cref="QueryStoreIntervalWide.ClampedStart"/>), $3/$4 window end (naive UTC;
    /// $3 binds arm 1's placement filter, $4 binds arm 2's collection-time filter — both are the caller's
    /// unclamped <c>endUtc</c>), $5 database filter.
    /// </summary>
    public const string QueryStoreDurationTrendTableSql = """
        WITH placed AS
        (
            SELECT
                interval_start_time_utc AS point_time,
                execution_count,
                avg_duration_us
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   interval_start_time_utc >= $2
            AND   interval_start_time_utc <= $3
            AND   interval_start_time_utc IS NOT NULL
            AND   ($5::text[] IS NULL OR database_name = ANY($5))

            UNION ALL

            /* Arm 2 — rows collected before tier 2, which the table carries unchanged from raw (the writer
               applies every outcome; a NULL interval start is never deduped away). Byte-identical to
               QueryStoreDurationTrendSql's own arm 2 apart from the source table. */
            SELECT
                collection_time AS point_time,
                execution_count,
                avg_duration_us
            FROM query_store_interval_wide
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $4
            AND   interval_start_time_utc IS NULL
            AND   ($5::text[] IS NULL OR database_name = ANY($5))
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

    /// <summary>Execution-count trend: executions/sec per BUCKET from query_stats (#4234) — the executions
    /// column of <see cref="QueryDurationTrendSql"/>'s bucketing pattern on its own, so it reads the interval
    /// the same way (#3653 A11): the collection's stored <c>sample_interval_seconds</c>, 0 → NULL (unrated,
    /// left out of the bucket's numerator and denominator), and the LAG over <c>collection_time</c> only for a
    /// pre-V128 collection. Kept as its own text rather than reading the duration statement's second column (as
    /// the hourly route does) because the raw tier's elapsed sum is a cost this chart does not need paid. Has
    /// no shared builder (unlike its duration siblings) since it is the only raw trend that projects one rate;
    /// <c>first_collection_time</c> and <c>collection_count</c> are stated here rather than factored out for a
    /// builder of one caller. A static readonly rather than a const: the interpolated <see cref="TrendBucketSql.OriginSql"/>
    /// reference is not a compile-time constant. $1 server_id, $2/$3 window (naive UTC), $4 database filter, $5
    /// the bucket width in minutes.</summary>
    public static readonly string ExecutionCountTrendSql = $"""
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
        ),
        rated AS
        (
            SELECT
                collection_time,
                CASE WHEN interval_seconds > 0 THEN total_executions END AS rated_executions,
                CASE WHEN interval_seconds > 0 THEN interval_seconds END AS rated_seconds,
                CASE WHEN interval_seconds > 0 THEN CAST(total_executions AS DOUBLE PRECISION) / interval_seconds END AS executions_per_second
            FROM raw
        )
        SELECT
            GREATEST(date_bin(CAST($5 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
            CAST(SUM(rated_executions) AS DOUBLE PRECISION) / SUM(rated_seconds) AS executions_per_second,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY 1
        ORDER BY 1
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
    /// <para>#4234: the RAW arm is now BUCKETED (<see cref="ReadBucketedDurationTrendAsync"/>) — the hourly
    /// arm needs no further bucketing, because a window routed to it is already bounded to one row per hour.
    /// </para>
    /// </summary>
    private async Task<QueryTrendSeries> ReadRoutedDurationTrendAsync(
        string rawSql, string hourlySql, string hourlyView, string dailyView, Func<RollupAvailability, bool> hourlyAvailable,
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, DateTime? nowUtc,
        CancellationToken cancellationToken)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = DurationTrendRouting.ResolveTier(
            startUtc, nowUtc ?? DateTime.UtcNow, hourlyAvailable(rollups), coverage.For(hourlyView, dailyView));

        List<QueryTrendPoint> points;
        var bucketMinutes = 0;
        DateTime? firstServedUtc;
        if (tier == RetentionTier.Raw)
        {
            (points, bucketMinutes, firstServedUtc) = await ReadBucketedDurationTrendAsync(
                rawSql, serverId, startUtc, endUtc, databaseNames,
                valueOrdinal: 1, executionsOrdinal: 2, firstCollectionTimeOrdinal: 4, collectionCountOrdinal: 6, cancellationToken);
        }
        else
        {
            /* #3653 A6: the tier is decided over the LEGACY pair's coverage above (the deeper of the two on any
               upgraded store); the hourly FROM-clause item is then RollupCoverage.StitchedRelationSql — either
               "collect.<legacy> AS f" unchanged (byte-equal to hourlySql, which stays the MCP reader's pinned
               constant a store without successors still runs), or the stitched form splicing in the successor
               past its floor. BuildHourlyTrendSql already parameterises its FROM target, so the stitch's aliased
               relation (bare "f", no schema prefix needed — the stitch already schema-qualifies both sides)
               slots in exactly where the legacy/successor name used to. */
            var hourlyFromClause = coverage.StitchedRelationSql(hourlyView, "f", startUtc, RollupCoverage.StitchTier.Hourly);
            points = await ReadDurationTrendAsync(
                string.Equals(hourlyFromClause, $"collect.{hourlyView} AS f", StringComparison.Ordinal)
                    ? hourlySql
                    : DurationTrendRouting.BuildHourlyTrendSql(hourlyFromClause, withDatabaseFilter: true),
                serverId, startUtc, endUtc, databaseNames, cancellationToken);
            firstServedUtc = points.Count > 0 ? points[0].CollectionTime : null;
        }
        /* A bucket start is not a collection the store held (#4234): "data begins" names the first served
           collection's own instant, the instant the MCP payload's effective_start names. */
        var (effectiveStart, truncated) = DurationTrendRouting.DescribeCoverage(firstServedUtc, startUtc);
        return new QueryTrendSeries(points, tier, effectiveStart, truncated, bucketMinutes);
    }

    /// <summary>
    /// #4310 site 3's own minimum window (ruling issuecomment-5836972848 item 5's pattern, restated per-site as
    /// <see cref="PerformanceMonitor.Darling.Service.Mcp.DarlingDataReader.QueryStoreTopMinWindow"/> already
    /// does): below this window the table's own gate round trips (a second connection, a transaction, the
    /// coverage/floor probes) cost more than they save, so the read stays raw regardless of coverage. Does NOT
    /// share <see cref="QueryStoreIntervalWide.GridWideMinWindow"/> (the grid's 12h) — this site's query shape
    /// measured differently. 48 hours (NULL-free 15-day seed at a field store's
    /// rate, end-to-end through <see cref="GetQueryStoreDurationTrendAsync"/>, gate-routed, 1 cold + 5 warm):
    /// at 24h the table lost (279 ms raw vs. 370 ms table); at 48h the table won (median of 5,
    /// raw 423.0 ms vs. table 361.4 ms); at 72h the table also won (raw 515.0 ms vs. table 380.5 ms). Raw vs.
    /// table point equality was exact at 48h (46 of 46 points) on the NULL-free seed. 48h is the lower of the
    /// two measured wins, so it is the threshold — the exact crossover between 24h and 48h was not bisected.
    /// </summary>
    public static readonly TimeSpan QueryStoreDurationTrendMinWindow = TimeSpan.FromHours(48);

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
    /// <para>#4310 site 3: below <see cref="QueryStoreDurationTrendMinWindow"/> the read stays exactly
    /// as it was (the rollup route above, or raw). At or above it, and only when the store's schema is V145 or
    /// later, <see cref="QueryStoreIntervalWide.ReadsTableAsync"/> gets ONE chance to route the RAW arm (never
    /// the rollup route, which stays untouched — this is #2736's own fallback path, the pattern site 3 mirrors
    /// off the grid's top read) through <see cref="QueryStoreDurationTrendTableSql"/> instead; any fault or a
    /// "no" leaves <paramref name="startUtc"/>/<paramref name="endUtc"/> on the raw read unchanged, exactly as
    /// before this table existed. <paramref name="literalEndUtc"/> is the gate's clause-4 input, mirroring the
    /// grid's own parameter: null for an open end (the WPF preset means "through now"), or a concrete instant
    /// for a custom range. The window check comes first, before the schema probe or the gate's own round
    /// trips, so a short window costs zero extra store round trips (review D4R H1's rule, restated here).</para>
    /// </summary>
    public async Task<QueryStoreTrendSeries> GetQueryStoreDurationTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        DateTime? literalEndUtc = null, CancellationToken cancellationToken = default)
    {
        var route = await QueryStoreTrendRouting.ResolveAsync(_dataSource, cancellationToken);
        List<QueryTrendPoint> points;
        if (route.UseRollup)
        {
            points = await ReadQueryStoreRollupTrendAsync(route, serverId, startUtc, endUtc, databaseNames, cancellationToken);
        }
        else
        {
            points = null!;
            if (endUtc - startUtc >= QueryStoreDurationTrendMinWindow)
            {
                var schemaVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
                if (schemaVersion is int version && version >= QueryStoreIntervalWideMinSchemaVersion)
                {
                    points = await TryReadQueryStoreDurationTrendFromTableAsync(
                        serverId, startUtc, endUtc, literalEndUtc, databaseNames, cancellationToken)!;
                }
            }

            points ??= await ReadDurationTrendAsync(QueryStoreDurationTrendSql, serverId, startUtc, endUtc, databaseNames, cancellationToken);
        }

        /* Coverage the MCP tool's way: effective_start from the first served point (the shared rule, so the
           chart and the payload name the same instant), the unserved head from the route's measured floor. */
        var (effectiveStart, _) = DurationTrendRouting.DescribeCoverage(points.Count > 0 ? points[0].CollectionTime : null, startUtc);
        return new QueryStoreTrendSeries(points, route, effectiveStart, QueryStoreTrendRouting.UnservedBefore(route, startUtc));
    }

    /// <summary>
    /// #4310 site 3's gate and table read, on ONE connection in ONE read-only REPEATABLE READ transaction —
    /// the SAME arrangement <see cref="TryGetQueryStoreTopQueriesFromTableAsync"/> uses for the grid, so the
    /// gate's decision and the read it authorizes see one snapshot. Returns null (never an empty list) when the
    /// gate says raw, so the caller can tell "read raw instead" from "the table legitimately has nothing"; any
    /// fault opening the connection, starting the transaction, running the gate, or reading the table also
    /// returns null (except cancellation, which propagates).
    /// </summary>
    private async Task<List<QueryTrendPoint>?> TryReadQueryStoreDurationTrendFromTableAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? literalEndUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
            await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken);

            await using (var readOnly = new Npgsql.NpgsqlCommand("SET TRANSACTION READ ONLY", connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds })
            {
                await readOnly.ExecuteNonQueryAsync(cancellationToken);
            }

            var (useTable, clampedStart) = await QueryStoreIntervalWide.ReadsTableAsync(
                connection, serverId, startUtc, endUtc, literalEndUtc, QueryStoreDurationTrendMinWindow,
                ViewerCommandDeadlines.CurrentInteractiveReadSeconds, logger: null, cancellationToken);
            if (!useTable)
            {
                return null;
            }

            var items = new List<QueryTrendPoint>();
            await using var command = new Npgsql.NpgsqlCommand(QueryStoreDurationTrendTableSql, connection, transaction) { CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds };
            command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = serverId });
            command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(clampedStart, DateTimeKind.Unspecified) });
            command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
            command.Parameters.Add(new Npgsql.NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
            command.Parameters.Add(DatabaseFilterParameter(databaseNames));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (reader.IsDBNull(1))
                {
                    continue;
                }

                items.Add(new QueryTrendPoint
                {
                    CollectionTime = reader.GetDateTime(0),
                    Value = Convert.ToDouble(reader.GetValue(1)),
                    ExecutionCount = reader.IsDBNull(2) ? 0 : (long)Convert.ToDouble(reader.GetValue(2)),
                });
            }

            return items;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            System.Diagnostics.Trace.TraceWarning($"#4310 Query Store duration trend table read fell back to raw: {ex.GetType().Name}: {ex.Message}");
            return null;
        }
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
    /// #4234: the RAW tier's bucketed reader for the three delta-based trends — the PR #4304 pattern
    /// <see cref="GetWaitStatsTrendsByTypesAsync"/> established, ported from "one series per wait type" to
    /// "one series per call". Computes the auto bucket width for the window against
    /// <see cref="TrendBudget.Chart"/>'s per-series point budget, runs <paramref name="sql"/> (a
    /// <see cref="DurationTrendRouting.BuildBucketedRawTrendSql"/> statement, or <see cref="ExecutionCountTrendSql"/>,
    /// which follows the same shape by hand), and buffers every row so it can decide ONE timestamp rule for the
    /// whole series: a bucket is stamped at its own <c>first_collection_time</c> — its one physical collection's
    /// raw time — only when EVERY bucket this call returned held exactly one collection (<paramref name="collectionCountOrdinal"/>
    /// == 1 throughout); a single merged bucket anywhere keeps <c>bucket_start</c> for every point, so the chart
    /// never mixes on-grid and off-grid timestamps in one series. A NULL value is a bucket with nothing rated
    /// (#3541 A12 at the bucket level) and is skipped, matching <see cref="ReadTrendPointsAsync"/>.
    /// </summary>
    private async Task<(List<QueryTrendPoint> Points, int BucketMinutes, DateTime? FirstServedUtc)> ReadBucketedDurationTrendAsync(
        string sql, int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames,
        int valueOrdinal, int? executionsOrdinal, int firstCollectionTimeOrdinal, int collectionCountOrdinal,
        CancellationToken cancellationToken)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double Value, long ExecutionCount)>();
        var everyBucketSingleton = true;

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        command.Parameters.Add(new Npgsql.NpgsqlParameter<int> { TypedValue = bucketMinutes });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            /* A NULL rate is a bucket with nothing rated in it (#3541 A12, at the bucket level since #4234's
               bucketing): every collection the bucket held was unrated (a restart, or the window's first
               pre-V128 collection with no LAG). Skipped, not read as 0 — a chart has nowhere to draw "unknown". */
            if (reader.IsDBNull(valueOrdinal))
            {
                continue;
            }

            if (reader.GetInt64(collectionCountOrdinal) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(firstCollectionTimeOrdinal),
                Convert.ToDouble(reader.GetValue(valueOrdinal)),
                executionsOrdinal is int executions && !reader.IsDBNull(executions)
                    ? (long)Convert.ToDouble(reader.GetValue(executions))
                    : 0));
        }

        var items = new List<QueryTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new QueryTrendPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                Value = row.Value,
                ExecutionCount = row.ExecutionCount,
            });
        }

        return (items, everyBucketSingleton ? 0 : bucketMinutes, rows.Count > 0 ? rows[0].FirstCollectionTime : (DateTime?)null);
    }

    /// <summary>
    /// Execution-count trend over the window (single value column; no ExecutionCount), routed like its
    /// duration sibling (#3653): raw <see cref="ExecutionCountTrendSql"/> (BUCKETED, #4234) for a window raw
    /// can serve; for an older window the SAME <see cref="QueryDurationTrendHourlySql"/> statement the duration
    /// trend runs, plotting its <c>executions_per_second</c> column — the two charts are one read's two columns
    /// drawn apart, and on the hourly tier they literally are.
    /// </summary>
    public async Task<QueryTrendSeries> GetExecutionCountTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null,
        DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var (rollups, coverage) = await GetRollupAvailabilityAsync(cancellationToken);
        var tier = DurationTrendRouting.ResolveTier(
            startUtc, nowUtc ?? DateTime.UtcNow, rollups.QueryGrainHourly,
            coverage.For(TimescaleSupport.QueryStatsHourlyView, TimescaleSupport.QueryStatsDailyView));

        List<QueryTrendPoint> points;
        var bucketMinutes = 0;
        DateTime? firstServedUtc;
        if (tier == RetentionTier.Raw)
        {
            (points, bucketMinutes, firstServedUtc) = await ReadBucketedDurationTrendAsync(
                ExecutionCountTrendSql, serverId, startUtc, endUtc, databaseNames,
                valueOrdinal: 1, executionsOrdinal: null, firstCollectionTimeOrdinal: 2, collectionCountOrdinal: 3, cancellationToken);
        }
        else
        {
            points = await ReadTrendPointsAsync(QueryDurationTrendHourlySql, serverId, startUtc, endUtc, databaseNames, valueOrdinal: 2, executionsOrdinal: null, cancellationToken);
            firstServedUtc = points.Count > 0 ? points[0].CollectionTime : null;
        }
        /* A bucket start is not a collection the store held (#4234): "data begins" names the first served
           collection's own instant, the instant the MCP payload's effective_start names. */
        var (effectiveStart, truncated) = DurationTrendRouting.DescribeCoverage(firstServedUtc, startUtc);
        return new QueryTrendSeries(points, tier, effectiveStart, truncated, bucketMinutes);
    }
}
