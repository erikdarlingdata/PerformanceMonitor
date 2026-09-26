/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>The Memory Overview summary snapshot — the latest memory_stats row, a mirror of Lite's
/// <c>MemoryStatsRow</c> (<c>LocalDataService.Memory.cs</c>). The eight MB metrics are
/// <c>numeric(18,2)</c> in the store and CAST to double precision in the read for the typed GetDouble
/// reader; the two state strings are text. Lite's computed <c>UsedPhysicalMemoryMb</c> /
/// <c>MemoryUtilizationPercent</c> helpers are dropped — the summary strip displays only these fields.</summary>
public sealed record MemoryStatsRow(
    DateTime CollectionTime,
    double TotalPhysicalMemoryMb,
    double AvailablePhysicalMemoryMb,
    double TotalPageFileMb,
    double AvailablePageFileMb,
    string SystemMemoryState,
    string SqlMemoryModel,
    double TargetServerMemoryMb,
    double TotalServerMemoryMb,
    double BufferPoolMb,
    double PlanCacheMb);

/// <summary>One point of the Memory Overview's memory-grant overlay line: total granted MB across all
/// pools at a collection (or, for a singleton bucket, a raw collection). Lite crammed this into its
/// mutable <c>MemoryTrendPoint.TotalGrantedMb</c> and left the other fields 0; the viewer's
/// <see cref="MemoryTrendPoint"/> is an immutable positional record with no grant slot, so the overlay
/// gets its own two-field record.
/// <para>#4349: bucketed like <see cref="MemoryClerkTrendPoint"/> — <c>CollectionTime</c> is a
/// <c>date_bin</c> bucket start, EXCEPT when every bucket the call returned holds exactly one physical
/// collection, in which case every point is stamped at its own raw collection time instead — see
/// <see cref="ViewerDataService.GetMemoryGrantTrendAsync"/>.</para>
/// </summary>
public sealed record MemoryGrantTrendPoint(
    DateTime CollectionTime,
    double TotalGrantedMb);

/// <summary>One point on a selected memory clerk's trend line: its memory footprint (MB) at a
/// collection. Mirror of Lite's <c>MemoryClerkTrendPoint</c> — the clerk type is the dictionary key of
/// the batched read, so it is not repeated on each point.
/// <para>#4234: a gauge, bucketed to <see cref="TrendBudget.Chart"/>'s point budget — <c>MemoryMb</c> is the
/// AVERAGE footprint across the bucket's collections. <c>CollectionTime</c> is a <c>date_bin</c> bucket start,
/// EXCEPT when every bucket the call returned holds exactly one physical collection, in which case every point
/// is stamped at its own raw collection time instead — see
/// <see cref="ViewerDataService.GetMemoryClerkTrendsByTypesAsync"/>.</para>
/// </summary>
public sealed record MemoryClerkTrendPoint(
    DateTime CollectionTime,
    double MemoryMb);

/// <summary>One aggregated Memory Grants chart point (per collection_time/bucket + resource pool): the three
/// sizing MB metrics, the workspace-memory ceiling (<see cref="TargetMemoryMb"/> = the semaphore's current
/// grant target, <see cref="MaxTargetMemoryMb"/> = its hard maximum — the granted-vs-ceiling headroom
/// signal the Dashboard's get_resource_semaphore surfaces), and the four activity counts. A mirror of Lite's
/// <c>MemoryGrantChartPoint</c> plus the ceiling columns. MB SUMs CAST to double precision; the count SUMs
/// CAST to bigint (Postgres <c>SUM(integer)</c> widens to bigint and <c>SUM(bigint)</c> to numeric — the CAST
/// keeps the typed GetInt64 reader happy either way, the same reason the perfmon trend CASTs its
/// aggregates).
/// <para>#4349: <c>CollectionTime</c> is a <c>date_bin</c> bucket start, EXCEPT when every bucket the call
/// returned holds exactly one physical collection, in which case every point is stamped at its own raw
/// collection time instead — see <see cref="ViewerDataService.GetMemoryGrantChartDataAsync"/>.</para>
/// </summary>
public sealed record MemoryGrantChartPoint(
    DateTime CollectionTime,
    int PoolId,
    double AvailableMemoryMb,
    double GrantedMemoryMb,
    double UsedMemoryMb,
    int GranteeCount,
    int WaiterCount,
    long TimeoutErrorCountDelta,
    long ForcedGrantCountDelta,
    double TargetMemoryMb,
    double MaxTargetMemoryMb);

/// <summary>One RING_BUFFER_RESOURCE_MONITOR sample for the Memory Pressure Events chart, a mirror of
/// Lite's <c>MemoryPressureEventRow</c>: the sample time plus SQL Server (process) and OS (system)
/// pressure indicators. The chart hour-buckets these and counts indicator&gt;=2 as pressure.</summary>
public sealed record MemoryPressureEventRow(
    DateTime SampleTime,
    string MemoryNotification,
    int MemoryIndicatorsProcess,
    int MemoryIndicatorsSystem);

/// <summary>
/// The Memory inner tab's reads (W1j viewer copy-parity), ported from Lite's
/// <c>LocalDataService.Memory.cs</c> / <c>LocalDataService.MemoryGrants.cs</c> to Postgres. The Overview's
/// memory trend (Lite's <c>GetMemoryTrendAsync</c>) is REUSED as-is from
/// <c>ViewerDataService.OverviewLanes.cs</c> (the W1d Overview lanes added it for the buffer-pool lane) —
/// only the reads that did not already exist live here: the latest-snapshot summary, the grant overlay,
/// the clerk picker's distinct + batched-trend reads, the grant sizing/activity chart data, and the
/// pressure-event samples. All run against the <c>v_memory_stats</c> / <c>v_memory_grant_stats</c> /
/// <c>v_memory_clerks</c> / <c>v_memory_pressure_events</c> passthrough views (the Darling-analysis
/// convention); the last two views are created by migration V7 (V4/V5 left them out). Window bounds bind
/// naive-UTC (Kind-Unspecified), like every other viewer read.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>#4234: TTL memoization for <see cref="GetDistinctMemoryClerkTypesAsync"/> — see
    /// <see cref="ViewerNameListCache"/>'s remarks.</summary>
    private readonly ViewerNameListCache _distinctMemoryClerkTypesCache = new();

    /// <summary>
    /// The Overview summary snapshot — Lite's <c>GetLatestMemoryStatsAsync</c> ported to Postgres: the
    /// most recent memory_stats row for the server (no window; ORDER BY collection_time DESC LIMIT 1).
    /// The eight MB metrics are CAST to double precision for the typed reader. $1 server_id.
    /// </summary>
    public const string LatestMemoryStatsSql = """
        SELECT
            collection_time,
            CAST(total_physical_memory_mb AS double precision) AS total_physical_memory_mb,
            CAST(available_physical_memory_mb AS double precision) AS available_physical_memory_mb,
            CAST(total_page_file_mb AS double precision) AS total_page_file_mb,
            CAST(available_page_file_mb AS double precision) AS available_page_file_mb,
            system_memory_state,
            sql_memory_model,
            CAST(target_server_memory_mb AS double precision) AS target_server_memory_mb,
            CAST(total_server_memory_mb AS double precision) AS total_server_memory_mb,
            CAST(buffer_pool_mb AS double precision) AS buffer_pool_mb,
            CAST(plan_cache_mb AS double precision) AS plan_cache_mb
        FROM v_memory_stats
        WHERE server_id = $1
        ORDER BY collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The Overview's memory-grant overlay — Lite's <c>GetMemoryGrantTrendAsync</c> ported to Postgres,
    /// then bucketed (#4349) exactly like <see cref="MemoryTrendSql"/> (<c>ViewerDataService.OverviewLanes.cs</c>):
    /// total granted MB across all pools per collection, SUM CAST to double precision, then AVERAGED per
    /// bucket (a gauge, not an accumulating counter — no #3540 rated split applies). Lite selected four
    /// dummy 0-columns to reuse its <c>MemoryTrendPoint</c> shape; the viewer reads only the two fields it
    /// plots. $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket width minutes.
    /// <para>#3548/#4349: the <c>per_collection</c> CTE body is <see cref="TrendBucketSql.MemoryGrantPerCollectionSql"/>,
    /// byte-for-byte the same text the MCP tool's unbucketed <c>DarlingTrendReader.MemoryGrantTrendSql</c>
    /// reads — the one shared per-collection read #3548 requires. This outer bucketing select is the
    /// viewer's OWN wrapper on top of it; MCP wraps the same shared text in its own separate
    /// <c>date_bin</c> (<c>MemoryGrantTrendBucketedSql</c>) rather than this one, so neither SKU buckets
    /// twice.</para>
    /// </summary>
    public const string MemoryGrantTrendSql = $$"""
        WITH per_collection AS (
            {{TrendBucketSql.MemoryGrantPerCollectionSql}}
        )
        SELECT
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            AVG(total_granted_mb) AS total_granted_mb,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM per_collection
        GROUP BY 1
        ORDER BY 1
        """;

    /// <summary>
    /// The clerk picker's population read — Lite's <c>GetDistinctMemoryClerkTypesAsync</c> ported to
    /// Postgres verbatim: every clerk_type collected for the server over the window, ranked by total
    /// memory descending so the picker + "Top Clerks" fill see the heaviest clerks first. $1 server_id,
    /// $2 window start, $3 window end (naive UTC).
    /// <para>#4234: memoized through <see cref="_distinctMemoryClerkTypesCache"/> — keyed on (server, window
    /// length) for <see cref="ViewerNameListCache.Ttl"/>, so the full-window DISTINCT behind this runs at most
    /// once per 15 minutes rather than on every 1-minute auto-refresh, exactly like
    /// <c>GetDistinctWaitTypesAsync</c>.</para>
    /// </summary>
    public const string DistinctMemoryClerkTypesSql = """
        SELECT
            clerk_type
        FROM v_memory_clerks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY clerk_type
        ORDER BY SUM(memory_mb) DESC
        """;

    /// <summary>
    /// Lite's batched <c>GetMemoryClerkTrendsByTypesAsync</c> body, ported to Postgres, then bucketed (#4234):
    /// the trend for ALL selected clerk types in ONE query (replacing an N+1 loop), grouped by clerk type and
    /// <c>date_bin</c> bucket. The <c>clerk_type IN (...)</c> list is built dynamically from <c>$4</c> onward
    /// (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>, the bucket width takes the trailing parameter
    /// after the list, exactly like <c>WaitTrendsSql</c>), exactly like Lite's numbering. memory_mb is
    /// <c>numeric(18,2)</c> in the store, CAST to double precision for the typed reader. A caller passing
    /// <paramref name="typeCount"/> = 0 is a bug the <see cref="GetMemoryClerkTrendsByTypesAsync"/> guard
    /// prevents.
    /// <para>#4234: a gauge — <c>memory_mb</c> is the AVERAGE footprint across the bucket's collections, not a
    /// summed-delta rate (a clerk's memory_mb is a point-in-time reading, not an accumulating counter).
    /// <c>first_collection_time</c> (<c>MIN(collection_time)</c>) and <c>collection_count</c> (<c>COUNT(*)</c>)
    /// ride along per bucket, exactly like <c>WaitTrendsSql</c>, so the caller can tell a true singleton bucket
    /// from one the bucketing merged.</para>
    /// $4.. clerk types, last $ the bucket width in minutes.
    /// </summary>
    public static string MemoryClerkTrendsSql(int typeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, typeCount).Select(i => "$" + (i + 4)));
        var widthParam = "$" + (typeCount + 4);
        return $$"""
            SELECT
                clerk_type,
                GREATEST(date_bin(CAST({{widthParam}} AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
                CAST(AVG(memory_mb) AS double precision) AS memory_mb,
                MIN(collection_time) AS first_collection_time,
                COUNT(*) AS collection_count
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   clerk_type IN ({{typeParams}})
            GROUP BY clerk_type, 2
            ORDER BY clerk_type, 2
            """;
    }

    /// <summary>
    /// The Memory Grants chart data — Lite's <c>GetMemoryGrantChartDataAsync</c> ported to Postgres, then
    /// bucketed (#4349): per collection_time + pool_id, the summed sizing MB (available/granted/used) and
    /// activity counts (grantees/waiters/timeouts/forced) — the inner <c>per_collection</c> CTE is the
    /// UNCHANGED pre-bucket aggregation (multiple resource-semaphore rows per pool summed to one row per
    /// collection + pool). The outer bucket then AVERAGES every gauge (sizing MB, grantee/waiter counts —
    /// point-in-time readings, not accumulating counters) and SUMS the two true deltas
    /// (<c>timeout_error_count_delta</c> / <c>forced_grant_count_delta</c>) over a <c>rated</c> CTE that
    /// nulls them out (not filters the row) when <c>sample_interval_seconds</c> is 0 (#3540) — the same
    /// null-not-drop treatment <see cref="TempDbFileIoTrendSql"/> uses, so a bucket's sizing gauges are
    /// never lost just because its one collection's delta was unrated. <c>grantee_count</c> /
    /// <c>waiter_count</c> round their averaged bigint back to bigint (<c>ROUND</c> before the CAST — an
    /// AVG of integers is fractional). $1 server_id, $2 window start, $3 window end (naive UTC), $4 bucket
    /// width minutes.
    /// </summary>
    public const string MemoryGrantChartDataSql = $$"""
        WITH per_collection AS (
            SELECT
                collection_time,
                pool_id,
                CAST(SUM(available_memory_mb) AS double precision) AS available_memory_mb,
                CAST(SUM(granted_memory_mb) AS double precision) AS granted_memory_mb,
                CAST(SUM(used_memory_mb) AS double precision) AS used_memory_mb,
                CAST(SUM(target_memory_mb) AS double precision) AS target_memory_mb,
                CAST(SUM(max_target_memory_mb) AS double precision) AS max_target_memory_mb,
                CAST(SUM(grantee_count) AS bigint) AS grantee_count,
                CAST(SUM(waiter_count) AS bigint) AS waiter_count,
                /* #3540/#4349/#4364: MAX over the collection's own resource-semaphore rows. interval_seconds
                   is the sanctioned NULLIF form the measurement-contract census requires of any alias named
                   interval_sec(onds) — 0 (a restart, the ONLY known-unrateable case) becomes NULL through it.
                   But NULLIF cannot by itself tell that known-zero NULL apart from a pre-V128 row's true NULL
                   (interval never recorded) once collapsed into one alias, so the rated CTE below tests the
                   RAW max_interval_seconds_raw instead — the same IS DISTINCT FROM 0 the pre-#4364 guard used —
                   to keep an unknown interval's delta (LockWaitTrendSql's NULL-falls-back idiom, not #3540's
                   drop rule) while still nulling a genuine restart's. */
                NULLIF(MAX(sample_interval_seconds), 0) AS interval_seconds,
                MAX(sample_interval_seconds) AS max_interval_seconds_raw,
                CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
                CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grant_count_delta
            FROM v_memory_grant_stats
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            GROUP BY collection_time, pool_id
        ),
        rated AS (
            SELECT
                collection_time,
                pool_id,
                available_memory_mb,
                granted_memory_mb,
                used_memory_mb,
                target_memory_mb,
                max_target_memory_mb,
                grantee_count,
                waiter_count,
                CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN timeout_error_count_delta END AS rated_timeout_error_count_delta,
                CASE WHEN max_interval_seconds_raw IS DISTINCT FROM 0 THEN forced_grant_count_delta END AS rated_forced_grant_count_delta
            FROM per_collection
        )
        SELECT
            pool_id,
            GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {{TrendBucketSql.OriginSql}}), $2) AS bucket_start,
            AVG(available_memory_mb) AS available_memory_mb,
            AVG(granted_memory_mb) AS granted_memory_mb,
            AVG(used_memory_mb) AS used_memory_mb,
            CAST(ROUND(AVG(grantee_count)) AS bigint) AS grantee_count,
            CAST(ROUND(AVG(waiter_count)) AS bigint) AS waiter_count,
            CAST(SUM(rated_timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
            CAST(SUM(rated_forced_grant_count_delta) AS bigint) AS forced_grant_count_delta,
            AVG(target_memory_mb) AS target_memory_mb,
            AVG(max_target_memory_mb) AS max_target_memory_mb,
            MIN(collection_time) AS first_collection_time,
            COUNT(*) AS collection_count
        FROM rated
        GROUP BY pool_id, 2
        ORDER BY pool_id, 2
        """;

    /// <summary>
    /// The Memory Pressure Events samples — Lite's <c>GetMemoryPressureEventsAsync</c> ported to Postgres:
    /// the RING_BUFFER_RESOURCE_MONITOR samples over the window, windowed on <c>sample_time</c> (the
    /// payload's own clock, not collection_time — the memory_pressure_events index keys on it). $1
    /// server_id, $2 window start, $3 window end (naive UTC). $4 is the <see cref="EventWindowFloor"/> for
    /// $2 — <c>v_memory_pressure_events</c> is a hypertable partitioned on <c>collection_time</c>, which this
    /// sample-time window alone gives the planner nothing to exclude a chunk on (#4229); the floor lets it
    /// skip every chunk older than the window, without being able to drop a row (a sample is collected at
    /// or after its own <c>sample_time</c>).
    /// </summary>
    public const string MemoryPressureEventsSql = """
        SELECT
            sample_time,
            memory_notification,
            memory_indicators_process,
            memory_indicators_system
        FROM v_memory_pressure_events
        WHERE server_id = $1
        AND   sample_time >= $2
        AND   sample_time <= $3
        AND   collection_time >= $4
        ORDER BY sample_time
        """;

    /// <summary>The most recent memory_stats snapshot for the Overview summary strip, or null when the
    /// server has no memory_stats rows yet.</summary>
    public async Task<MemoryStatsRow?> GetLatestMemoryStatsAsync(
        int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(LatestMemoryStatsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return null;
        }

        return new MemoryStatsRow(
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0 : reader.GetDouble(1),
            reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
            reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
            reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
            reader.IsDBNull(5) ? "" : reader.GetString(5),
            reader.IsDBNull(6) ? "" : reader.GetString(6),
            reader.IsDBNull(7) ? 0 : reader.GetDouble(7),
            reader.IsDBNull(8) ? 0 : reader.GetDouble(8),
            reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
            reader.IsDBNull(10) ? 0 : reader.GetDouble(10));
    }

    /// <summary>Total granted MB across all pools per collection over the window — the Overview memory
    /// chart's grant overlay line. Bucketed to <see cref="TrendBudget.Chart"/>'s point budget (#4349); a
    /// bucket holding exactly one physical collection is stamped at that collection's own raw time rather
    /// than the bucket grid when EVERY bucket this call returned is such a singleton (ruling item 3).</summary>
    public async Task<List<MemoryGrantTrendPoint>> GetMemoryGrantTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(MemoryGrantTrendSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, double TotalGrantedMb)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(3) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(2),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1)));
        }

        var items = new List<MemoryGrantTrendPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryGrantTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.TotalGrantedMb));
        }

        return items;
    }

    /// <summary>The distinct clerk types collected for one server in the window, ranked by total memory
    /// descending — feeds the clerk picker's population + "Top Clerks" fill.
    /// <para><paramref name="nowUtc"/> is the cache's clock seam — null uses the wall clock; a test passes an
    /// explicit time to fast-forward past the TTL without sleeping.</para>
    /// </summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var effectiveNow = nowUtc ?? DateTime.UtcNow;
        var windowLength = endUtc - startUtc;
        if (_distinctMemoryClerkTypesCache.TryGet(serverId, windowLength, endUtc, effectiveNow, out var cached))
        {
            return cached;
        }

        var items = new List<string>();

        await using var command = _dataSource.CreateCommand(DistinctMemoryClerkTypesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(reader.GetString(0));
        }

        _distinctMemoryClerkTypesCache.Set(serverId, windowLength, endUtc, items, effectiveNow);
        return items;
    }

    /// <summary>The memory trend for every selected clerk type in one query, grouped by clerk type.
    /// Empty selection returns an empty map without touching the store.
    /// <para>#4234: buckets to <see cref="TrendBudget.Chart"/>'s point budget PER SERIES (clerk type), exactly
    /// like <c>GetWaitStatsTrendsByTypesAsync</c> — see its remarks for the width and singleton-stamping rules,
    /// which this read shares verbatim (only the metric itself differs: an averaged gauge here, a summed-delta
    /// rate there).</para>
    /// </summary>
    public async Task<Dictionary<string, List<MemoryClerkTrendPoint>>> GetMemoryClerkTrendsByTypesAsync(
        int serverId, List<string> clerkTypes, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<MemoryClerkTrendPoint>>();
        if (clerkTypes.Count == 0)
        {
            return result;
        }

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(MemoryClerkTrendsSql(clerkTypes.Count));
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        foreach (var clerkType in clerkTypes)
        {
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = clerkType });
        }
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(string ClerkType, DateTime BucketStart, DateTime FirstCollectionTime, double MemoryMb)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(4) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetString(0),
                reader.GetDateTime(1),
                reader.GetDateTime(3),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        foreach (var row in rows)
        {
            if (!result.TryGetValue(row.ClerkType, out var list))
            {
                list = new List<MemoryClerkTrendPoint>();
                result[row.ClerkType] = list;
            }

            list.Add(new MemoryClerkTrendPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.MemoryMb));
        }

        return result;
    }

    /// <summary>The per-pool grant sizing + activity chart data over the window (the Memory Grants
    /// sub-tab's two charts). Bucketed to <see cref="TrendBudget.Chart"/>'s point budget PER POOL (#4349);
    /// a bucket holding exactly one physical collection is stamped at that collection's own raw time
    /// rather than the bucket grid when EVERY bucket this call returned (across every pool) is such a
    /// singleton (ruling item 3).</summary>
    public async Task<List<MemoryGrantChartPoint>> GetMemoryGrantChartDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(MemoryGrantChartDataSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(int PoolId, DateTime BucketStart, DateTime FirstCollectionTime, double Available, double Granted, double Used, int Grantee, int Waiter, long TimeoutDelta, long ForcedDelta, double Target, double MaxTarget)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(12) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                reader.GetDateTime(1),
                reader.GetDateTime(11),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2),
                reader.IsDBNull(3) ? 0 : reader.GetDouble(3),
                reader.IsDBNull(4) ? 0 : reader.GetDouble(4),
                reader.IsDBNull(5) ? 0 : (int)reader.GetInt64(5),
                reader.IsDBNull(6) ? 0 : (int)reader.GetInt64(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt64(8),
                reader.IsDBNull(9) ? 0 : reader.GetDouble(9),
                reader.IsDBNull(10) ? 0 : reader.GetDouble(10)));
        }

        var items = new List<MemoryGrantChartPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new MemoryGrantChartPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.PoolId,
                row.Available,
                row.Granted,
                row.Used,
                row.Grantee,
                row.Waiter,
                row.TimeoutDelta,
                row.ForcedDelta,
                row.Target,
                row.MaxTarget));
        }

        return items;
    }

    /// <summary>The RING_BUFFER_RESOURCE_MONITOR pressure samples over the window (the Memory Pressure
    /// Events sub-tab's chart), windowed on <c>sample_time</c>.</summary>
    public async Task<List<MemoryPressureEventRow>> GetMemoryPressureEventsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryPressureEventRow>();

        await using var command = _dataSource.CreateCommand(MemoryPressureEventsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = EventWindowFloor.For(startUtc) });

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryPressureEventRow(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? "" : reader.GetString(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3)));
        }

        return items;
    }
}
