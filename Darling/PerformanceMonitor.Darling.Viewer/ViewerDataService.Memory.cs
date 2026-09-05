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
/// pools at a collection. Lite crammed this into its mutable <c>MemoryTrendPoint.TotalGrantedMb</c> and
/// left the other fields 0; the viewer's <see cref="MemoryTrendPoint"/> is an immutable positional
/// record with no grant slot, so the overlay gets its own two-field record.</summary>
public sealed record MemoryGrantTrendPoint(
    DateTime CollectionTime,
    double TotalGrantedMb);

/// <summary>One point on a selected memory clerk's trend line: its memory footprint (MB) at a
/// collection. Mirror of Lite's <c>MemoryClerkTrendPoint</c> — the clerk type is the dictionary key of
/// the batched read, so it is not repeated on each point.</summary>
public sealed record MemoryClerkTrendPoint(
    DateTime CollectionTime,
    double MemoryMb);

/// <summary>One aggregated Memory Grants chart point (per collection_time + resource pool): the three
/// sizing MB metrics, the workspace-memory ceiling (<see cref="TargetMemoryMb"/> = the semaphore's current
/// grant target, <see cref="MaxTargetMemoryMb"/> = its hard maximum — the granted-vs-ceiling headroom
/// signal the Dashboard's get_resource_semaphore surfaces), and the four activity counts. A mirror of Lite's
/// <c>MemoryGrantChartPoint</c> plus the ceiling columns. MB SUMs CAST to double precision; the count SUMs
/// CAST to bigint (Postgres <c>SUM(integer)</c> widens to bigint and <c>SUM(bigint)</c> to numeric — the CAST
/// keeps the typed GetInt64 reader happy either way, the same reason the perfmon trend CASTs its
/// aggregates).</summary>
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
    /// The Overview's memory-grant overlay — Lite's <c>GetMemoryGrantTrendAsync</c> ported to Postgres:
    /// total granted MB across all pools per collection. SUM CAST to double precision. Lite selected four
    /// dummy 0-columns to reuse its <c>MemoryTrendPoint</c> shape; the viewer reads only the two fields it
    /// plots. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string MemoryGrantTrendSql = """
        SELECT
            collection_time,
            CAST(SUM(granted_memory_mb) AS double precision) AS total_granted_mb
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY collection_time
        ORDER BY collection_time
        """;

    /// <summary>
    /// The clerk picker's population read — Lite's <c>GetDistinctMemoryClerkTypesAsync</c> ported to
    /// Postgres verbatim: every clerk_type collected for the server over the window, ranked by total
    /// memory descending so the picker + "Top Clerks" fill see the heaviest clerks first. $1 server_id,
    /// $2 window start, $3 window end (naive UTC).
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
    /// Lite's batched <c>GetMemoryClerkTrendsByTypesAsync</c> body, ported to Postgres: the trend for ALL
    /// selected clerk types in ONE query (replacing an N+1 loop), grouped by clerk type. The
    /// <c>clerk_type IN (...)</c> list is built dynamically from <c>$4</c> onward
    /// (server_id/start/end take <c>$1</c>/<c>$2</c>/<c>$3</c>), exactly like Lite's numbering. memory_mb
    /// is <c>numeric(18,2)</c> in the store, CAST to double precision for the typed reader. A caller
    /// passing <paramref name="typeCount"/> = 0 is a bug the <see cref="GetMemoryClerkTrendsByTypesAsync"/>
    /// guard prevents.
    /// </summary>
    public static string MemoryClerkTrendsSql(int typeCount)
    {
        var typeParams = string.Join(", ", Enumerable.Range(0, typeCount).Select(i => "$" + (i + 4)));
        return $"""
            SELECT
                clerk_type,
                collection_time,
                CAST(memory_mb AS double precision) AS memory_mb
            FROM v_memory_clerks
            WHERE server_id = $1
            AND   collection_time >= $2
            AND   collection_time <= $3
            AND   clerk_type IN ({typeParams})
            ORDER BY clerk_type, collection_time
            """;
    }

    /// <summary>
    /// The Memory Grants chart data — Lite's <c>GetMemoryGrantChartDataAsync</c> ported to Postgres:
    /// per collection_time + pool_id, the summed sizing MB (available/granted/used) and activity counts
    /// (grantees/waiters/timeouts/forced). The MB SUMs CAST to double precision; the count SUMs CAST to
    /// bigint (see <see cref="MemoryGrantChartPoint"/>). $1 server_id, $2 window start, $3 window end
    /// (naive UTC).
    /// </summary>
    public const string MemoryGrantChartDataSql = """
        SELECT
            collection_time,
            pool_id,
            CAST(SUM(available_memory_mb) AS double precision) AS available_memory_mb,
            CAST(SUM(granted_memory_mb) AS double precision) AS granted_memory_mb,
            CAST(SUM(used_memory_mb) AS double precision) AS used_memory_mb,
            CAST(SUM(grantee_count) AS bigint) AS grantee_count,
            CAST(SUM(waiter_count) AS bigint) AS waiter_count,
            CAST(SUM(timeout_error_count_delta) AS bigint) AS timeout_error_count_delta,
            CAST(SUM(forced_grant_count_delta) AS bigint) AS forced_grant_count_delta,
            CAST(SUM(target_memory_mb) AS double precision) AS target_memory_mb,
            CAST(SUM(max_target_memory_mb) AS double precision) AS max_target_memory_mb
        FROM v_memory_grant_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        GROUP BY collection_time, pool_id
        ORDER BY collection_time, pool_id
        """;

    /// <summary>
    /// The Memory Pressure Events samples — Lite's <c>GetMemoryPressureEventsAsync</c> ported to Postgres:
    /// the RING_BUFFER_RESOURCE_MONITOR samples over the window, windowed on <c>sample_time</c> (the
    /// payload's own clock, not collection_time — the memory_pressure_events index keys on it). $1
    /// server_id, $2 window start, $3 window end (naive UTC).
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
    /// chart's grant overlay line.</summary>
    public async Task<List<MemoryGrantTrendPoint>> GetMemoryGrantTrendAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryGrantTrendPoint>();

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

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryGrantTrendPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetDouble(1)));
        }

        return items;
    }

    /// <summary>The distinct clerk types collected for one server in the window, ranked by total memory
    /// descending — feeds the clerk picker's population + "Top Clerks" fill.</summary>
    public async Task<List<string>> GetDistinctMemoryClerkTypesAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
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

        return items;
    }

    /// <summary>The memory trend for every selected clerk type in one query, grouped by clerk type.
    /// Empty selection returns an empty map without touching the store.</summary>
    public async Task<Dictionary<string, List<MemoryClerkTrendPoint>>> GetMemoryClerkTrendsByTypesAsync(
        int serverId, List<string> clerkTypes, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new Dictionary<string, List<MemoryClerkTrendPoint>>();
        if (clerkTypes.Count == 0)
        {
            return result;
        }

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

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var clerkType = reader.GetString(0);
            if (!result.TryGetValue(clerkType, out var list))
            {
                list = new List<MemoryClerkTrendPoint>();
                result[clerkType] = list;
            }

            list.Add(new MemoryClerkTrendPoint(
                reader.GetDateTime(1),
                reader.IsDBNull(2) ? 0 : reader.GetDouble(2)));
        }

        return result;
    }

    /// <summary>The per-pool grant sizing + activity chart data over the window (the Memory Grants
    /// sub-tab's two charts).</summary>
    public async Task<List<MemoryGrantChartPoint>> GetMemoryGrantChartDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<MemoryGrantChartPoint>();

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

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            items.Add(new MemoryGrantChartPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
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
