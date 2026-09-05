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
using Npgsql;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Per-item execution HISTORY reads backing the double-click drill-down windows (Query Stats / Procedure /
/// Query Store): the full per-collection-snapshot row set for one query (by query_hash), procedure (by
/// db+schema+object), or Query Store query (by query_id, across every plan) over the window. This is the
/// FULLER companion to <see cref="ItemTimelinePoint"/> (ViewerDataService.ItemTimeline.cs, #1409): that read
/// returns a five-metric per-interval curve for the slicer overlay; the history windows need every collected
/// column (the deltas, the min/max spreads, the memory-grant / spill / thread columns, the plan identity), so
/// these reads carry Lite's whole <c>Get{QueryStats,ProcedureStats,QueryStore}HistoryAsync</c> column list
/// (LocalDataService.QueryStats.cs / .QueryStore.cs) ported DuckDB → Postgres. Every column was verified
/// present in the shared collector schema (QueryStatsCollector / ProcedureStatsCollector / query_store_stats).
///
/// <para>
/// Convention deviations from Lite (matching every other viewer read): (1) reads are both-sides window-bound
/// on naive-UTC <c>collection_time</c> ($ positional params, Kind=Unspecified); (2) <c>collection_time</c> is
/// naive UTC and renders through <see cref="ViewerTimeHelper.ForDisplay"/>, while the DMV wall-clock stamps
/// (creation_time / last_execution_time / cached_time / first_execution_time) are the SQL server's own local
/// time and render raw through <see cref="FormatServerClock"/>; (3) Darling's <c>delta_*</c> columns are
/// already per-collection-cycle deltas, so — like the overlay — there is no C# row-over-row differencing
/// (Lite's history rows carry the same collector-computed deltas). Numeric columns read through
/// <c>Convert.ToXxx(GetValue)</c> so a bigint / integer / numeric provider type all bind cleanly.
/// </para>
/// </summary>
public sealed partial class ViewerDataService
{
    // ══════════════════════════════ Query Stats history ══════════════════════════════

    /// <summary>
    /// Every collected query_stats snapshot for one (database, query_hash) over the window — Lite's
    /// <c>GetQueryStatsHistoryAsync</c> column list against the base <c>query_stats</c> table (Lite reads its
    /// <c>v_query_stats</c> view; Darling's collector writes the deltas directly, so the base table already
    /// carries the per-cycle <c>delta_*</c> the view computes). $1 server_id, $2 database_name, $3 query_hash,
    /// $4 window start, $5 window end (naive UTC).
    /// </summary>
    public const string QueryStatsHistorySql = """
        SELECT
            collection_time,
            delta_execution_count,
            delta_worker_time,
            delta_elapsed_time,
            delta_logical_reads,
            delta_logical_writes,
            delta_physical_reads,
            delta_rows,
            delta_spills,
            min_dop,
            max_dop,
            min_worker_time,
            max_worker_time,
            min_elapsed_time,
            max_elapsed_time,
            query_plan_hash,
            min_grant_kb,
            max_grant_kb,
            min_used_grant_kb,
            max_used_grant_kb,
            min_ideal_grant_kb,
            max_ideal_grant_kb,
            min_reserved_threads,
            max_reserved_threads,
            min_used_threads,
            max_used_threads,
            min_physical_reads,
            max_physical_reads,
            min_rows,
            max_rows,
            min_spills,
            max_spills,
            total_clr_time,
            creation_time,
            last_execution_time,
            execution_count,
            total_worker_time,
            total_elapsed_time,
            total_logical_reads,
            total_logical_writes,
            total_physical_reads,
            total_rows,
            total_spills,
            sql_handle,
            plan_handle,
            query_hash,
            sample_interval_seconds
        FROM query_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_hash = $3
        AND   collection_time >= $4
        AND   collection_time <= $5
        ORDER BY collection_time
        """;

    /// <summary>The selected Top-Queries row's full per-collection history over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], ordered by collection time (the drill-down window's grid + metric chart).</summary>
    public async Task<List<ViewerQueryStatsHistoryRow>> GetQueryStatsHistoryAsync(
        int serverId, string databaseName, string queryHash, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerQueryStatsHistoryRow>();

        await using var command = _dataSource.CreateCommand(QueryStatsHistorySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddItemWindowParameters(command, serverId, databaseName, queryHash, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQueryStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = ReadLong(reader, 1),
                DeltaCpuUs = ReadLong(reader, 2),
                DeltaElapsedUs = ReadLong(reader, 3),
                DeltaLogicalReads = ReadLong(reader, 4),
                DeltaLogicalWrites = ReadLong(reader, 5),
                DeltaPhysicalReads = ReadLong(reader, 6),
                DeltaRows = ReadLong(reader, 7),
                DeltaSpills = ReadLong(reader, 8),
                MinDop = ReadInt(reader, 9),
                MaxDop = ReadInt(reader, 10),
                MinCpuUs = ReadLong(reader, 11),
                MaxCpuUs = ReadLong(reader, 12),
                MinElapsedUs = ReadLong(reader, 13),
                MaxElapsedUs = ReadLong(reader, 14),
                QueryPlanHash = reader.IsDBNull(15) ? "" : reader.GetString(15),
                MinGrantKb = ReadLong(reader, 16),
                MaxGrantKb = ReadLong(reader, 17),
                MinUsedGrantKb = ReadLong(reader, 18),
                MaxUsedGrantKb = ReadLong(reader, 19),
                MinIdealGrantKb = ReadLong(reader, 20),
                MaxIdealGrantKb = ReadLong(reader, 21),
                MinReservedThreads = ReadLong(reader, 22),
                MaxReservedThreads = ReadLong(reader, 23),
                MinUsedThreads = ReadLong(reader, 24),
                MaxUsedThreads = ReadLong(reader, 25),
                MinPhysicalReads = ReadLong(reader, 26),
                MaxPhysicalReads = ReadLong(reader, 27),
                MinRows = ReadLong(reader, 28),
                MaxRows = ReadLong(reader, 29),
                MinSpills = ReadLong(reader, 30),
                MaxSpills = ReadLong(reader, 31),
                TotalClrTimeUs = ReadLong(reader, 32),
                CreationTime = reader.IsDBNull(33) ? null : reader.GetDateTime(33),
                LastExecutionTime = reader.IsDBNull(34) ? null : reader.GetDateTime(34),
                TotalExecutions = ReadLong(reader, 35),
                TotalCpuUs = ReadLong(reader, 36),
                TotalElapsedUs = ReadLong(reader, 37),
                TotalLogicalReads = ReadLong(reader, 38),
                TotalLogicalWrites = ReadLong(reader, 39),
                TotalPhysicalReads = ReadLong(reader, 40),
                TotalRows = ReadLong(reader, 41),
                TotalSpills = ReadLong(reader, 42),
                SqlHandle = reader.IsDBNull(43) ? "" : reader.GetString(43),
                PlanHandle = reader.IsDBNull(44) ? "" : reader.GetString(44),
                QueryHash = reader.IsDBNull(45) ? "" : reader.GetString(45),
                SampleIntervalSeconds = reader.IsDBNull(46) ? null : ReadInt(reader, 46),
            });
        }

        return rows;
    }

    // ══════════════════════════════ Procedure stats history ══════════════════════════════

    /// <summary>
    /// Every collected procedure_stats snapshot for one (database, schema, object) over the window — Lite's
    /// <c>GetProcedureStatsHistoryAsync</c> column list against the base <c>procedure_stats</c> table.
    /// procedure_stats has no <c>sample_interval_seconds</c> column (unlike query_stats), so — exactly as Lite
    /// does — the interval is derived per row from the gap to the previous collection via
    /// <c>LAG(collection_time)</c> (identical syntax in Postgres). $1 server_id, $2 database_name, $3
    /// schema_name, $4 object_name, $5 window start, $6 window end (naive UTC).
    /// </summary>
    public const string ProcStatsHistorySql = """
        SELECT
            collection_time,
            delta_execution_count,
            delta_worker_time,
            delta_elapsed_time,
            delta_logical_reads,
            delta_logical_writes,
            delta_physical_reads,
            min_worker_time,
            max_worker_time,
            min_elapsed_time,
            max_elapsed_time,
            total_spills,
            min_logical_reads,
            max_logical_reads,
            min_physical_reads,
            max_physical_reads,
            min_logical_writes,
            max_logical_writes,
            min_spills,
            max_spills,
            sql_handle,
            plan_handle,
            cached_time,
            last_execution_time,
            object_type,
            execution_count,
            total_worker_time,
            total_elapsed_time,
            total_logical_reads,
            total_physical_reads,
            total_logical_writes,
            delta_spills,
            CAST(extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (ORDER BY collection_time)))) AS bigint) AS sample_interval_seconds
        FROM procedure_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   schema_name = $3
        AND   object_name = $4
        AND   collection_time >= $5
        AND   collection_time <= $6
        ORDER BY collection_time
        """;

    /// <summary>The selected Top-Procedures row's full per-collection history over [<paramref name="startUtc"/>,
    /// <paramref name="endUtc"/>], ordered by collection time.</summary>
    public async Task<List<ViewerProcedureStatsHistoryRow>> GetProcedureStatsHistoryAsync(
        int serverId, string databaseName, string schemaName, string objectName, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerProcedureStatsHistoryRow>();

        await using var command = _dataSource.CreateCommand(ProcStatsHistorySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = schemaName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = objectName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerProcedureStatsHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeltaExecutions = ReadLong(reader, 1),
                DeltaCpuUs = ReadLong(reader, 2),
                DeltaElapsedUs = ReadLong(reader, 3),
                DeltaLogicalReads = ReadLong(reader, 4),
                DeltaLogicalWrites = ReadLong(reader, 5),
                DeltaPhysicalReads = ReadLong(reader, 6),
                MinWorkerTimeUs = ReadLong(reader, 7),
                MaxWorkerTimeUs = ReadLong(reader, 8),
                MinElapsedTimeUs = ReadLong(reader, 9),
                MaxElapsedTimeUs = ReadLong(reader, 10),
                TotalSpills = ReadLong(reader, 11),
                MinLogicalReads = ReadLong(reader, 12),
                MaxLogicalReads = ReadLong(reader, 13),
                MinPhysicalReads = ReadLong(reader, 14),
                MaxPhysicalReads = ReadLong(reader, 15),
                MinLogicalWrites = ReadLong(reader, 16),
                MaxLogicalWrites = ReadLong(reader, 17),
                MinSpills = ReadLong(reader, 18),
                MaxSpills = ReadLong(reader, 19),
                SqlHandle = reader.IsDBNull(20) ? "" : reader.GetString(20),
                PlanHandle = reader.IsDBNull(21) ? "" : reader.GetString(21),
                CachedTime = reader.IsDBNull(22) ? null : reader.GetDateTime(22),
                LastExecutionTime = reader.IsDBNull(23) ? null : reader.GetDateTime(23),
                ObjectType = reader.IsDBNull(24) ? "" : reader.GetString(24),
                TotalExecutions = ReadLong(reader, 25),
                TotalCpuUs = ReadLong(reader, 26),
                TotalElapsedUs = ReadLong(reader, 27),
                TotalLogicalReads = ReadLong(reader, 28),
                TotalPhysicalReads = ReadLong(reader, 29),
                TotalLogicalWrites = ReadLong(reader, 30),
                DeltaSpills = ReadLong(reader, 31),
                SampleIntervalSeconds = reader.IsDBNull(32) ? null : ReadInt(reader, 32),
            });
        }

        return rows;
    }

    // ══════════════════════════════ Query Store history ══════════════════════════════

    /// <summary>
    /// Every collected query_store_stats snapshot for one (database, query_id) over the window — Lite's
    /// <c>GetQueryStoreHistoryAsync</c> column list against the base <c>query_store_stats</c> table. Query-scoped
    /// (NOT plan-filtered): the window returns every plan the query ran under so plan switches / regressions
    /// draw as separate chart series, exactly like Lite. Duration / CPU / CLR convert us → ms and memory pages
    /// → MB in SQL (mirroring the DuckDB casts). $1 server_id, $2 database_name, $3 query_id, $4 window start,
    /// $5 window end (naive UTC).
    ///
    /// <para>Deliberately NOT deduped per interval (#1841), unlike every Query Store AGGREGATE read: this is
    /// the "show me every snapshot" surface — a raw per-collection projection with no SUM, AVG or COUNT, so
    /// nothing is multiply-counted. What it does show is the cumulative restatement itself (one interval
    /// re-collected N times appears as N rows with a growing execution_count), and first_execution_time is
    /// already projected so a reader can tell those rows apart. Collapsing them would change what this
    /// drilldown displays, which is a product call rather than an arithmetic fix — left to #1841 tier 2 along
    /// with storing the real interval identity. Mirrors Lite's GetQueryStoreHistoryAsync.</para>
    /// </summary>
    public const string QueryStoreHistorySql = """
        SELECT
            collection_time,
            execution_count,
            CAST(avg_duration_us AS double precision) / 1000.0 AS avg_duration_ms,
            CAST(avg_cpu_time_us AS double precision) / 1000.0 AS avg_cpu_time_ms,
            CAST(avg_logical_io_reads AS double precision) AS avg_logical_reads,
            CAST(avg_logical_io_writes AS double precision) AS avg_logical_writes,
            CAST(avg_physical_io_reads AS double precision) AS avg_physical_reads,
            CAST(avg_rowcount AS double precision) AS avg_rowcount,
            last_execution_time,
            min_dop,
            max_dop,
            execution_type_desc,
            first_execution_time,
            module_name,
            CAST(avg_num_physical_io_reads AS double precision) AS avg_num_physical_reads,
            CAST(min_num_physical_io_reads AS double precision) AS min_num_physical_reads,
            CAST(max_num_physical_io_reads AS double precision) AS max_num_physical_reads,
            CAST(avg_clr_time_us AS double precision) / 1000.0 AS avg_clr_time_ms,
            CAST(min_clr_time_us AS double precision) / 1000.0 AS min_clr_time_ms,
            CAST(max_clr_time_us AS double precision) / 1000.0 AS max_clr_time_ms,
            CAST(avg_log_bytes_used AS double precision) / 1048576.0 AS avg_log_mb,
            CAST(min_log_bytes_used AS double precision) / 1048576.0 AS min_log_mb,
            CAST(max_log_bytes_used AS double precision) / 1048576.0 AS max_log_mb,
            plan_id,
            CAST(min_duration_us AS double precision) / 1000.0 AS min_duration_ms,
            CAST(max_duration_us AS double precision) / 1000.0 AS max_duration_ms,
            CAST(min_cpu_time_us AS double precision) / 1000.0 AS min_cpu_time_ms,
            CAST(max_cpu_time_us AS double precision) / 1000.0 AS max_cpu_time_ms,
            CAST(min_logical_io_reads AS double precision) AS min_logical_reads,
            CAST(max_logical_io_reads AS double precision) AS max_logical_reads,
            CAST(min_logical_io_writes AS double precision) AS min_logical_writes,
            CAST(max_logical_io_writes AS double precision) AS max_logical_writes,
            CAST(min_physical_io_reads AS double precision) AS min_physical_reads,
            CAST(max_physical_io_reads AS double precision) AS max_physical_reads,
            CAST(min_rowcount AS double precision) AS min_rowcount,
            CAST(max_rowcount AS double precision) AS max_rowcount,
            CAST(avg_query_max_used_memory AS double precision) * 8.0 / 1024.0 AS avg_memory_mb,
            CAST(min_query_max_used_memory AS double precision) * 8.0 / 1024.0 AS min_memory_mb,
            CAST(max_query_max_used_memory AS double precision) * 8.0 / 1024.0 AS max_memory_mb,
            CAST(avg_tempdb_space_used AS double precision) * 8.0 / 1024.0 AS avg_tempdb_mb,
            CAST(min_tempdb_space_used AS double precision) * 8.0 / 1024.0 AS min_tempdb_mb,
            CAST(max_tempdb_space_used AS double precision) * 8.0 / 1024.0 AS max_tempdb_mb,
            plan_type,
            is_forced_plan,
            force_failure_count,
            last_force_failure_reason,
            plan_forcing_type,
            compatibility_level,
            query_hash,
            query_plan_hash
        FROM query_store_stats
        WHERE server_id = $1
        AND   database_name = $2
        AND   query_id = $3
        AND   collection_time >= $4
        AND   collection_time <= $5
        ORDER BY collection_time
        """;

    /// <summary>The selected Query Store row's full per-collection history for its query_id (every plan) over
    /// [<paramref name="startUtc"/>, <paramref name="endUtc"/>], ordered by collection time.</summary>
    public async Task<List<ViewerQueryStoreHistoryRow>> GetQueryStoreHistoryAsync(
        int serverId, string databaseName, long queryId, DateTime startUtc, DateTime endUtc,
        CancellationToken cancellationToken = default)
    {
        var rows = new List<ViewerQueryStoreHistoryRow>();

        await using var command = _dataSource.CreateCommand(QueryStoreHistorySql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = databaseName ?? "" });
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = queryId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQueryStoreHistoryRow
            {
                CollectionTime = reader.GetDateTime(0),
                ExecutionCount = ReadLong(reader, 1),
                AvgDurationMs = ReadDouble(reader, 2),
                AvgCpuTimeMs = ReadDouble(reader, 3),
                AvgLogicalReads = ReadDouble(reader, 4),
                AvgLogicalWrites = ReadDouble(reader, 5),
                AvgPhysicalReads = ReadDouble(reader, 6),
                AvgRowcount = ReadDouble(reader, 7),
                LastExecutionTime = reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                MinDop = ReadLong(reader, 9),
                MaxDop = ReadLong(reader, 10),
                ExecutionTypeDesc = reader.IsDBNull(11) ? "" : reader.GetString(11),
                FirstExecutionTime = reader.IsDBNull(12) ? null : reader.GetDateTime(12),
                ModuleName = reader.IsDBNull(13) ? "" : reader.GetString(13),
                AvgNumPhysicalReads = ReadDouble(reader, 14),
                MinNumPhysicalReads = ReadDouble(reader, 15),
                MaxNumPhysicalReads = ReadDouble(reader, 16),
                AvgClrTimeMs = ReadDouble(reader, 17),
                MinClrTimeMs = ReadDouble(reader, 18),
                MaxClrTimeMs = ReadDouble(reader, 19),
                AvgLogMb = ReadDouble(reader, 20),
                MinLogMb = ReadDouble(reader, 21),
                MaxLogMb = ReadDouble(reader, 22),
                PlanId = ReadLong(reader, 23),
                MinDurationMs = ReadDouble(reader, 24),
                MaxDurationMs = ReadDouble(reader, 25),
                MinCpuTimeMs = ReadDouble(reader, 26),
                MaxCpuTimeMs = ReadDouble(reader, 27),
                MinLogicalReads = ReadDouble(reader, 28),
                MaxLogicalReads = ReadDouble(reader, 29),
                MinLogicalWrites = ReadDouble(reader, 30),
                MaxLogicalWrites = ReadDouble(reader, 31),
                MinPhysicalReads = ReadDouble(reader, 32),
                MaxPhysicalReads = ReadDouble(reader, 33),
                MinRowcount = ReadDouble(reader, 34),
                MaxRowcount = ReadDouble(reader, 35),
                AvgMemoryMb = ReadDouble(reader, 36),
                MinMemoryMb = ReadDouble(reader, 37),
                MaxMemoryMb = ReadDouble(reader, 38),
                AvgTempdbMb = ReadDouble(reader, 39),
                MinTempdbMb = ReadDouble(reader, 40),
                MaxTempdbMb = ReadDouble(reader, 41),
                PlanType = reader.IsDBNull(42) ? "" : reader.GetString(42),
                IsForcedPlan = !reader.IsDBNull(43) && reader.GetBoolean(43),
                ForceFailureCount = ReadLong(reader, 44),
                LastForceFailureReason = reader.IsDBNull(45) ? "" : reader.GetString(45),
                PlanForcingType = reader.IsDBNull(46) ? "" : reader.GetString(46),
                CompatibilityLevel = ReadInt(reader, 47),
                QueryHash = reader.IsDBNull(48) ? "" : reader.GetString(48),
                QueryPlanHash = reader.IsDBNull(49) ? "" : reader.GetString(49),
            });
        }

        return rows;
    }

    // ── Type-tolerant column readers (the history reads pull raw bigint / integer / numeric columns) ──

    private static long ReadLong(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? 0L : Convert.ToInt64(reader.GetValue(ordinal));

    private static int ReadInt(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? 0 : Convert.ToInt32(reader.GetValue(ordinal));

    private static double ReadDouble(NpgsqlDataReader reader, int ordinal)
        => reader.IsDBNull(ordinal) ? 0.0 : Convert.ToDouble(reader.GetValue(ordinal));
}
