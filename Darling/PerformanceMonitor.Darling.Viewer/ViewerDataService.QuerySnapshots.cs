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
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// One Active-Queries snapshot row — the viewer copy of Lite's <c>QuerySnapshotRow</c>
/// (LocalDataService.Blocking.cs). Carries the full column set Lite's two grids bind: the Active Queries
/// grid's columns plus the #1286 memory-grant / tempdb / transaction triage columns Lite's WaitDrillDownWindow
/// adds (all verified present in the query_snapshots store). A captured
/// running request from one collection cycle. <see cref="CollectionTime"/> is the collector's naive-UTC
/// capture time, so <see cref="CollectionTimeLocal"/> converts through <see cref="ViewerTimeHelper.ForDisplay"/>
/// (Lite's <c>ServerTimeHelper.FormatServerTime</c>). <see cref="QueryPlan"/> / <see cref="LiveQueryPlan"/>
/// are the stored estimated / live plan XML the collector now captures inline; <see cref="HasQueryPlan"/> /
/// <see cref="HasLiveQueryPlan"/> gate the grid's Estimated / Actual plan buttons.
/// </summary>
public sealed class ViewerQuerySnapshotRow
{
    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string ElapsedTimeFormatted { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string Status { get; set; } = "";
    public int BlockingSessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public string WaitResource { get; set; } = "";
    public long CpuTimeMs { get; set; }
    public long TotalElapsedTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public long LogicalReads { get; set; }
    public double GrantedQueryMemoryGb { get; set; }
    public string TransactionIsolationLevel { get; set; } = "";
    public int Dop { get; set; }
    public int ParallelWorkerCount { get; set; }
    public DateTime CollectionTime { get; set; }
    public string? QueryPlan { get; set; }
    public string? LiveQueryPlan { get; set; }
    public string LoginName { get; set; } = "";
    public string HostName { get; set; } = "";
    public string ProgramName { get; set; } = "";
    public int OpenTransactionCount { get; set; }
    public decimal PercentComplete { get; set; }
    public string QueryHash { get; set; } = "";

    /* #1286 memory-grant / tempdb / transaction triage columns — collected into query_snapshots (verified
       present) and surfaced by Lite's WaitDrillDownWindow grid (not its leaner Active Queries grid). The
       Darling WaitDrillDownWindow mirrors that grid, so these bind there. */
    public double RequestedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public double MaxUsedMemoryMb { get; set; }
    public double TempdbCurrentMb { get; set; }
    public double TempdbAllocationsMb { get; set; }
    public double TranLogUsedMb { get; set; }
    public DateTime? TranStartTime { get; set; }
    public int RequestId { get; set; }

    /* Chain-view metadata (Wait Stats "Show Queries With This Wait" drill-down, LCK_M_* chain branch):
       set only when WaitDrillDownWindow walks blocking chains — the transitive blocked-session count and
       the "Head SPID X blocking N session(s)" path. Default 0 / empty for every other read. Mirrors Lite's
       QuerySnapshotRow.BlockedSessionCount / ChainBlockingPath. */
    public int BlockedSessionCount { get; set; }
    public string ChainBlockingPath { get; set; } = "";

    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
    public bool HasLiveQueryPlan => !string.IsNullOrEmpty(LiveQueryPlan);
    public string CollectionTimeLocal =>
        CollectionTime == DateTime.MinValue ? "" : ViewerTimeHelper.ForDisplay(CollectionTime).ToString("yyyy-MM-dd HH:mm:ss");

    /// <summary>The transaction begin time, empty when the request has no open transaction. Mirrors Lite's
    /// <c>QuerySnapshotRow.TranStartTimeLocal</c> (raw server-clock <c>FormatServerTime</c>):
    /// transaction_begin_time from sys.dm_tran_active_transactions is a SQL-server-local wall-clock time (NOT
    /// naive UTC), so it renders raw via <see cref="ViewerDataService.FormatServerClock"/> like the other
    /// dm_exec_* times (last_execution_time / cached_time), NOT through the UTC-converting ForDisplay.</summary>
    public string TranStartTimeLocal => ViewerDataService.FormatServerClock(TranStartTime);
}

public sealed partial class ViewerDataService
{
    /* The Active-Queries column list Lite's GetLatestQuerySnapshotsAsync selects (Blocking.cs:131),
       shared by the window read and the latest-batch read so both materialize the same row shape. */
    private const string QuerySnapshotColumns = """
            session_id,
            database_name,
            elapsed_time_formatted,
            query_text,
            status,
            blocking_session_id,
            wait_type,
            wait_time_ms,
            wait_resource,
            cpu_time_ms,
            total_elapsed_time_ms,
            reads,
            writes,
            logical_reads,
            granted_query_memory_gb,
            transaction_isolation_level,
            dop,
            parallel_worker_count,
            query_plan,
            live_query_plan,
            collection_time,
            login_name,
            host_name,
            program_name,
            open_transaction_count,
            percent_complete,
            query_hash,
            requested_memory_mb,
            used_memory_mb,
            max_used_memory_mb,
            tempdb_current_mb,
            tempdb_allocations_mb,
            tran_log_used_mb,
            tran_start_time,
            request_id
        """;

    /// <summary>
    /// The Active-Queries grid read — Lite's <c>GetLatestQuerySnapshotsAsync</c> (v_query_snapshots,
    /// ORDER BY collection_time DESC, cpu_time_ms DESC, WAITFOR shells dropped) against the base
    /// <c>query_snapshots</c> table. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public static readonly string LatestQuerySnapshotsSql = $"""
        SELECT
        {QuerySnapshotColumns}
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        AND   query_text NOT LIKE 'WAITFOR%'
        ORDER BY collection_time DESC, cpu_time_ms DESC
        """;

    /// <summary>
    /// The "Latest Snapshot" button read — the newest STORED capture only (all rows sharing
    /// MAX(collection_time) for the server). Erik's decision: the viewer reads only what the collector
    /// stored, so this replaces Lite's live-server query with the newest persisted snapshot batch —
    /// the closest stored analogue of Lite's point-in-time "currently running" view. $1 server_id.
    /// </summary>
    public static readonly string LatestQuerySnapshotBatchSql = $"""
        SELECT
        {QuerySnapshotColumns}
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = (SELECT MAX(collection_time) FROM query_snapshots WHERE server_id = $1)
        AND   ($2::text[] IS NULL OR database_name = ANY($2))
        AND   query_text NOT LIKE 'WAITFOR%'
        ORDER BY cpu_time_ms DESC
        """;

    /// <summary>Active-Queries grid rows over [<paramref name="startUtc"/>, <paramref name="endUtc"/>].</summary>
    public async Task<List<ViewerQuerySnapshotRow>> GetLatestQuerySnapshotsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(LatestQuerySnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        return await ReadQuerySnapshotsAsync(command, cancellationToken);
    }

    /// <summary>
    /// The "Show Queries With This Wait" drill-down read (Wait Stats chart right-click) — Lite's
    /// <c>GetQuerySnapshotsByWaitTypeAsync</c>: the captured running-query snapshots whose <c>wait_type</c>
    /// exactly matches, over the drill window. Same row shape / column list as the Active Queries grid.
    /// $1 server_id, $2 window start, $3 window end (naive UTC), $4 wait_type.
    /// </summary>
    public static readonly string QuerySnapshotsByWaitTypeSql = $"""
        SELECT
        {QuerySnapshotColumns}
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($5::text[] IS NULL OR database_name = ANY($5))
        AND   wait_type = $4
        AND   query_text NOT LIKE 'WAITFOR%'
        ORDER BY collection_time DESC, cpu_time_ms DESC
        """;

    /// <summary>Active-Queries snapshot rows filtered to one <paramref name="waitType"/> over the window.</summary>
    public async Task<List<ViewerQuerySnapshotRow>> GetQuerySnapshotsByWaitTypeAsync(
        int serverId, string waitType, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(QuerySnapshotsByWaitTypeSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = waitType });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        return await ReadQuerySnapshotsAsync(command, cancellationToken);
    }

    /// <summary>
    /// The newest stored snapshot batch (rows at MAX(collection_time)) plus that batch's time (null when
    /// no snapshots are stored) — backs the "Latest Snapshot" button. The batch time is taken from the
    /// returned rows (all share one collection_time).
    /// </summary>
    public async Task<(DateTime? BatchTime, List<ViewerQuerySnapshotRow> Rows)> GetLatestQuerySnapshotBatchAsync(
        int serverId, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(LatestQuerySnapshotBatchSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        var rows = await ReadQuerySnapshotsAsync(command, cancellationToken);
        DateTime? batchTime = rows.Count > 0 ? rows[0].CollectionTime : null;
        return (batchTime, rows);
    }

    private static async Task<List<ViewerQuerySnapshotRow>> ReadQuerySnapshotsAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerQuerySnapshotRow>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerQuerySnapshotRow
            {
                SessionId = reader.IsDBNull(0) ? 0 : reader.GetInt32(0),
                DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                ElapsedTimeFormatted = reader.IsDBNull(2) ? "" : reader.GetString(2),
                QueryText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                Status = reader.IsDBNull(4) ? "" : reader.GetString(4),
                BlockingSessionId = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                WaitType = reader.IsDBNull(6) ? "" : reader.GetString(6),
                WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                CpuTimeMs = reader.IsDBNull(9) ? 0 : reader.GetInt64(9),
                TotalElapsedTimeMs = reader.IsDBNull(10) ? 0 : reader.GetInt64(10),
                Reads = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                Writes = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                LogicalReads = reader.IsDBNull(13) ? 0 : reader.GetInt64(13),
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : Convert.ToDouble(reader.GetValue(14)),
                TransactionIsolationLevel = reader.IsDBNull(15) ? "" : reader.GetString(15),
                Dop = reader.IsDBNull(16) ? 0 : reader.GetInt32(16),
                ParallelWorkerCount = reader.IsDBNull(17) ? 0 : reader.GetInt32(17),
                QueryPlan = reader.IsDBNull(18) ? null : reader.GetString(18),
                LiveQueryPlan = reader.IsDBNull(19) ? null : reader.GetString(19),
                CollectionTime = reader.IsDBNull(20) ? DateTime.MinValue : reader.GetDateTime(20),
                LoginName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                HostName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                ProgramName = reader.IsDBNull(23) ? "" : reader.GetString(23),
                OpenTransactionCount = reader.IsDBNull(24) ? 0 : reader.GetInt32(24),
                PercentComplete = reader.IsDBNull(25) ? 0m : Convert.ToDecimal(reader.GetValue(25)),
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26),
                RequestedMemoryMb = reader.IsDBNull(27) ? 0 : Convert.ToDouble(reader.GetValue(27)),
                UsedMemoryMb = reader.IsDBNull(28) ? 0 : Convert.ToDouble(reader.GetValue(28)),
                MaxUsedMemoryMb = reader.IsDBNull(29) ? 0 : Convert.ToDouble(reader.GetValue(29)),
                TempdbCurrentMb = reader.IsDBNull(30) ? 0 : Convert.ToDouble(reader.GetValue(30)),
                TempdbAllocationsMb = reader.IsDBNull(31) ? 0 : Convert.ToDouble(reader.GetValue(31)),
                TranLogUsedMb = reader.IsDBNull(32) ? 0 : Convert.ToDouble(reader.GetValue(32)),
                TranStartTime = reader.IsDBNull(33) ? null : reader.GetDateTime(33),
                RequestId = reader.IsDBNull(34) ? 0 : reader.GetInt32(34),
            });
        }

        return rows;
    }

    /// <summary>
    /// Hourly Active-Queries buckets for the slicer — Lite's <c>GetActiveQuerySlicerDataAsync</c>
    /// (Blocking.cs:72) ported. Buckets by <c>date_trunc('hour', collection_time)</c>; the default
    /// metric is session count (Value = SessionCount), matching Lite's "Sessions" slicer label.
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string ActiveQuerySlicerSql = """
        SELECT
            date_trunc('hour', collection_time) AS bucket,
            COUNT(*) AS session_count,
            COALESCE(SUM(cpu_time_ms), 0) AS total_cpu,
            COALESCE(SUM(total_elapsed_time_ms), 0) AS total_elapsed,
            COALESCE(SUM(reads), 0) AS total_reads,
            COALESCE(SUM(logical_reads), 0) AS total_logical_reads,
            COALESCE(SUM(writes), 0) AS total_writes
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        GROUP BY date_trunc('hour', collection_time)
        ORDER BY bucket
        """;

    /// <summary>Hourly Active-Queries slicer buckets over the window (Value = session count).</summary>
    public async Task<List<TimeSliceBucket>> GetActiveQuerySlicerDataAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = new List<TimeSliceBucket>();

        await using var command = _dataSource.CreateCommand(ActiveQuerySlicerSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var sessions = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = sessions,
                TotalCpu = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : Convert.ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : Convert.ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : Convert.ToDouble(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : Convert.ToDouble(reader.GetValue(6)),
                Value = sessions,
            });
        }

        return items;
    }
}
