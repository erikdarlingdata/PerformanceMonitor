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
/// (Lite's <c>ServerTimeHelper.FormatServerTime</c>). <see cref="HasQueryPlan"/> / <see cref="HasLiveQueryPlan"/>
/// are independent flags — set directly from the store's presence check on a stored-row read, or from a
/// non-empty in-row plan on the live DMV read — and gate the grid's Estimated / Actual plan buttons.
/// <see cref="QueryPlan"/> / <see cref="LiveQueryPlan"/> carry plan XML only for the live path; a stored-row
/// read leaves them null and fetches on demand via
/// <see cref="ViewerDataService.GetQuerySnapshotPlanXmlAsync"/> when a plan button is clicked (#4239).
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

    public bool HasQueryPlan { get; set; }
    public bool HasLiveQueryPlan { get; set; }
    public string CollectionTimeLocal =>
        CollectionTime == DateTime.MinValue ? "" : ViewerTimeHelper.ForDisplay(CollectionTime).ToString("yyyy-MM-dd HH:mm:ss");

    /// <summary>The transaction begin time, empty when the request has no open transaction.
    /// transaction_begin_time from sys.dm_tran_active_transactions is a SQL-server-local wall-clock time (NOT
    /// naive UTC), so it converts via <see cref="ViewerDataService.FormatServerClock"/> like the other
    /// dm_exec_* times (last_execution_time / cached_time), NOT through the naive-UTC ForDisplay.
    ///
    /// <para>Lite's <c>QuerySnapshotRow.TranStartTimeLocal</c> is the mirror, through
    /// <c>ServerTimeHelper.FormatServerClock</c>. It is NOT <c>FormatServerTime</c>: that method takes
    /// naive UTC and adds the collected offset, so it is the wrong renderer for this frame in either
    /// SKU.</para></summary>
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
            query_plan IS NOT NULL AS has_query_plan,
            live_query_plan IS NOT NULL AS has_live_query_plan,
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

    /// <summary>The Active-Queries grid cap (#4239) — the newest this many rows of the matched window, so a
    /// wide window with a busy server can't pull an unbounded amount of plan-bearing data into the viewer.</summary>
    private const int MaxLatestQuerySnapshotRows = 1000;

    /// <summary>
    /// The Active-Queries grid read — Lite's <c>GetLatestQuerySnapshotsAsync</c> (v_query_snapshots,
    /// ORDER BY collection_time DESC, cpu_time_ms DESC, WAITFOR shells dropped) against the base
    /// <c>query_snapshots</c> table, capped to the newest <see cref="MaxLatestQuerySnapshotRows"/> (#4239) —
    /// the only one of the three snapshot reads that caps; <c>session_id, request_id</c> break ties so the
    /// cap is deterministic. The trailing <c>total_count</c> column is the pre-cap match count, read by
    /// <see cref="ReadQuerySnapshotsWithTotalAsync"/>. $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public static readonly string LatestQuerySnapshotsSql = $"""
        SELECT
        {QuerySnapshotColumns},
            COUNT(*) OVER () AS total_count
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        AND   query_text NOT LIKE 'WAITFOR%'
        ORDER BY collection_time DESC, cpu_time_ms DESC, session_id, request_id
        LIMIT {MaxLatestQuerySnapshotRows}
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

    /// <summary>Active-Queries grid rows over [<paramref name="startUtc"/>, <paramref name="endUtc"/>], newest
    /// <see cref="MaxLatestQuerySnapshotRows"/> only. TotalCount is the full window's pre-cap match count, so
    /// the caller can show a "showing newest N of total" note when the window holds more than the cap.</summary>
    public async Task<(int TotalCount, List<ViewerQuerySnapshotRow> Rows)> GetLatestQuerySnapshotsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(LatestQuerySnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddServerWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        return await ReadQuerySnapshotsWithTotalAsync(command, cancellationToken);
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        var rows = await ReadQuerySnapshotsAsync(command, cancellationToken);
        DateTime? batchTime = rows.Count > 0 ? rows[0].CollectionTime : null;
        return (batchTime, rows);
    }

    /// <summary>
    /// The stored estimated execution plan for one Active-Queries / wait drill-down snapshot row, keyed by
    /// its natural key (server, collection_time, session_id, request_id). All four key columns are
    /// equality-bound, so <c>ORDER BY collection_time DESC</c> would be a no-op here; <c>LIMIT 1</c> plus the
    /// <c>IS NOT NULL</c> guard exist only to protect against a hypothetical duplicate row sharing that key
    /// (query_snapshots has no enforced uniqueness on it) shadowing the real one.
    /// </summary>
    public const string QuerySnapshotEstimatedPlanSql = """
        SELECT query_plan
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = $2
        AND   session_id = $3
        AND   request_id = $4
        AND   query_plan IS NOT NULL
        LIMIT 1
        """;

    /// <summary>The stored live/actual execution plan for one snapshot row. See
    /// <see cref="QuerySnapshotEstimatedPlanSql"/> for the key-uniqueness note.</summary>
    public const string QuerySnapshotLivePlanSql = """
        SELECT live_query_plan
        FROM query_snapshots
        WHERE server_id = $1
        AND   collection_time = $2
        AND   session_id = $3
        AND   request_id = $4
        AND   live_query_plan IS NOT NULL
        LIMIT 1
        """;

    /// <summary>
    /// On-demand plan fetch for one Active-Queries / wait drill-down snapshot row (#4239) — a stored-row
    /// read no longer carries full plan XML in every row (only the has-plan flags), so a plan button click
    /// fetches the one row's XML by its natural key. <paramref name="collectionTimeUtc"/> must be the row's
    /// <see cref="ViewerQuerySnapshotRow.CollectionTime"/> exactly (naive UTC, microsecond-preserving) —
    /// never <c>CollectionTimeLocal</c>, which has already been shifted for display. Returns null when no
    /// plan was captured for that request.
    /// </summary>
    public async Task<string?> GetQuerySnapshotPlanXmlAsync(
        int serverId, DateTime collectionTimeUtc, int sessionId, int requestId, bool live, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(live ? QuerySnapshotLivePlanSql : QuerySnapshotEstimatedPlanSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = sessionId });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = requestId });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is string s ? s : null;
    }

    /// <summary>
    /// Maps one <c>query_snapshots</c> row (<see cref="QuerySnapshotColumns"/> shape, ordinals 0-34) — shared
    /// by every store-read snapshot query. <see cref="ViewerQuerySnapshotRow.HasQueryPlan"/> /
    /// <see cref="ViewerQuerySnapshotRow.HasLiveQueryPlan"/> come straight from the <c>IS NOT NULL</c> presence
    /// columns at 18/19 (never DBNull), and the plan XML properties stay null — a stored-row read fetches plan
    /// text on demand via <see cref="GetQuerySnapshotPlanXmlAsync"/> instead of carrying it in every row (#4239).
    /// </summary>
    private static ViewerQuerySnapshotRow ReadQuerySnapshotRow(NpgsqlDataReader reader)
    {
        return new ViewerQuerySnapshotRow
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
            HasQueryPlan = reader.GetBoolean(18),
            HasLiveQueryPlan = reader.GetBoolean(19),
            QueryPlan = null,
            LiveQueryPlan = null,
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
        };
    }

    private static async Task<List<ViewerQuerySnapshotRow>> ReadQuerySnapshotsAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerQuerySnapshotRow>();

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadQuerySnapshotRow(reader));
        }

        return rows;
    }

    /// <summary>The capped-read counterpart of <see cref="ReadQuerySnapshotsAsync"/> — also reads the
    /// trailing <c>total_count</c> column (ordinal 35, <see cref="LatestQuerySnapshotsSql"/> only) carrying
    /// the pre-cap match count, constant across every returned row.</summary>
    private static async Task<(int TotalCount, List<ViewerQuerySnapshotRow> Rows)> ReadQuerySnapshotsWithTotalAsync(
        NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerQuerySnapshotRow>();
        var totalCount = 0;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(ReadQuerySnapshotRow(reader));
            totalCount = (int)reader.GetInt64(35);
        }

        return (totalCount, rows);
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
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
