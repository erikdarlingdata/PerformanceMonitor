/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Ui;
using PerformanceMonitor.Common;
using static PerformanceMonitor.Common.DeadlockGraphProcessParser;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// The CURRENT total blocked wait time (#1839): the newest <c>dmv_blocking_snapshots</c> snapshot's
    /// summed <c>wait_time_ms</c> and distinct blocked-SPID count, or null when the store holds no
    /// snapshot for the server. Freshness is judged by the caller (the alert adapter, which knows the
    /// server's effective cadence) — this read reports the snapshot's time, not a verdict on it.
    /// <para>
    /// Scoped to ONE snapshot by <c>collection_time = MAX(collection_time)</c>, never a time window:
    /// summing a window would add up blocking that has already ended, and the alert could then never
    /// clear while the window still held it. Reads the archive-aware view for the same reason
    /// <see cref="GetLatestRunningJobsSnapshotTimeAsync"/> does — after a 512 MB reset the newest
    /// snapshot can live only in parquet, and it must read as its true (possibly stale) age rather than
    /// as absent.
    /// </para>
    /// </summary>
    public async Task<(DateTime SnapshotTime, long TotalWaitMs, int BlockedSessionCount)?> GetCurrentBlockingWaitAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        /* The two CASTs are load-bearing, not decoration: DuckDB widens SUM(BIGINT) to HUGEINT, which
           DuckDB.NET materializes as System.Numerics.BigInteger — a type Convert.ToInt64 cannot convert
           (BigInteger does not implement IConvertible), so the read threw at alert-check time without
           them. COUNT is cast for the same reason its result is read as an int. */
        command.CommandText = @"
SELECT
    collection_time,
    CAST(COALESCE(SUM(wait_time_ms), 0) AS BIGINT) AS total_wait_ms,
    CAST(COUNT(DISTINCT blocked_spid) AS INTEGER) AS blocked_sessions
FROM v_dmv_blocking_snapshots
WHERE server_id = $1
AND   collection_time = (
    SELECT MAX(collection_time)
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1
)
GROUP BY collection_time";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return null;
        }

        return (
            reader.GetDateTime(0),
            reader.IsDBNull(1) ? 0L : reader.GetInt64(1),
            reader.IsDBNull(2) ? 0 : reader.GetInt32(2));
    }

    /// <summary>
    /// Gets recent deadlock events for a server.
    /// </summary>
    public async Task<List<DeadlockRow>> GetRecentDeadlocksAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    collection_time,
    deadlock_time,
    victim_process_id,
    victim_sql_text,
    deadlock_graph_xml
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
ORDER BY deadlock_time DESC
LIMIT 50";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<DeadlockRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new DeadlockRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeadlockTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                VictimProcessId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                VictimSqlText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                DeadlockGraphXml = reader.IsDBNull(4) ? "" : reader.GetString(4)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from query snapshots for the time-range slicer.
    /// The metric column is determined by the caller's sort preference.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetActiveQuerySlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    date_trunc('hour', collection_time) AS bucket,
    COUNT(*) AS session_count,
    COALESCE(SUM(cpu_time_ms), 0) AS total_cpu,
    COALESCE(SUM(total_elapsed_time_ms), 0) AS total_elapsed,
    COALESCE(SUM(reads), 0) AS total_reads,
    COALESCE(SUM(logical_reads), 0) AS total_logical_reads,
    COALESCE(SUM(writes), 0) AS total_writes
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
GROUP BY date_trunc('hour', collection_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                TotalWrites = reader.IsDBNull(6) ? 0 : ToDouble(reader.GetValue(6)),
                Value = reader.IsDBNull(1) ? 0 : Convert.ToDouble(reader.GetValue(1)), // default: session count
            });
        }

        return items;
    }

    /// <summary>
    /// Gets query snapshots (currently running queries) for a server.
    /// </summary>
    public async Task<List<QuerySnapshotRow>> GetLatestQuerySnapshotsAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var _q = TimeQuery("GetLatestQuerySnapshotsAsync", "v_query_snapshots latest");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
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
    query_hash
FROM v_query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
AND   query_text NOT LIKE 'WAITFOR%'
ORDER BY collection_time DESC, cpu_time_ms DESC";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<QuerySnapshotRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new QuerySnapshotRow
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
                GrantedQueryMemoryGb = reader.IsDBNull(14) ? 0 : ToDouble(reader.GetValue(14)),
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
                QueryHash = reader.IsDBNull(26) ? "" : reader.GetString(26)
            });
        }

        return items;
    }

    /// <summary>
    /// Gets lightweight blocking + deadlock counts and latest event time for alert badge updates.
    /// Much cheaper than fetching full rows with XML — just COUNT(*) and MAX(time).
    /// </summary>
    public async Task<(int blockingCount, int deadlockCount, DateTime? latestEventTime)> GetAlertCountsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        /* blocking_count prefers the blocked-process-report; falls back to the always-on DMV snapshot when
           BPR captured nothing (AWS RDS). latest_event_time includes DMV blocking recency too. */
        command.CommandText = @"
SELECT
    COALESCE(NULLIF((SELECT COUNT(*) FROM v_blocked_process_reports
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3), 0),
     (SELECT COUNT(*) FROM v_dmv_blocking_snapshots
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)) AS blocking_count,
    (SELECT COUNT(*) FROM v_deadlocks
     WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3) AS deadlock_count,
    (SELECT MAX(t) FROM (
        SELECT MAX(event_time) AS t FROM v_blocked_process_reports
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
        UNION ALL
        SELECT MAX(event_time) AS t FROM v_dmv_blocking_snapshots
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
        UNION ALL
        SELECT MAX(deadlock_time) AS t FROM v_deadlocks
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    )) AS latest_event_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return (0, 0, null);

        var blockingCount = reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0));
        var deadlockCount = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1));
        var latestEventTime = reader.IsDBNull(2) ? (DateTime?)null : reader.GetDateTime(2);

        return (blockingCount, deadlockCount, latestEventTime);
    }

    /// <summary>
    /// Gets recent blocked process reports from the XE-based collector.
    /// </summary>
    public async Task<List<BlockedProcessReportRow>> GetRecentBlockedProcessReportsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        command.CommandText = @"
SELECT
    collection_time,
    event_time,
    database_name,
    blocked_spid,
    blocked_ecid,
    blocking_spid,
    blocking_ecid,
    wait_time_ms,
    wait_resource,
    lock_mode,
    blocked_status,
    blocked_isolation_level,
    blocked_log_used,
    blocked_transaction_count,
    blocked_client_app,
    blocked_host_name,
    blocked_login_name,
    blocked_sql_text,
    blocking_status,
    blocking_isolation_level,
    blocking_client_app,
    blocking_host_name,
    blocking_login_name,
    blocking_sql_text,
    blocked_process_report_xml,
    blocked_transaction_name,
    blocking_transaction_name,
    blocked_last_tran_started,
    blocking_last_tran_started,
    blocked_last_batch_started,
    blocking_last_batch_started,
    blocked_last_batch_completed,
    blocking_last_batch_completed,
    blocked_priority,
    blocking_priority,
    contentious_object,
    monitor_loop
FROM v_blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3" + dbClause + @"
ORDER BY event_time DESC
LIMIT 200";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<BlockedProcessReportRow>();
        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                items.Add(new BlockedProcessReportRow
                {
                    CollectionTime = reader.GetDateTime(0),
                    EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    BlockedSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockedEcid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingSpid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    BlockingEcid = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                    WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    WaitResource = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    LockMode = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    BlockedStatus = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    BlockedIsolationLevel = reader.IsDBNull(11) ? "" : reader.GetString(11),
                    BlockedLogUsed = reader.IsDBNull(12) ? 0 : reader.GetInt64(12),
                    BlockedTransactionCount = reader.IsDBNull(13) ? 0 : reader.GetInt32(13),
                    BlockedClientApp = reader.IsDBNull(14) ? "" : reader.GetString(14),
                    BlockedHostName = reader.IsDBNull(15) ? "" : reader.GetString(15),
                    BlockedLoginName = reader.IsDBNull(16) ? "" : reader.GetString(16),
                    BlockedSqlText = reader.IsDBNull(17) ? "" : reader.GetString(17),
                    BlockingStatus = reader.IsDBNull(18) ? "" : reader.GetString(18),
                    BlockingIsolationLevel = reader.IsDBNull(19) ? "" : reader.GetString(19),
                    BlockingClientApp = reader.IsDBNull(20) ? "" : reader.GetString(20),
                    BlockingHostName = reader.IsDBNull(21) ? "" : reader.GetString(21),
                    BlockingLoginName = reader.IsDBNull(22) ? "" : reader.GetString(22),
                    BlockingSqlText = reader.IsDBNull(23) ? "" : reader.GetString(23),
                    BlockedProcessReportXml = reader.IsDBNull(24) ? "" : reader.GetString(24),
                    BlockedTransactionName = reader.IsDBNull(25) ? "" : reader.GetString(25),
                    BlockingTransactionName = reader.IsDBNull(26) ? "" : reader.GetString(26),
                    BlockedLastTranStarted = reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                    BlockingLastTranStarted = reader.IsDBNull(28) ? null : reader.GetDateTime(28),
                    BlockedLastBatchStarted = reader.IsDBNull(29) ? null : reader.GetDateTime(29),
                    BlockingLastBatchStarted = reader.IsDBNull(30) ? null : reader.GetDateTime(30),
                    BlockedLastBatchCompleted = reader.IsDBNull(31) ? null : reader.GetDateTime(31),
                    BlockingLastBatchCompleted = reader.IsDBNull(32) ? null : reader.GetDateTime(32),
                    BlockedPriority = reader.IsDBNull(33) ? 0 : reader.GetInt32(33),
                    BlockingPriority = reader.IsDBNull(34) ? 0 : reader.GetInt32(34),
                    ContentiousObject = reader.IsDBNull(35) ? "" : reader.GetString(35),
                    MonitorLoop = reader.IsDBNull(36) ? (int?)null : reader.GetInt32(36)
                });
            }
        }

        // Always-on DMV blocking snapshot: surface its rows in the grid too, so the block-chain viewer is
        // reachable when the blocked-process-report XE captured nothing (AWS RDS). Same connection/lock.
        await AppendDmvBlockedProcessGridRowsAsync(connection.CreateCommand, items, serverId, startTime, endTime, databaseNames);

        return items;
    }

    /// <summary>
    /// Fetches always-on DMV blocking-snapshot rows for the blocked-process grid and merges them into the
    /// BPR list — BPR preferred (dedup by blocked/blocker SPID within a minute), re-capped to 200 newest
    /// first. Runs on the caller's connection/lock (the read lock is non-recursive, so a second connection
    /// can't be opened). v_dmv_blocking_snapshots is created by DuckDbInitializer, so it always exists.
    /// </summary>
    private static async Task AppendDmvBlockedProcessGridRowsAsync(
        Func<DuckDBCommand> createCommand, List<BlockedProcessReportRow> items, int serverId, DateTime startTime, DateTime endTime, IReadOnlyList<string>? databaseNames = null)
    {
        const int gridCap = 200;
        var dmvItems = new List<BlockedProcessReportRow>();
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);
        using (var command = createCommand())
        {
            command.CommandText = @"
SELECT
    collection_time, event_time, database_name,
    blocked_spid, blocked_ecid, blocking_spid, blocking_ecid,
    wait_time_ms, lock_mode, blocking_status, contentious_object,
    blocked_sql_text, blocking_sql_text,
    blocked_login_name, blocked_host_name, blocked_client_app,
    blocked_last_tran_started, blocking_last_tran_started, monitor_loop,
    blocking_login_name, blocking_host_name, blocking_client_app
FROM v_dmv_blocking_snapshots
WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3" + dbClause + @"
ORDER BY event_time DESC
LIMIT 200";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            foreach (var db in dbValues)
                command.Parameters.Add(new DuckDBParameter { Value = db });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                dmvItems.Add(new BlockedProcessReportRow
                {
                    CollectionTime = reader.GetDateTime(0),
                    EventTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                    DatabaseName = reader.IsDBNull(2) ? "" : reader.GetString(2),
                    BlockedSpid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockedEcid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingSpid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    BlockingEcid = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                    WaitTimeMs = reader.IsDBNull(7) ? 0 : reader.GetInt64(7),
                    LockMode = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    BlockingStatus = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    ContentiousObject = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    BlockedSqlText = reader.IsDBNull(11) ? "" : reader.GetString(11),
                    BlockingSqlText = reader.IsDBNull(12) ? "" : reader.GetString(12),
                    BlockedLoginName = reader.IsDBNull(13) ? "" : reader.GetString(13),
                    BlockedHostName = reader.IsDBNull(14) ? "" : reader.GetString(14),
                    BlockedClientApp = reader.IsDBNull(15) ? "" : reader.GetString(15),
                    BlockedLastTranStarted = reader.IsDBNull(16) ? null : reader.GetDateTime(16),
                    BlockingLastTranStarted = reader.IsDBNull(17) ? null : reader.GetDateTime(17),
                    MonitorLoop = reader.IsDBNull(18) ? (int?)null : reader.GetInt32(18),
                    /* Blocking-side identity — the DMV snapshot is the only blocking source on AWS RDS / when the
                       BPR threshold is unset, so surface it here too (the XE path already sets these). */
                    BlockingLoginName = reader.IsDBNull(19) ? "" : reader.GetString(19),
                    BlockingHostName = reader.IsDBNull(20) ? "" : reader.GetString(20),
                    BlockingClientApp = reader.IsDBNull(21) ? "" : reader.GetString(21),
                    Source = BlockedProcessAlertRow.DmvSnapshotSource
                });
            }
        }

        /* Dedup + re-cap moved verbatim to the shared BlockedProcessReportMerge (Phase-5 slice B)
           so the Darling Postgres adapter reproduces EXACTLY these XE-preferred fallback semantics. */
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems, gridCap);
    }

    /// <summary>
    /// Fetches the blocked/blocker pair rows for a window, for the block-chain viewer to feed
    /// <see cref="BlockingChainReconstructor"/>. Internal (not public): <see cref="BlockingPairRow"/> is
    /// internal to the analysis assembly — a public method returning it would be CS0050.
    /// <see cref="OpenConnectionAsync"/> already takes the DuckDB read lock, so do NOT acquire it again
    /// (the lock is NoRecursion). Uses <see cref="Analysis.BlockingPairRowQuery.SpidFilter"/> so it agrees
    /// with the drill-down + fact collectors on the apex.
    /// </summary>
    internal async Task<List<BlockingPairRow>> GetBlockingPairRowsAsync(int serverId, DateTime start, DateTime end)
    {
        var rows = new List<BlockingPairRow>();

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.LeadingColumns},
    blocked_sql_text, blocking_sql_text,
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {PerformanceMonitorLite.Analysis.BlockingPairRowQuery.TrailingIdentityColumns}
FROM v_blocked_process_reports
WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
{PerformanceMonitorLite.Analysis.BlockingPairRowQuery.SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = start });
        command.Parameters.Add(new DuckDBParameter { Value = end });

        using (var reader = await command.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
                rows.Add(PerformanceMonitorLite.Analysis.BlockingPairRowQuery.Read(reader));
        }

        // Always-on DMV blocking snapshot: merge in the fallback rows so the viewer works even when the
        // blocked-process-report XE captured nothing (threshold unset / AWS RDS). Same connection/lock.
        /* #2443: CancellationToken.None because the viewer genuinely has no pass to abandon — this
           read serves a person waiting at a grid, not an analysis pass with a budget or a service
           stop. Stated here rather than defaulted in the shared method, so the exception belongs to
           the caller that actually has it. */
        await PerformanceMonitorLite.Analysis.BlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            connection.CreateCommand, rows, serverId, start, end, System.Threading.CancellationToken.None);

        return rows;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from blocked process reports for the time-range slicer.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetBlockingSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        /* BPR buckets, falling back to the always-on DMV snapshot only when BPR has no buckets in the
           window (AWS RDS) — so a server with both sources never double-counts. */
        command.CommandText = @"
WITH bpr AS (
    SELECT
        date_trunc('hour', collection_time) AS bucket,
        COUNT(*) AS event_count,
        COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
        COUNT(DISTINCT blocking_spid) AS distinct_blockers,
        COUNT(DISTINCT blocked_spid) AS distinct_blocked,
        COUNT(DISTINCT database_name) AS distinct_databases
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    GROUP BY date_trunc('hour', collection_time)
),
dmv AS (
    SELECT
        date_trunc('hour', collection_time) AS bucket,
        COUNT(*) AS event_count,
        COALESCE(SUM(wait_time_ms), 0) / 1000.0 AS total_wait_sec,
        COUNT(DISTINCT blocking_spid) AS distinct_blockers,
        COUNT(DISTINCT blocked_spid) AS distinct_blocked,
        COUNT(DISTINCT database_name) AS distinct_databases
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3" + dbClause + @"
    GROUP BY date_trunc('hour', collection_time)
)
SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM bpr
UNION ALL
SELECT bucket, event_count, total_wait_sec, distinct_blockers, distinct_blocked, distinct_databases FROM dmv
WHERE NOT EXISTS (SELECT 1 FROM bpr)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var eventCount = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = eventCount,
                TotalCpu = reader.IsDBNull(2) ? 0 : ToDouble(reader.GetValue(2)),
                TotalElapsed = reader.IsDBNull(3) ? 0 : ToDouble(reader.GetValue(3)),
                TotalReads = reader.IsDBNull(4) ? 0 : ToDouble(reader.GetValue(4)),
                TotalLogicalReads = reader.IsDBNull(5) ? 0 : ToDouble(reader.GetValue(5)),
                Value = eventCount,
            });
        }

        return items;
    }

    /// <summary>
    /// Gets hourly-bucketed metrics from deadlocks for the time-range slicer.
    /// </summary>
    public async Task<List<TimeSliceBucket>> GetDeadlockSlicerDataAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate);

        command.CommandText = @"
SELECT
    date_trunc('hour', collection_time) AS bucket,
    COUNT(*) AS deadlock_count
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY date_trunc('hour', collection_time)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TimeSliceBucket>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var count = reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1));
            items.Add(new TimeSliceBucket
            {
                BucketTime = reader.GetDateTime(0),
                SessionCount = count,
                Value = count,
            });
        }

        return items;
    }

    /// <summary>
    /// Gets blocking incident trend (count of distinct blocking events per time bucket).
    /// Uses blocked_process_reports from Extended Events for more reliable detection.
    /// Falls back to blocking_snapshots if no XE data available.
    /// </summary>
    public async Task<List<TrendPoint>> GetBlockingTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, IReadOnlyList<string>? databaseNames = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);
        var dbClause = BuildDbInClause(databaseNames, "database_name", 4, out var dbValues);

        /* Use blocked_process_reports from XE session - more reliable than point-in-time snapshots
           Group by event_time (when blocking actually occurred) rather than collection_time */
        /* BPR per-minute buckets, falling back to the always-on DMV snapshot only when BPR has none in the
           window (AWS RDS) — so a server with both sources never double-counts. */
        command.CommandText = @"
WITH bpr AS (
    SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
    FROM v_blocked_process_reports
    WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause + @"
    GROUP BY DATE_TRUNC('minute', event_time)
),
dmv AS (
    SELECT DATE_TRUNC('minute', event_time) AS bucket, COUNT(*) AS incident_count
    FROM v_dmv_blocking_snapshots
    WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3" + dbClause + @"
    GROUP BY DATE_TRUNC('minute', event_time)
)
SELECT bucket, incident_count FROM bpr
UNION ALL
SELECT bucket, incident_count FROM dmv WHERE NOT EXISTS (SELECT 1 FROM bpr)
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        foreach (var db in dbValues)
            command.Parameters.Add(new DuckDBParameter { Value = db });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }

    /// <summary>
    /// Gets deadlock trend (count of deadlocks per minute bucket).
    /// </summary>
    public async Task<List<TrendPoint>> GetDeadlockTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    bucket,
    deadlock_count
FROM (
    SELECT
        DATE_TRUNC('minute', deadlock_time) AS bucket,
        COUNT(*) AS deadlock_count
    FROM v_deadlocks
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    GROUP BY DATE_TRUNC('minute', deadlock_time)
) sub
ORDER BY bucket";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<TrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new TrendPoint
            {
                Time = reader.GetDateTime(0),
                Count = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1))
            });
        }
        return items;
    }

    /* ───────────────── the denominator an empty trend needs (#2485) ───────────────── */

    /// <summary>
    /// Successful runs per blocking collector inside the window, with the first and last of them.
    /// <para>The blocking trend reads EDGE tables — rows exist only where an event happened — so an
    /// absence of rows is a capture that found nothing and a capture that never ran, wearing the same
    /// face. <c>collection_log</c> can tell them apart, because a collector that ran and stored nothing
    /// still records a SUCCESS with zero rows.</para>
    /// <para>BOTH capture paths are counted, deliberately: <see cref="GetBlockingTrendAsync"/> unions
    /// <c>v_blocked_process_reports</c> with <c>v_dmv_blocking_snapshots</c>, so counting one of them
    /// would report "never captured" for a server capturing perfectly well through the other. Only
    /// SUCCESS counts as having looked — a PERMISSIONS or ERROR row is a collector that did not see the
    /// window either. Darling's twin is <c>DarlingBlockingTrendReader.BlockingCaptureCountsSql</c>; the
    /// two must stay in step so a user moving between the SKUs is not told a different story about the
    /// same state.</para>
    /// </summary>
    public Task<List<CollectorCaptureCount>> GetBlockingCaptureCountsAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        GetCaptureCountsAsync(serverId, hoursBack, "collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')", fromDate, toDate, asOfUtc);

    /// <summary>
    /// Successful runs of the deadlock collector inside the window. One capture path here, not two —
    /// deadlocks come only from the <c>deadlocks</c> collector's system_health read, and there is no DMV
    /// fallback to count.
    /// </summary>
    public Task<List<CollectorCaptureCount>> GetDeadlockCaptureCountsAsync(
        int serverId, int hoursBack, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null) =>
        GetCaptureCountsAsync(serverId, hoursBack, "collector_name = 'deadlocks'", fromDate, toDate, asOfUtc);

    /// <summary>
    /// Whether either blocking collector has EVER run successfully for this server, ignoring any window.
    /// <para>Asked ONLY when the window count came back zero, and only to pick which sentence is true: a
    /// server whose collectors have run before has a GAP in this window, while one that has never run
    /// them is not collecting blocking at all. Both are "not an all-clear" and they want different next
    /// moves. LIMIT 1, so it stops at the first row.</para>
    /// <para>NOT the same question as the neighbouring <c>HasAnyBlockingCaptureAsync</c> in
    /// <c>LocalDataService.BlockingStats.cs</c>, which asks whether an EVENT was ever captured. That one is
    /// right for get_blocking_stats, whose verdict is about severity; it is wrong here, because a server
    /// collected perfectly for months that simply never blocked has no event rows and would be reported as
    /// never captured — the reassuring-answer failure this read exists to prevent, inverted. Hence
    /// "collector run", not "capture": the denominator is whether we LOOKED, not whether we found
    /// something. Darling's twin is <c>DarlingBlockingTrendReader.HasAnyBlockingCollectorRunAsync</c>.</para>
    /// </summary>
    public Task<bool> HasAnyBlockingCollectorRunAsync(int serverId) =>
        HasAnyCaptureAsync(serverId, "collector_name IN ('blocked_process_report', 'dmv_blocking_snapshot')");

    /// <summary>Whether the deadlock collector has EVER run successfully for this server. See
    /// <see cref="HasAnyBlockingCollectorRunAsync"/> for why the question is asked at all.</summary>
    public Task<bool> HasAnyDeadlockCollectorRunAsync(int serverId) =>
        HasAnyCaptureAsync(serverId, "collector_name = 'deadlocks'");

    /// <summary>
    /// The shared body behind the two capture-count reads. The collector predicate is a COMPILE-TIME
    /// literal from the two call sites above — never caller input — so it is concatenated rather than
    /// bound; the server id and window still bind as parameters.
    /// </summary>
    /* fromDate/toDate let a CALLER pin one instant across the trend read and this one. Resolving now
       independently in each means the two can straddle a row that arrives between them, and the whole
       point of asking both is that their answers are compared. Darling's twin takes explicit bounds
       for the same reason. */
    private async Task<List<CollectorCaptureCount>> GetCaptureCountsAsync(
        int serverId, int hoursBack, string collectorPredicate, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    collector_name,
    COUNT(*),
    MIN(collection_time),
    MAX(collection_time)
FROM v_collection_log
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   status = 'SUCCESS'
AND   " + collectorPredicate + @"
GROUP BY collector_name
ORDER BY collector_name";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<CollectorCaptureCount>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CollectorCaptureCount(
                reader.GetString(0),
                reader.IsDBNull(1) ? 0 : Convert.ToInt64(reader.GetValue(1)),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                reader.IsDBNull(3) ? null : reader.GetDateTime(3)));
        }
        return items;
    }

    /// <summary>The shared body behind the two existence probes. Same literal-predicate contract as
    /// <see cref="GetCaptureCountsAsync"/>.</summary>
    private async Task<bool> HasAnyCaptureAsync(int serverId, string collectorPredicate)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT 1
FROM v_collection_log
WHERE server_id = $1
AND   status = 'SUCCESS'
AND   " + collectorPredicate + @"
LIMIT 1";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        return await command.ExecuteScalarAsync() is not null and not DBNull;
    }

    /// <summary>
    /// Gets lock wait stats trend data (LCK% wait types) for the blocking trends chart.
    /// Returns per-second rates grouped by wait type.
    ///
    /// <para>#2484: takes <paramref name="asOfUtc"/> so the MCP twin (get_lock_wait_trend) can anchor the
    /// window at a past incident. Threaded as the anchor rather than as fromDate/toDate because those two
    /// are SERVER-LOCAL and converted back to UTC inside GetTimeRange — handing them an instant already in
    /// UTC would shift the window by the monitored server's offset. collection_time is stored in UTC, so
    /// this read windows on the UTC bounds.</para>
    /// </summary>
    public async Task<List<LockWaitTrendPoint>> GetLockWaitTrendAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
WITH raw AS
(
    SELECT
        collection_time,
        wait_type,
        delta_wait_time_ms,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', LAG(collection_time) OVER (PARTITION BY wait_type ORDER BY collection_time)))) AS interval_seconds
    FROM v_wait_stats
    WHERE server_id = $1
    AND   wait_type LIKE 'LCK%'
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    collection_time,
    wait_type,
    CASE WHEN interval_seconds > 0 THEN CAST(delta_wait_time_ms AS DOUBLE PRECISION) / interval_seconds ELSE 0 END AS wait_time_ms_per_second
FROM raw
WHERE delta_wait_time_ms >= 0
ORDER BY collection_time, wait_type";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<LockWaitTrendPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new LockWaitTrendPoint
            {
                CollectionTime = reader.GetDateTime(0),
                WaitType = reader.IsDBNull(1) ? string.Empty : reader.GetString(1),
                WaitTimeMsPerSecond = reader.IsDBNull(2) ? 0 : reader.GetDouble(2)
            });
        }
        return items;
    }
}

/// <summary>
/// One collector's SUCCESSFUL run count inside a window, with the first and last of those runs — the
/// denominator an empty blocking or deadlock trend needs to be interpretable (#2485). Darling's twin is
/// <c>DarlingBlockingTrendReader.CaptureCount</c>.
/// </summary>
public sealed record CollectorCaptureCount(string CollectorName, long Runs, DateTime? FirstRunAt, DateTime? LastRunAt);

public class LockWaitTrendPoint
{
    public DateTime CollectionTime { get; set; }
    public string WaitType { get; set; } = string.Empty;
    public double WaitTimeMsPerSecond { get; set; }
}

public class TrendPoint
{
    public DateTime Time { get; set; }
    public int Count { get; set; }
}

/// <summary>
/// Lite's deadlock grid row. The alert-consumed members (victim process/SQL, graph XML, the parsed
/// <c>ProcessSummary</c>) live on the shared <see cref="DeadlockAlertRow"/> base (Phase-5 slice B)
/// so store reads flow into the shared alert builders without a mapping copy; only the
/// grid-display extras stay here.
/// </summary>
public class DeadlockRow : DeadlockAlertRow
{
    public DateTime CollectionTime { get; set; }
    public DateTime? DeadlockTime { get; set; }
}

/// <summary>
/// Lite's Deadlocks-grid row — one parsed process per deadlock graph. The raw parsed fields and the
/// sp_BlitzLock-style graph walk now live once on the shared <see cref="DeadlockProcessInfo"/> /
/// <see cref="DeadlockGraphProcessParser"/> (Common); only Lite's per-server display getters
/// (<see cref="ServerTimeHelper"/>) stay here.
/// </summary>
public class DeadlockProcessDetail : DeadlockProcessInfo
{
    public string DeadlockTimeLocal => ServerTimeHelper.FormatServerTime(DeadlockTime);
    public string VictimDisplay => IsVictim ? "Victim" : "";
    public string WaitTimeFormatted => WaitTime > 0 ? $"{WaitTime:N0} ms" : "";
    public string LastTranStartedLocal => ServerTimeHelper.FormatServerTime(LastTranStarted);

    /// <summary>
    /// Parses a list of <see cref="DeadlockRow"/> into per-process detail rows via the shared
    /// <see cref="DeadlockGraphProcessParser"/> (the sp_BlitzLock-style graph walk, de-duplicated with the
    /// Darling viewer). Lite never captures a victim plan, so the plan input is always null.
    /// </summary>
    public static List<DeadlockProcessDetail> ParseFromRows(List<DeadlockRow> rows)
        => DeadlockGraphProcessParser.Parse<DeadlockProcessDetail>(
            rows.Select(r => new DeadlockGraphInput(r.DeadlockGraphXml, r.DeadlockTime, r.VictimSqlText, null))).ToList();
}

/// <summary>
/// Lite's blocked-process grid row. The alert-consumed members (event time, database, SPID pair,
/// wait/lock, the query pair, report XML, contentious object, Source) live on the shared
/// <see cref="BlockedProcessAlertRow"/> base (Phase-5 slice B) so store reads flow into the shared
/// alert builders and the XE→DMV fallback merge without a mapping copy; only the grid-display
/// extras stay here.
/// </summary>
public class BlockedProcessReportRow : BlockedProcessAlertRow
{
    public DateTime CollectionTime { get; set; }
    public int BlockedEcid { get; set; }
    public int BlockingEcid { get; set; }
    public int? MonitorLoop { get; set; }

    public string WaitResource { get; set; } = "";
    public string BlockedStatus { get; set; } = "";
    public string BlockedIsolationLevel { get; set; } = "";
    public long BlockedLogUsed { get; set; }
    public int BlockedTransactionCount { get; set; }
    public string BlockedClientApp { get; set; } = "";
    public string BlockedHostName { get; set; } = "";
    public string BlockedLoginName { get; set; } = "";
    public string BlockingStatus { get; set; } = "";
    public string BlockingIsolationLevel { get; set; } = "";
    public string BlockingClientApp { get; set; } = "";
    public string BlockingHostName { get; set; } = "";
    public string BlockingLoginName { get; set; } = "";
    public string BlockedTransactionName { get; set; } = "";
    public string BlockingTransactionName { get; set; } = "";
    public DateTime? BlockedLastTranStarted { get; set; }
    public DateTime? BlockingLastTranStarted { get; set; }
    public DateTime? BlockedLastBatchStarted { get; set; }
    public DateTime? BlockingLastBatchStarted { get; set; }
    public DateTime? BlockedLastBatchCompleted { get; set; }
    public DateTime? BlockingLastBatchCompleted { get; set; }
    public int BlockedPriority { get; set; }
    public int BlockingPriority { get; set; }

    public string EventTimeLocal => ServerTimeHelper.FormatServerTime(EventTime);
    public string WaitTimeFormatted => WaitTimeMs < 1000 ? $"{WaitTimeMs} ms" : $"{WaitTimeMs / 1000.0:F1} sec";
    public bool IsLongBlock => WaitTimeMs > 30000;
    public string BlockedLastTranStartedLocal => ServerTimeHelper.FormatServerTime(BlockedLastTranStarted);
    public string BlockedLastBatchStartedLocal => ServerTimeHelper.FormatServerTime(BlockedLastBatchStarted);
    public string BlockedLastBatchCompletedLocal => ServerTimeHelper.FormatServerTime(BlockedLastBatchCompleted);
}

public class QuerySnapshotRow
{
    /// <summary>Gates "Get Actual Plan (re-run)" in the query grids' plan menu — the re-run executes this row's
    /// query text (ServerTab.Plans.cs). Row types the re-run handler has no case for simply omit this property,
    /// so the menu item's binding falls back to Collapsed and the verb is hidden rather than silently no-op.</summary>
    public bool CanGetActualPlan => !string.IsNullOrEmpty(QueryText);

    public int SessionId { get; set; }
    public string DatabaseName { get; set; } = "";
    public string ElapsedTimeFormatted { get; set; } = "";
    public string QueryText { get; set; } = "";
    public string Status { get; set; } = "";
    public int BlockingSessionId { get; set; }
    public string WaitType { get; set; } = "";
    public long WaitTimeMs { get; set; }
    public long CpuTimeMs { get; set; }
    public long TotalElapsedTimeMs { get; set; }
    public long Reads { get; set; }
    public long Writes { get; set; }
    public long LogicalReads { get; set; }
    public double GrantedQueryMemoryGb { get; set; }
    public string TransactionIsolationLevel { get; set; } = "";
    public int Dop { get; set; }
    public int ParallelWorkerCount { get; set; }
    public string WaitResource { get; set; } = "";
    public DateTime CollectionTime { get; set; }
    public string? QueryPlan { get; set; }
    public string? LiveQueryPlan { get; set; }
    public string LoginName { get; set; } = "";
    public string HostName { get; set; } = "";
    public string ProgramName { get; set; } = "";
    public int OpenTransactionCount { get; set; }
    public decimal PercentComplete { get; set; }
    public string QueryHash { get; set; } = "";
    public bool HasQueryPlan => !string.IsNullOrEmpty(QueryPlan);
    public bool HasLiveQueryPlan => !string.IsNullOrEmpty(LiveQueryPlan);
    public string CollectionTimeLocal => CollectionTime == DateTime.MinValue ? "" : ServerTimeHelper.FormatServerTime(CollectionTime);

    // Sessions this session is blocking at the same collection_time (SQL-derived in the
    // snapshot queries; Chain mode overwrites it with the chain walker's transitive count)
    public int BlockedSessionCount { get; set; }

    // Wait-drilldown triage columns collected from dm_exec_query_memory_grants,
    // session/task tempdb space usage, and dm_tran_* (schema v34)
    public double RequestedMemoryMb { get; set; }
    public double UsedMemoryMb { get; set; }
    public double MaxUsedMemoryMb { get; set; }
    public double TempdbCurrentMb { get; set; }
    public double TempdbAllocationsMb { get; set; }
    public double TranLogUsedMb { get; set; }
    public DateTime? TranStartTime { get; set; }
    public int RequestId { get; set; }
    public string TranStartTimeLocal => ServerTimeHelper.FormatServerTime(TranStartTime);

    // Chain mode — set by WaitDrillDownWindow when showing head blockers
    public string ChainBlockingPath { get; set; } = "";
}
