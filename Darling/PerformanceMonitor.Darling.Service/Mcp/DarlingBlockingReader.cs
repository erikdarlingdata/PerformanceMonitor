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
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// Service-side reads for the blocking / deadlock MCP tools (<see cref="DarlingMcpBlockingTools"/>) —
/// the SAME collected data the viewer's <c>ViewerDataService.Blocking.cs</c> / <c>.Deadlock.cs</c>
/// partials read, adapted here so the MCP host never references the WPF viewer project. Each read is a
/// STORED read (no live monitored-server hit), keyed by <c>server_id</c> and windowed on the naive-UTC
/// <c>collection_time</c> prefix.
///
/// <para>
/// The blocked-process read reproduces the viewer's / alert adapter's XE-preferred + DMV-fallback merge
/// via the shared <see cref="BlockedProcessReportMerge"/>, and the row types derive from the shared
/// <see cref="BlockedProcessAlertRow"/> / <see cref="DeadlockAlertRow"/> so the merge and the parsed
/// <see cref="DeadlockAlertRow.ProcessSummary"/> come for free and cannot drift from Lite. The reads hit
/// the BASE tables <c>blocked_process_reports</c> / <c>deadlocks</c> (like the viewer's grid reads and the
/// merged <see cref="DarlingAlertReadAdapter"/>) — the V7 best-effort plan columns are not reliably
/// projected through the frozen <c>SELECT *</c> views on an upgraded store — and <c>v_dmv_blocking_snapshots</c>
/// for the always-on DMV fallback (which carries no report XML). Every SQL string is a public const so
/// Darling.Tests can pin the dialect + columns without a live Postgres.
/// </para>
///
/// <para><b>Two clock frames in one row, reconciled at the read boundary (load-bearing).</b> A
/// blocked-process row carries an <c>event_time</c> that is already naive UTC on BOTH arms — the XE
/// <c>@timestamp</c> for the report rows, the collector's own <c>collection_time</c> for the DMV snapshot —
/// while the six transaction/batch stamps beside it are the monitored server's LOCAL wall clock: the report
/// arm parses them out of the blocked-process-report XML's <c>lasttranstarted</c> / <c>lastbatchstarted</c> /
/// <c>lastbatchcompleted</c> attributes, and the DMV arm ships
/// <c>sys.dm_tran_active_transactions.transaction_begin_time</c> verbatim. So the mixture is inside a single
/// ROW, not merely a single payload. These reads de-skew all six to naive UTC by the collected
/// <c>server_properties.utc_offset_minutes</c> (V16), leaving the STORED frame local so the collectors keep
/// comparing local against local; a server with no offset yet collected falls back to 0, and the single-row
/// COALESCE CTE guarantees the cross join never drops an event.
/// </para>
///
/// <para><b>Why converting matters here specifically.</b> These six are how a reader establishes whether a
/// blocker's transaction PREDATES the blocking event — that ordering is the whole diagnostic. Read as UTC
/// when they are local they are early by the server's offset (4 hours on the production fleet), which
/// inverts the ordering and makes a transaction that began during the block look like it began before it.
/// The window stays on the naive-UTC <c>collection_time</c>, so the de-skew changes no row SELECTION — only
/// the values returned.
/// </para>
///
/// <para>
/// The tool result shapes mirror LITE's <c>McpBlockingTools</c> (get_blocked_process_reports /
/// get_deadlocks / get_deadlock_detail / get_blocked_process_xml) field-for-field — the store-faithful
/// shape Darling's collector-mirror schema can serve, the precedent slices 1+2 set. The Dashboard's
/// get_blocking additionally carries a pre-computed <c>blocking_tree</c> / <c>wait_time_sec</c> /
/// <c>activity</c> from its SQL-Server-side wide store; those columns do NOT exist in the Postgres store,
/// so get_blocking here returns the store-native blocked/blocking pair rows (the same rows the viewer's
/// chain reconstructor consumes) under the Dashboard tool name + param contract.
/// </para>
/// </summary>
internal static class DarlingBlockingReader
{
    /* ─────────────────────────── result rows ─────────────────────────── */

    /// <summary>One blocked-process event — the shared alert-row fields (used by the XE→DMV merge and the
    /// context builders) plus the fuller per-report columns Lite's get_blocked_process_reports surfaces.
    /// DMV-fallback rows leave the XE-only columns at their defaults (the DMV snapshot has no report XML,
    /// isolation levels, transaction/batch times, or priorities).</summary>
    public sealed class BlockedProcessReadRow : BlockedProcessAlertRow
    {
        public int BlockedEcid { get; set; }
        public int BlockingEcid { get; set; }
        public string? WaitResource { get; set; }
        public string? BlockedStatus { get; set; }
        public string? BlockedIsolationLevel { get; set; }
        public long BlockedLogUsed { get; set; }
        public int BlockedTransactionCount { get; set; }
        public string? BlockedClientApp { get; set; }
        public string? BlockedHostName { get; set; }
        public string? BlockedLoginName { get; set; }
        public string? BlockingStatus { get; set; }
        public string? BlockingIsolationLevel { get; set; }
        public string? BlockingClientApp { get; set; }
        public string? BlockingHostName { get; set; }
        public string? BlockingLoginName { get; set; }
        public string? BlockedTransactionName { get; set; }
        public string? BlockingTransactionName { get; set; }
        public DateTime? BlockedLastTranStartedUtc { get; set; }
        public DateTime? BlockingLastTranStartedUtc { get; set; }
        public DateTime? BlockedLastBatchStartedUtc { get; set; }
        public DateTime? BlockingLastBatchStartedUtc { get; set; }
        public DateTime? BlockedLastBatchCompletedUtc { get; set; }
        public DateTime? BlockingLastBatchCompletedUtc { get; set; }
        public int BlockedPriority { get; set; }
        public int BlockingPriority { get; set; }
    }

    /// <summary>One deadlock event — the shared alert-row fields (victim id/SQL, graph XML, the parsed
    /// <see cref="DeadlockAlertRow.ProcessSummary"/>) plus the two store timestamps the grid/tool surface.</summary>
    public sealed class DeadlockReadRow : DeadlockAlertRow
    {
        public DateTime CollectionTime { get; set; }
        public DateTime? DeadlockTime { get; set; }
    }

    /* ─────────────────────────── blocked-process reports (XE + DMV fallback) ─────────────────────────── */

    /// <summary>
    /// The XE blocked-process-report read — the viewer's <c>BlockedProcessReportsSql</c> projection trimmed
    /// to the columns Lite's get_blocked_process_reports surfaces. Reads the BASE table (the viewer reads
    /// base here too, for the V7 plan-column safety). The six transaction/batch stamps are de-skewed from the
    /// server's local clock to naive UTC; <c>event_time</c> is the XE <c>@timestamp</c> and is already UTC, so
    /// it is deliberately left alone. $1 server_id, $2/$3 window (naive UTC).
    /// </summary>
    public const string BlockedProcessReportsSql = """
        WITH svr AS (
            SELECT COALESCE((
                SELECT sp.utc_offset_minutes
                FROM server_properties AS sp
                WHERE sp.server_id = $1
                AND   sp.utc_offset_minutes IS NOT NULL
                ORDER BY sp.collection_time DESC
                LIMIT 1), 0) AS offset_minutes
        )
        SELECT
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
            blocked_transaction_name,
            blocking_transaction_name,
            blocked_last_tran_started - make_interval(mins => svr.offset_minutes) AS blocked_last_tran_started,
            blocking_last_tran_started - make_interval(mins => svr.offset_minutes) AS blocking_last_tran_started,
            blocked_last_batch_started - make_interval(mins => svr.offset_minutes) AS blocked_last_batch_started,
            blocking_last_batch_started - make_interval(mins => svr.offset_minutes) AS blocking_last_batch_started,
            blocked_last_batch_completed - make_interval(mins => svr.offset_minutes) AS blocked_last_batch_completed,
            blocking_last_batch_completed - make_interval(mins => svr.offset_minutes) AS blocking_last_batch_completed,
            blocked_priority,
            blocking_priority,
            blocked_process_report_xml,
            contentious_object
        FROM blocked_process_reports, svr
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY event_time DESC
        LIMIT 200
        """;

    /// <summary>
    /// The always-on DMV blocking-snapshot fallback read — the viewer's <c>DmvBlockingSnapshotsSql</c>
    /// projection (the DMV snapshot carries no report XML, isolation levels, transaction/batch times, or
    /// priorities). Its <c>event_time</c> is the collector's own naive-UTC <c>collection_time</c>
    /// (<c>DmvBlockingSnapshotCollector</c> stamps it from <c>context.CollectionTime</c>), while its two
    /// transaction stamps come straight off <c>sys.dm_tran_active_transactions</c> and are server-local — so
    /// the same de-skew applies here, on two columns instead of six. Same parameters as
    /// <see cref="BlockedProcessReportsSql"/>.
    /// </summary>
    public const string DmvBlockingSnapshotsSql = """
        WITH svr AS (
            SELECT COALESCE((
                SELECT sp.utc_offset_minutes
                FROM server_properties AS sp
                WHERE sp.server_id = $1
                AND   sp.utc_offset_minutes IS NOT NULL
                ORDER BY sp.collection_time DESC
                LIMIT 1), 0) AS offset_minutes
        )
        SELECT
            event_time,
            database_name,
            blocked_spid,
            blocked_ecid,
            blocking_spid,
            blocking_ecid,
            wait_time_ms,
            lock_mode,
            blocking_status,
            contentious_object,
            blocked_sql_text,
            blocking_sql_text,
            blocked_login_name,
            blocked_host_name,
            blocked_client_app,
            blocking_login_name,
            blocking_host_name,
            blocking_client_app,
            blocked_last_tran_started - make_interval(mins => svr.offset_minutes) AS blocked_last_tran_started,
            blocking_last_tran_started - make_interval(mins => svr.offset_minutes) AS blocking_last_tran_started
        FROM v_dmv_blocking_snapshots, svr
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY event_time DESC
        LIMIT 200
        """;

    /// <summary>
    /// The recent blocked-process reports over the window — the XE rows plus the DMV-fallback rows for any
    /// (blocked, blocker) SPID pair the XE session did not capture in the same minute, merged and re-capped
    /// to the newest 200 via the shared <see cref="BlockedProcessReportMerge"/> (Lite's exact semantics).
    /// </summary>
    public static async Task<List<BlockedProcessReadRow>> GetRecentBlockedProcessReportsAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var items = new List<BlockedProcessReadRow>();
        var dmvItems = new List<BlockedProcessReadRow>();

        await using var connection = await postgres.OpenConnectionAsync(cancellationToken);

        await using (var command = new NpgsqlCommand(BlockedProcessReportsSql, connection))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                items.Add(new BlockedProcessReadRow
                {
                    EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                    DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                    BlockedEcid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockingSpid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingEcid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    WaitTimeMs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    WaitResource = reader.IsDBNull(7) ? null : reader.GetString(7),
                    LockMode = reader.IsDBNull(8) ? "" : reader.GetString(8),
                    BlockedStatus = reader.IsDBNull(9) ? null : reader.GetString(9),
                    BlockedIsolationLevel = reader.IsDBNull(10) ? null : reader.GetString(10),
                    BlockedLogUsed = reader.IsDBNull(11) ? 0 : reader.GetInt64(11),
                    BlockedTransactionCount = reader.IsDBNull(12) ? 0 : reader.GetInt32(12),
                    BlockedClientApp = reader.IsDBNull(13) ? null : reader.GetString(13),
                    BlockedHostName = reader.IsDBNull(14) ? null : reader.GetString(14),
                    BlockedLoginName = reader.IsDBNull(15) ? null : reader.GetString(15),
                    BlockedSqlText = reader.IsDBNull(16) ? "" : reader.GetString(16),
                    BlockingStatus = reader.IsDBNull(17) ? null : reader.GetString(17),
                    BlockingIsolationLevel = reader.IsDBNull(18) ? null : reader.GetString(18),
                    BlockingClientApp = reader.IsDBNull(19) ? null : reader.GetString(19),
                    BlockingHostName = reader.IsDBNull(20) ? null : reader.GetString(20),
                    BlockingLoginName = reader.IsDBNull(21) ? null : reader.GetString(21),
                    BlockingSqlText = reader.IsDBNull(22) ? "" : reader.GetString(22),
                    BlockedTransactionName = reader.IsDBNull(23) ? null : reader.GetString(23),
                    BlockingTransactionName = reader.IsDBNull(24) ? null : reader.GetString(24),
                    BlockedLastTranStartedUtc = reader.IsDBNull(25) ? null : reader.GetDateTime(25),
                    BlockingLastTranStartedUtc = reader.IsDBNull(26) ? null : reader.GetDateTime(26),
                    BlockedLastBatchStartedUtc = reader.IsDBNull(27) ? null : reader.GetDateTime(27),
                    BlockingLastBatchStartedUtc = reader.IsDBNull(28) ? null : reader.GetDateTime(28),
                    BlockedLastBatchCompletedUtc = reader.IsDBNull(29) ? null : reader.GetDateTime(29),
                    BlockingLastBatchCompletedUtc = reader.IsDBNull(30) ? null : reader.GetDateTime(30),
                    BlockedPriority = reader.IsDBNull(31) ? 0 : reader.GetInt32(31),
                    BlockingPriority = reader.IsDBNull(32) ? 0 : reader.GetInt32(32),
                    BlockedProcessReportXml = reader.IsDBNull(33) ? "" : reader.GetString(33),
                    ContentiousObject = reader.IsDBNull(34) ? "" : reader.GetString(34),
                });
            }
        }

        await using (var command = new NpgsqlCommand(DmvBlockingSnapshotsSql, connection))
        {
            command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
            DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                dmvItems.Add(new BlockedProcessReadRow
                {
                    EventTime = reader.IsDBNull(0) ? null : reader.GetDateTime(0),
                    DatabaseName = reader.IsDBNull(1) ? "" : reader.GetString(1),
                    BlockedSpid = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                    BlockedEcid = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                    BlockingSpid = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                    BlockingEcid = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                    WaitTimeMs = reader.IsDBNull(6) ? 0 : reader.GetInt64(6),
                    LockMode = reader.IsDBNull(7) ? "" : reader.GetString(7),
                    BlockingStatus = reader.IsDBNull(8) ? null : reader.GetString(8),
                    ContentiousObject = reader.IsDBNull(9) ? "" : reader.GetString(9),
                    BlockedSqlText = reader.IsDBNull(10) ? "" : reader.GetString(10),
                    BlockingSqlText = reader.IsDBNull(11) ? "" : reader.GetString(11),
                    BlockedLoginName = reader.IsDBNull(12) ? null : reader.GetString(12),
                    BlockedHostName = reader.IsDBNull(13) ? null : reader.GetString(13),
                    BlockedClientApp = reader.IsDBNull(14) ? null : reader.GetString(14),
                    BlockingLoginName = reader.IsDBNull(15) ? null : reader.GetString(15),
                    BlockingHostName = reader.IsDBNull(16) ? null : reader.GetString(16),
                    BlockingClientApp = reader.IsDBNull(17) ? null : reader.GetString(17),
                    BlockedLastTranStartedUtc = reader.IsDBNull(18) ? null : reader.GetDateTime(18),
                    BlockingLastTranStartedUtc = reader.IsDBNull(19) ? null : reader.GetDateTime(19),
                    Source = BlockedProcessAlertRow.DmvSnapshotSource,
                });
            }
        }

        /* Lite's XE-preferred fallback, verbatim via the shared merge: keep all BPR rows; append a DMV row
           only where no BPR covers the same SPID pair in the same minute; re-cap to the 200 newest. */
        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems);

        return items;
    }

    /* ─────────────────────────── deadlocks ─────────────────────────── */

    /// <summary>
    /// The recent deadlock events over the window — the viewer's <c>RecentDeadlocksSql</c> against the BASE
    /// <c>deadlocks</c> table (base, for the V7 victim-plan column). The parsed
    /// <see cref="DeadlockAlertRow.ProcessSummary"/> is computed on access from the graph XML. $1 server_id,
    /// $2/$3 window (naive UTC).
    /// </summary>
    public const string RecentDeadlocksSql = """
        SELECT
            collection_time,
            deadlock_time,
            victim_process_id,
            victim_sql_text,
            deadlock_graph_xml
        FROM deadlocks
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY deadlock_time DESC
        LIMIT 50
        """;

    public static async Task<List<DeadlockReadRow>> GetRecentDeadlocksAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<DeadlockReadRow>();
        await using var command = postgres.CreateCommand(RecentDeadlocksSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddWindow(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new DeadlockReadRow
            {
                CollectionTime = reader.GetDateTime(0),
                DeadlockTime = reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                VictimProcessId = reader.IsDBNull(2) ? "" : reader.GetString(2),
                VictimSqlText = reader.IsDBNull(3) ? "" : reader.GetString(3),
                DeadlockGraphXml = reader.IsDBNull(4) ? "" : reader.GetString(4),
            });
        }

        return rows;
    }
}
