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
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Analysis;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Blocking-tab grid row — widened to Lite parity (W1e). All alert-consumed members (event time,
/// database, SPID pair, wait/lock, the query pair, report XML, contentious object, Source) live on the
/// shared <see cref="BlockedProcessAlertRow"/> base — the same shape Lite's grid row derives from — so
/// the viewer's store reads flow through the shared XE→DMV fallback merge
/// (<see cref="BlockedProcessReportMerge"/>) without a mapping copy. The grid-display extras below mirror
/// Lite's <c>BlockedProcessReportRow</c> exactly. event_time is stored naive-UTC (the XE @timestamp is
/// UTC; the DMV snapshot stamps the collector's UTC collection time), so <see cref="EventTimeLocal"/>
/// converts to viewer-local like every other collection_time — Lite's per-server
/// <c>ServerTimeHelper.FormatServerTime</c> becomes <see cref="ViewerTimeHelper.ForDisplay"/> (the
/// viewer's one machine-local convention; the same swap the trend charts already document).
/// </summary>
public sealed class ViewerBlockedProcessRow : BlockedProcessAlertRow
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

    /// <summary>The stored naive-UTC event time in the viewer machine's local time (Lite's grid format).</summary>
    public string EventTimeLocal
        => EventTime is { } eventTime ? ViewerTimeHelper.ForDisplay(eventTime).ToString("yyyy-MM-dd HH:mm:ss") : "";

    /// <summary>Lite's wait-time rendering: sub-second in ms, else one-decimal seconds.</summary>
    public string WaitTimeFormatted => ViewerDataService.FormatWaitTime(WaitTimeMs);

    /// <summary>Lite's long-block tint threshold: over 30 seconds waiting.</summary>
    public bool IsLongBlock => WaitTimeMs > 30000;

    /// <summary>
    /// The blocked / blocking query plans the Darling collector resolved BEST-EFFORT alongside the report
    /// (blocked_process_reports.blocked_query_plan_xml / blocking_query_plan_xml, #1368 / V7 — from the
    /// report's sql_handle via dm_exec_query_stats → dm_exec_text_query_plan, only when the host sets
    /// CapturePlanXml; often NULL, and always NULL on the DMV-snapshot fallback rows and under Lite). The
    /// Has* flags gate the two "View Plan" context items per row so a plan-less row shows them disabled
    /// rather than shown-and-failed. The XML rides in-row (like blocked_process_report_xml already does),
    /// so "View Plan" opens it with no second fetch.
    /// </summary>
    public string? BlockedQueryPlanXml { get; set; }
    public string? BlockingQueryPlanXml { get; set; }
    public bool HasBlockedQueryPlan => !string.IsNullOrEmpty(BlockedQueryPlanXml);
    public bool HasBlockingQueryPlan => !string.IsNullOrEmpty(BlockingQueryPlanXml);

    /* HasReportXml is inherited from BlockedProcessAlertRow (same expression over the same base
       BlockedProcessReportXml). The viewer gates the "Fetch Live Plan" items on it: the report XML's
       executionStack frames carry the sql_handle those items key on, so a DMV-snapshot fallback row
       (which has no report XML) shows them disabled. */
}

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The XE blocked-process-report read — Lite's <c>GetRecentBlockedProcessReportsAsync</c> full
    /// 37-column SELECT ported to Postgres, including <c>blocked_process_report_xml</c> (the block-chain /
    /// XML-save surface needs it) and the two BEST-EFFORT plan columns (#1368 / V7) the grid's "View Plan"
    /// items open. Same WHERE / ORDER BY event_time DESC / LIMIT 200 semantics.
    ///
    /// <para>Reads the BASE <c>blocked_process_reports</c> table, not <c>v_blocked_process_reports</c>
    /// (deliberate divergence from the other blocking reads' v_-view twinning): the V7 plan columns are
    /// projected reliably only by the base table. Postgres pins <c>SELECT *</c> in a view to the column
    /// list at view-creation time (V4, before V7 added the plan columns), and V8 only <c>ALTER VIEW … SET
    /// SCHEMA</c>s the view (it never re-creates it) — so on a store upgraded across the V7 boundary the
    /// view would not expose the plan columns and this read would fail. The plan columns are Darling-only
    /// (Lite's DuckDB view has no such column), so a plan-carrying read was never twinnable with Lite
    /// anyway. The shared alert read (<c>DarlingAlertReadAdapter</c>) already reads this base table.</para>
    /// $1 server_id, $2 window start, $3 window end (naive UTC).
    /// </summary>
    public const string BlockedProcessReportsSql = """
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
            monitor_loop,
            blocked_query_plan_xml,
            blocking_query_plan_xml
        FROM blocked_process_reports
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ORDER BY event_time DESC
        LIMIT 200
        """;

    /// <summary>
    /// The always-on DMV blocking-snapshot fallback read for the grid — Lite's
    /// <c>AppendDmvBlockedProcessGridRowsAsync</c> 19-column SELECT ported to Postgres
    /// (dmv_blocking_snapshots carries no report XML). Same parameters as
    /// <see cref="BlockedProcessReportsSql"/>.
    /// </summary>
    public const string DmvBlockingSnapshotsSql = """
        SELECT
            collection_time,
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
            blocked_last_tran_started,
            blocking_last_tran_started,
            monitor_loop,
            blocking_login_name,
            blocking_host_name,
            blocking_client_app
        FROM v_dmv_blocking_snapshots
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   ($4::text[] IS NULL OR database_name = ANY($4))
        ORDER BY event_time DESC
        LIMIT 200
        """;

    /// <summary>
    /// The pair-row read for the block-chain viewer — Lite's <c>GetBlockingPairRowsAsync</c> ported to
    /// Postgres, built from the shared <see cref="PgBlockingPairRowQuery"/> column fragments (the same
    /// source of truth the Darling drill-down + fact collectors use, so the apex and column order can't
    /// drift). Selects the full SQL text (the chain viewer renders it). <see cref="PgBlockingPairRowQuery.SpidFilter"/>
    /// drops the missing-blocker sentinel so no consumer invents a SPID-0 apex.
    /// </summary>
    public const string BlockingPairRowsSql = $"""
        SELECT
            {PgBlockingPairRowQuery.LeadingColumns},
            blocked_sql_text, blocking_sql_text,
            {PgBlockingPairRowQuery.IdentityColumns},
            contentious_object,
            {PgBlockingPairRowQuery.TrailingIdentityColumns}
        FROM v_blocked_process_reports
        WHERE server_id = $1 AND event_time >= $2 AND event_time <= $3
        {PgBlockingPairRowQuery.SpidFilter}
        ORDER BY event_time DESC
        LIMIT 5000
        """;

    /// <summary>
    /// Recent blocked-process events for one server — XE-preferred with the always-on DMV
    /// snapshot as fallback, merged through the shared <see cref="BlockedProcessReportMerge"/>
    /// (keep every XE row; append a DMV row only when no XE row covers the same
    /// blocked/blocker SPID pair within the same minute; re-cap to the 200 newest) — EXACTLY
    /// the alert path's semantics, on the shared row shape.
    /// </summary>
    public async Task<List<ViewerBlockedProcessRow>> GetRecentBlockedProcessReportsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames = null, CancellationToken cancellationToken = default)
    {
        var items = await ReadBlockedProcessRowsAsync(serverId, startUtc, endUtc, databaseNames, cancellationToken);
        var dmvItems = await ReadDmvBlockedProcessRowsAsync(serverId, startUtc, endUtc, databaseNames, cancellationToken);

        BlockedProcessReportMerge.AppendDmvFallbackRows(items, dmvItems);

        return items;
    }

    /// <summary>Maps the full 37-column blocked-process-report read into the widened grid row.</summary>
    private async Task<List<ViewerBlockedProcessRow>> ReadBlockedProcessRowsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerBlockedProcessRow>();

        await using var command = _dataSource.CreateCommand(BlockedProcessReportsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddBlockingParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerBlockedProcessRow
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
                MonitorLoop = reader.IsDBNull(36) ? null : reader.GetInt32(36),
                BlockedQueryPlanXml = reader.IsDBNull(37) ? null : reader.GetString(37),
                BlockingQueryPlanXml = reader.IsDBNull(38) ? null : reader.GetString(38),
                Source = BlockedProcessAlertRow.XeReportSource,
            });
        }

        return rows;
    }

    /// <summary>Maps the 19-column DMV blocking-snapshot fallback read into the widened grid row.</summary>
    private async Task<List<ViewerBlockedProcessRow>> ReadDmvBlockedProcessRowsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, IReadOnlyList<string>? databaseNames, CancellationToken cancellationToken)
    {
        var rows = new List<ViewerBlockedProcessRow>();

        await using var command = _dataSource.CreateCommand(DmvBlockingSnapshotsSql);
        command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
        AddBlockingParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(DatabaseFilterParameter(databaseNames));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new ViewerBlockedProcessRow
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
                MonitorLoop = reader.IsDBNull(18) ? null : reader.GetInt32(18),
                /* Blocking-side identity — the DMV snapshot is the only blocking source on AWS RDS / when the
                   BPR threshold is unset, so surface it here too (the XE path already sets these). */
                BlockingLoginName = reader.IsDBNull(19) ? "" : reader.GetString(19),
                BlockingHostName = reader.IsDBNull(20) ? "" : reader.GetString(20),
                BlockingClientApp = reader.IsDBNull(21) ? "" : reader.GetString(21),
                Source = BlockedProcessAlertRow.DmvSnapshotSource,
            });
        }

        return rows;
    }

    /// <summary>
    /// Fetches the blocked/blocker pair rows for a window, for the block-chain viewer to feed
    /// <see cref="BlockingChainReconstructor"/>. Internal (not public): <see cref="BlockingPairRow"/> is
    /// internal to the analysis assembly — a public method returning it would be CS0050 (mirror of Lite's
    /// <c>GetBlockingPairRowsAsync</c>). Opens one pooled connection so the BPR fetch and the DMV-snapshot
    /// append share it (the append takes a command factory, the Lite shape). Uses
    /// <see cref="PgBlockingPairRowQuery"/> so the viewer agrees with the drill-down + fact collectors on
    /// the apex.
    /// </summary>
    internal async Task<List<BlockingPairRow>> GetBlockingPairRowsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var rows = new List<BlockingPairRow>();

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        await using (var command = connection.CreateCommand())
        {
            command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
            command.CommandText = BlockingPairRowsSql;
            AddBlockingParameters(command, serverId, startUtc, endUtc);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                rows.Add(PgBlockingPairRowQuery.Read(reader));
        }

        // Always-on DMV blocking snapshot: merge in the fallback rows so the viewer works even when the
        // blocked-process-report XE captured nothing (threshold unset / AWS RDS). Same connection.
        /* #2443: the viewer passes its own window token — this read serves a person waiting at a
           grid, not an analysis pass, so there is no budget or service stop for it to abandon under. */
        /* A FACTORY that stamps the deadline, not the bare `connection.CreateCommand` method group
           (#2874). This command is constructed inside PgBlockingPairRowQuery, so a deadline set here
           is the only one it can get - and a method-group hand-off is invisible to both of #2874's
           census regexes, which is how this site survived the sweep of the other 192. */
        await PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
            () =>
            {
                var command = connection.CreateCommand();
                command.CommandTimeout = ViewerCommandDeadlines.InteractiveReadSeconds;
                return command;
            },
            rows, serverId, startUtc, endUtc, cancellationToken);

        return rows;
    }

    /// <summary>The three blocking-window parameters ($1 server_id, $2/$3 naive-UTC window bounds).</summary>
    private static void AddBlockingParameters(NpgsqlCommand command, int serverId, DateTime startUtc, DateTime endUtc)
    {
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified),
        });
        command.Parameters.Add(new NpgsqlParameter<DateTime>
        {
            TypedValue = DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified),
        });
    }

    /// <summary>Lite's wait-time rendering: &lt; 1000 ms as "N ms", else "N.N sec".</summary>
    public static string FormatWaitTime(long waitTimeMs)
        => waitTimeMs < 1000 ? $"{waitTimeMs} ms" : $"{waitTimeMs / 1000.0:F1} sec";
}
