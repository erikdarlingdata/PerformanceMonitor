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
/// One point on the Session Stats trend: a single server-wide session-summary snapshot from
/// <c>session_summary_stats</c> — the status-count breakdown that drives the chart's seven series plus the
/// attribution columns (top application / top host / distinct databases) the summary panel shows.
/// </summary>
/// <remarks>
/// <b>Source note (verified).</b> This is the server-wide <c>session_summary_stats</c> table (view
/// <c>v_session_summary_stats</c>), the <see cref="PerformanceMonitor.Collectors.SessionSummaryStatsCollector"/>
/// that is the 1:1 Dashboard-parity port of <c>install/36_collect_session_stats.sql</c> — one aggregate row
/// per collection with all twelve columns the Dashboard's <c>SessionStatsItem</c> carries. It is deliberately
/// NOT the per-application <c>session_stats</c> table (view <c>v_session_stats</c>, one row per
/// <c>program_name</c>) that FinOps' Application Connections read uses — that table has no
/// background/idle/waiting-for-memory/host/database columns, so it cannot feed the Dashboard's chart or
/// summary. The names are intentionally distinct so the two never collide.
/// </remarks>
public sealed record SessionStatsPoint(
    DateTime CollectionTime,
    int TotalSessions,
    int RunningSessions,
    int SleepingSessions,
    int BackgroundSessions,
    int DormantSessions,
    int IdleSessionsOver30Min,
    int SessionsWaitingForMemory,
    int DatabasesWithConnections,
    string? TopApplicationName,
    int? TopApplicationConnections,
    string? TopHostName,
    int? TopHostConnections) : ISessionStatsPoint;

public sealed partial class ViewerDataService
{
    /// <summary>
    /// The Session Stats trend read: every server-wide session-summary snapshot in the settable window,
    /// ordered oldest-first, so the chart plots the seven status counts over time and the summary panel
    /// reads the latest point's attribution columns (the same shape the Dashboard's
    /// <c>GetSessionStatsAsync</c> returns). Runs on the <c>v_session_summary_stats</c> passthrough view;
    /// both bounds are inclusive (<c>&gt;= $2 AND &lt;= $3</c>, naive UTC via
    /// <see cref="AddWindowParameters"/>). $1 server_id, $2 window start, $3 window end.
    /// </summary>
    public const string SessionStatsSql = """
        SELECT
            collection_time,
            total_sessions,
            running_sessions,
            sleeping_sessions,
            background_sessions,
            dormant_sessions,
            idle_sessions_over_30min,
            sessions_waiting_for_memory,
            databases_with_connections,
            top_application_name,
            top_application_connections,
            top_host_name,
            top_host_connections
        FROM v_session_summary_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        ORDER BY collection_time
        """;

    /// <summary>The server-wide session-summary snapshots over the window, oldest-first (empty when none).</summary>
    public async Task<List<SessionStatsPoint>> GetSessionStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var result = new List<SessionStatsPoint>();

        await using var command = _dataSource.CreateCommand(SessionStatsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            result.Add(new SessionStatsPoint(
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetInt32(12)));
        }

        return result;
    }
}
