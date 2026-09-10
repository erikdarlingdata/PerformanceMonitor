/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /* The Session Stats reader — the Lite (DuckDB) port of the Darling viewer's
       ViewerDataService.SessionStats read. Reads the server-wide session_summary_stats table (view
       v_session_summary_stats): one aggregate row per collection carrying the status-count breakdown
       that drives the chart's seven series plus the attribution columns (top application / top host /
       distinct databases) the summary strip shows. This is the SUMMARY collector (1:1 with the
       Dashboard's session_stats table) — deliberately NOT the per-application session_stats table
       (v_session_stats) FinOps' Application Connections read uses; the names are distinct so the two
       never collide. collection_time is UTC (GetTimeRange). */

    /// <summary>
    /// The Session Stats trend: every server-wide session-summary snapshot in the window, oldest-first,
    /// so the chart plots the seven status counts over time and the summary strip reads the latest
    /// point's attribution columns.
    /// </summary>
    public async Task<List<SessionStatsPoint>> GetSessionStatsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetSessionStatsAsync", "v_session_summary_stats status-count trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        command.CommandText = @"
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
ORDER BY collection_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<SessionStatsPoint>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new SessionStatsPoint
            {
                CollectionTime = reader.GetDateTime(0),
                TotalSessions = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                RunningSessions = reader.IsDBNull(2) ? 0 : reader.GetInt32(2),
                SleepingSessions = reader.IsDBNull(3) ? 0 : reader.GetInt32(3),
                BackgroundSessions = reader.IsDBNull(4) ? 0 : reader.GetInt32(4),
                DormantSessions = reader.IsDBNull(5) ? 0 : reader.GetInt32(5),
                IdleSessionsOver30Min = reader.IsDBNull(6) ? 0 : reader.GetInt32(6),
                SessionsWaitingForMemory = reader.IsDBNull(7) ? 0 : reader.GetInt32(7),
                DatabasesWithConnections = reader.IsDBNull(8) ? 0 : reader.GetInt32(8),
                TopApplicationName = reader.IsDBNull(9) ? null : reader.GetString(9),
                TopApplicationConnections = reader.IsDBNull(10) ? null : reader.GetInt32(10),
                TopHostName = reader.IsDBNull(11) ? null : reader.GetString(11),
                TopHostConnections = reader.IsDBNull(12) ? null : reader.GetInt32(12)
            });
        }

        return items;
    }
}

/// <summary>One point on the Session Stats trend: a single server-wide session-summary snapshot — the
/// status-count breakdown that drives the chart's seven series plus the attribution columns (top
/// application / top host / distinct databases) the summary panel shows.</summary>
public class SessionStatsPoint : ISessionStatsPoint
{
    public DateTime CollectionTime { get; set; }
    public int TotalSessions { get; set; }
    public int RunningSessions { get; set; }
    public int SleepingSessions { get; set; }
    public int BackgroundSessions { get; set; }
    public int DormantSessions { get; set; }
    public int IdleSessionsOver30Min { get; set; }
    public int SessionsWaitingForMemory { get; set; }
    public int DatabasesWithConnections { get; set; }
    public string? TopApplicationName { get; set; }
    public int? TopApplicationConnections { get; set; }
    public string? TopHostName { get; set; }
    public int? TopHostConnections { get; set; }
}
