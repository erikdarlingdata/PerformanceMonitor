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
    /// The Session Stats trend: every server-wide session-summary snapshot in the window, bucketed to
    /// <see cref="TrendBudget.Chart"/>'s point budget (#4234), so the chart plots the seven status counts
    /// over time and the summary strip reads the latest bucket's attribution columns. A bucket holding
    /// exactly one physical collection is stamped at that collection's own raw time rather than the
    /// bucket grid when EVERY bucket this call returned is such a singleton (ruling item 3). 2,016 rows
    /// over 7 days at the collector's 5-minute cadence, with no cap, is the issue's own measured number.
    /// </summary>
    public async Task<List<SessionStatsPoint>> GetSessionStatsAsync(int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var _q = TimeQuery("GetSessionStatsAsync", "v_session_summary_stats status-count trend");
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc: null, SelectedServerTabUtcOffsetMinutes);

        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endTime - startTime).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        command.CommandText = SessionStatsTrendSql;

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });
        command.Parameters.Add(new DuckDBParameter { Value = bucketMinutes });

        var rows = new List<(DateTime BucketStart, int Total, int Running, int Sleeping, int Background, int Dormant,
            int Idle, int WaitingForMemory, int DatabasesWithConnections, string? TopAppName, int? TopAppConnections,
            string? TopHostName, int? TopHostConnections, DateTime FirstCollectionTime, long CollectionCount)>();
        var everyBucketSingleton = true;

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var collectionCount = ToInt64(reader.GetValue(14));
            if (collectionCount != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.IsDBNull(1) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(1))),
                reader.IsDBNull(2) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(2))),
                reader.IsDBNull(3) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(3))),
                reader.IsDBNull(4) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(4))),
                reader.IsDBNull(5) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(5))),
                reader.IsDBNull(6) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(6))),
                reader.IsDBNull(7) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(7))),
                reader.IsDBNull(8) ? 0 : (int)Math.Round(ToDouble(reader.GetValue(8))),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetInt32(12),
                reader.GetDateTime(13),
                collectionCount));
        }

        var items = new List<SessionStatsPoint>(rows.Count);
        foreach (var row in rows)
        {
            items.Add(new SessionStatsPoint
            {
                CollectionTime = everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                TotalSessions = row.Total,
                RunningSessions = row.Running,
                SleepingSessions = row.Sleeping,
                BackgroundSessions = row.Background,
                DormantSessions = row.Dormant,
                IdleSessionsOver30Min = row.Idle,
                SessionsWaitingForMemory = row.WaitingForMemory,
                DatabasesWithConnections = row.DatabasesWithConnections,
                TopApplicationName = row.TopAppName,
                TopApplicationConnections = row.TopAppConnections,
                TopHostName = row.TopHostName,
                TopHostConnections = row.TopHostConnections
            });
        }

        return items;
    }

    /// <summary>
    /// The bucketed session-stats trend statement text (#4234), pulled out of
    /// <see cref="GetSessionStatsAsync"/> so its shape is checkable without a live DuckDB. $1 server_id,
    /// $2/$3 the UTC window (also the GREATEST clamp so the first bucket never renders earlier than the
    /// window), $4 the bucket width in minutes. The eight status/count columns are gauges, averaged per
    /// bucket; the four attribution columns cannot be averaged, so <c>latest</c> carries forward the LAST
    /// physical collection's own values inside each bucket (DuckDB's <c>QUALIFY ROW_NUMBER()</c>, newest
    /// first) instead of any blended or dropped answer.
    /// </summary>
    internal static string SessionStatsTrendSql => $@"
WITH raw AS
(
    SELECT
        collection_time,
        GREATEST(time_bucket(to_minutes(CAST($4 AS INTEGER)), collection_time, {TrendBuckets.OriginSql}), $2) AS bucket_start,
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
),
agg AS
(
    SELECT
        bucket_start,
        AVG(COALESCE(total_sessions, 0)) AS total_sessions,
        AVG(COALESCE(running_sessions, 0)) AS running_sessions,
        AVG(COALESCE(sleeping_sessions, 0)) AS sleeping_sessions,
        AVG(COALESCE(background_sessions, 0)) AS background_sessions,
        AVG(COALESCE(dormant_sessions, 0)) AS dormant_sessions,
        AVG(COALESCE(idle_sessions_over_30min, 0)) AS idle_sessions_over_30min,
        AVG(COALESCE(sessions_waiting_for_memory, 0)) AS sessions_waiting_for_memory,
        AVG(COALESCE(databases_with_connections, 0)) AS databases_with_connections,
        MIN(collection_time) AS first_collection_time,
        COUNT(*) AS collection_count
    FROM raw
    GROUP BY bucket_start
),
latest AS
(
    SELECT
        bucket_start,
        top_application_name,
        top_application_connections,
        top_host_name,
        top_host_connections
    FROM raw
    QUALIFY ROW_NUMBER() OVER (PARTITION BY bucket_start ORDER BY collection_time DESC) = 1
)
SELECT
    agg.bucket_start,
    agg.total_sessions,
    agg.running_sessions,
    agg.sleeping_sessions,
    agg.background_sessions,
    agg.dormant_sessions,
    agg.idle_sessions_over_30min,
    agg.sessions_waiting_for_memory,
    agg.databases_with_connections,
    latest.top_application_name,
    latest.top_application_connections,
    latest.top_host_name,
    latest.top_host_connections,
    agg.first_collection_time,
    agg.collection_count
FROM agg
JOIN latest ON latest.bucket_start = agg.bucket_start
ORDER BY agg.bucket_start";
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
