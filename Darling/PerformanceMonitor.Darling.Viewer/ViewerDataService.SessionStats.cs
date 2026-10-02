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
using PerformanceMonitor.Darling.Storage;

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
    /// bucketed to <see cref="TrendBudget.Chart"/>'s point budget (#4234), so the chart plots the seven
    /// status counts over time and the summary panel reads the latest bucket's attribution columns (the
    /// same shape the Dashboard's <c>GetSessionStatsAsync</c> returns). Runs on the
    /// <c>v_session_summary_stats</c> passthrough view; both bounds are inclusive (<c>&gt;= $2 AND
    /// &lt;= $3</c>, naive UTC via <see cref="AddWindowParameters"/>). 2,016 rows over 7 days at the
    /// collector's 5-minute cadence, with no cap, is the issue's own measured number.
    /// <para>#4234: the eight status/count columns are gauges (ruling item 2) — a bucket's value is the
    /// plain average of its collections. The four attribution columns (top application/host name and
    /// their connection counts) cannot be averaged, so <c>latest</c> carries forward the LAST physical
    /// collection's own values inside each bucket (<c>DISTINCT ON</c>, newest first) rather than any
    /// blended or dropped answer — matching what the pre-bucket read's own newest row inside a would-be
    /// bucket already showed. <c>first_collection_time</c>/<c>collection_count</c> let the C# reader stamp
    /// a bucket that merged nothing at its one collection's own raw time (ruling item 3). $4 the bucket
    /// width in minutes.</para>
    /// </summary>
    public static readonly string SessionStatsSql = $"""
        WITH raw AS
        (
            SELECT
                collection_time,
                GREATEST(date_bin(CAST($4 AS integer) * INTERVAL '1 minute', collection_time, {TrendBucketSql.OriginSql}), $2) AS bucket_start,
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
            SELECT DISTINCT ON (bucket_start)
                bucket_start,
                top_application_name,
                top_application_connections,
                top_host_name,
                top_host_connections
            FROM raw
            ORDER BY bucket_start, collection_time DESC
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
        ORDER BY agg.bucket_start
        """;

    /// <summary>
    /// The server-wide session-summary trend over the window, bucketed to <see cref="TrendBudget.Chart"/>'s
    /// point budget (#4234). A bucket holding exactly one physical collection is stamped at that
    /// collection's own raw time rather than the bucket grid when EVERY bucket this call returned is such
    /// a singleton (ruling item 3).
    /// </summary>
    public async Task<List<SessionStatsPoint>> GetSessionStatsAsync(
        int serverId, DateTime startUtc, DateTime endUtc, CancellationToken cancellationToken = default)
    {
        var windowMinutes = Math.Max(1, (int)Math.Ceiling((endUtc - startUtc).TotalMinutes));
        var bucketMinutes = TrendBuckets.AutoMinutes(windowMinutes, 1, TrendBudget.Chart.AutoPoints);

        await using var command = _dataSource.CreateCommand(SessionStatsSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        AddWindowParameters(command, serverId, startUtc, endUtc);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = bucketMinutes });

        var rows = new List<(DateTime BucketStart, DateTime FirstCollectionTime, int Total, int Running, int Sleeping,
            int Background, int Dormant, int Idle, int WaitingForMemory, int DatabasesWithConnections,
            string? TopAppName, int? TopAppConnections, string? TopHostName, int? TopHostConnections)>();
        var everyBucketSingleton = true;

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            if (reader.GetInt64(14) != 1)
            {
                everyBucketSingleton = false;
            }

            rows.Add((
                reader.GetDateTime(0),
                reader.GetDateTime(13),
                reader.IsDBNull(1) ? 0 : (int)Math.Round(reader.GetDouble(1)),
                reader.IsDBNull(2) ? 0 : (int)Math.Round(reader.GetDouble(2)),
                reader.IsDBNull(3) ? 0 : (int)Math.Round(reader.GetDouble(3)),
                reader.IsDBNull(4) ? 0 : (int)Math.Round(reader.GetDouble(4)),
                reader.IsDBNull(5) ? 0 : (int)Math.Round(reader.GetDouble(5)),
                reader.IsDBNull(6) ? 0 : (int)Math.Round(reader.GetDouble(6)),
                reader.IsDBNull(7) ? 0 : (int)Math.Round(reader.GetDouble(7)),
                reader.IsDBNull(8) ? 0 : (int)Math.Round(reader.GetDouble(8)),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetInt32(10),
                reader.IsDBNull(11) ? null : reader.GetString(11),
                reader.IsDBNull(12) ? null : reader.GetInt32(12)));
        }

        var result = new List<SessionStatsPoint>(rows.Count);
        foreach (var row in rows)
        {
            result.Add(new SessionStatsPoint(
                everyBucketSingleton ? row.FirstCollectionTime : row.BucketStart,
                row.Total,
                row.Running,
                row.Sleeping,
                row.Background,
                row.Dormant,
                row.Idle,
                row.WaitingForMemory,
                row.DatabasesWithConnections,
                row.TopAppName,
                row.TopAppConnections,
                row.TopHostName,
                row.TopHostConnections));
        }

        return result;
    }
}
