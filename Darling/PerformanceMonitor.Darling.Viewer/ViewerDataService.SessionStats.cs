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
    int? TopHostConnections,
    DateTime? LatestCollectionTime = null) : ISessionStatsPoint;

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
    public static readonly string SessionStatsSql = ServerTrendSql.SessionSummary;

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
            string? TopAppName, int? TopAppConnections, string? TopHostName, int? TopHostConnections, DateTime LatestCollectionTime)>();
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
                reader.IsDBNull(12) ? null : reader.GetInt32(12),
                reader.GetDateTime(15)));
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
                row.TopHostConnections,
                row.LatestCollectionTime));
        }

        return result;
    }
}
