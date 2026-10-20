/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.ChangeWatch;

/// <summary>
/// #6234, the PostgreSQL pass: the evaluation of a PostgreSQL target's watch. The same three windows (the after-window from the first
/// change to the window's end, the 60 minutes before it, and the after-window shifted back 7 days), the same thresholds
/// (<see cref="WaitMoved"/>, <see cref="CpuMoved"/>, <see cref="DurationMoved"/>) and the same week-earlier rule as the SQL Server
/// evaluation, over the PostgreSQL readers: wait load from <see cref="DarlingPgWaitReader"/>, CPU from
/// <see cref="DarlingPgCpuUtilizationReader"/>, and statement duration from <see cref="DarlingPgStatementReader"/>.
/// <para>A target that lacks a signal (no CPU source, no wait collection, no statement statistics) reports that measure as NOT
/// MEASURED with the reason, in <see cref="ChangeWatchEvaluation.NotMeasured"/>: absent data is never read as flat.</para>
/// </summary>
public sealed partial class ChangeWatchEvaluator
{
    /// <summary>The statements compared in the before and week-earlier windows: more than the top <see cref="TopQueries"/> of the after-window, so a query that was lighter then is still found.</summary>
    private const int PostgresComparisonRows = 1000;

    private const string EngineSql = "SELECT engine_kind FROM servers WHERE server_id = $1";

    private const string WaitRowsSql = "SELECT count(*) FROM pg_wait_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3";

    private const string StatementRowsSql = "SELECT count(*) FROM pg_statement_stats WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3";

    /// <summary>Whether the registry says this target is PostgreSQL (or Aurora PostgreSQL).</summary>
    private async Task<bool> IsPostgresTargetAsync(int serverId, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(EngineSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        var kind = await command.ExecuteScalarAsync(cancellationToken);
        return MonitoredEngineKind.IsPostgres(kind as string);
    }

    /* A window is closed at its end and opens one microsecond after its start, so a sample on the boundary of two windows counts in one. */
    private static DateTime Open(DateTime start) => start.AddTicks(10);

    private async Task<long> CountRowsAsync(string sql, int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(Open(start)) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), CultureInfo.InvariantCulture);
    }

    private async Task<(double AvgPercent, int Samples)> ReadCpuAsync(int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var samples = await DarlingPgCpuUtilizationReader.GetHistoryAsync(_dataSource, serverId, Open(start), end, cancellationToken);
        return samples.Count == 0 ? (0, 0) : (samples.Average(s => s.CpuPercent), samples.Count);
    }

    private async Task<Dictionary<(long QueryId, long DatabaseId), DarlingPgStatementReader.PgStatementRow>> ReadStatementsAsync(
        int serverId, DateTime start, DateTime end, int limit, CancellationToken cancellationToken)
    {
        var page = await DarlingPgStatementReader.GetPgTopQueriesPageAsync(_dataSource, serverId, Open(start), end, limit, cancellationToken);
        var rows = new Dictionary<(long, long), DarlingPgStatementReader.PgStatementRow>();
        foreach (var row in page.Rows)
        {
            rows[(row.QueryId, row.DatabaseId)] = row;
        }

        return rows;
    }

    private async Task<ChangeWatchEvaluation> EvaluatePostgresAsync(ChangeWatchRow watch, CancellationToken cancellationToken)
    {
        var afterStart = watch.FirstChangeUtc;
        var afterEnd = watch.WindowEndsUtc;
        var beforeStart = afterStart - ChangeWatchStore.Window;
        var weekStart = afterStart.AddDays(-7);
        var weekEnd = afterEnd.AddDays(-7);
        var afterSeconds = Math.Max(1, (afterEnd - afterStart).TotalSeconds);
        var beforeSeconds = Math.Max(1, (afterStart - beforeStart).TotalSeconds);
        var id = watch.ServerId;

        var measures = new List<ChangeWatchMeasure>();
        var notMeasured = new List<ChangeWatchNotMeasured>();
        var notes = new List<string>();
        var hadData = false;

        /* Wait load: the wait collector's window totals (pg_wait_stats). No rows in the after or before window means the target does not
           collect waits (pg_wait_sampling or Performance Insights is missing, or the collector has not run), which is not "no waits". */
        var waitRowsAfter = await CountRowsAsync(WaitRowsSql, id, afterStart, afterEnd, cancellationToken);
        var waitRowsBefore = await CountRowsAsync(WaitRowsSql, id, beforeStart, afterStart, cancellationToken);
        if (waitRowsAfter > 0 && waitRowsBefore > 0)
        {
            hadData = true;
            var after = (await DarlingPgWaitReader.GetPgWaitStatsPageAsync(_dataSource, id, Open(afterStart), afterEnd, 1, cancellationToken)).WindowTotalWaitTimeMs / afterSeconds;
            var before = (await DarlingPgWaitReader.GetPgWaitStatsPageAsync(_dataSource, id, Open(beforeStart), afterStart, 1, cancellationToken)).WindowTotalWaitTimeMs / beforeSeconds;
            double? week = await CountRowsAsync(WaitRowsSql, id, weekStart, weekEnd, cancellationToken) > 0
                ? (await DarlingPgWaitReader.GetPgWaitStatsPageAsync(_dataSource, id, Open(weekStart), weekEnd, 1, cancellationToken)).WindowTotalWaitTimeMs / afterSeconds
                : null;
            measures.Add(new ChangeWatchMeasure("total_wait_ms_per_second", "ms/s", before, after, week,
                WaitMoved(before, after) && (week is null || WaitMoved(week.Value, after))));
        }
        else
        {
            notMeasured.Add(new ChangeWatchNotMeasured("total_wait_ms_per_second",
                $"the wait collector stored no rows for this target in the {(waitRowsAfter == 0 ? "after" : "before")} window (pg_wait_sampling or Performance Insights is not collecting)"));
        }

        /* CPU: only a target with a CPU source (Performance Insights) has samples. */
        var cpuAfter = await ReadCpuAsync(id, afterStart, afterEnd, cancellationToken);
        var cpuBefore = await ReadCpuAsync(id, beforeStart, afterStart, cancellationToken);
        if (cpuAfter.Samples > 0 && cpuBefore.Samples > 0)
        {
            hadData = true;
            var cpuWeek = await ReadCpuAsync(id, weekStart, weekEnd, cancellationToken);
            double? week = cpuWeek.Samples > 0 ? cpuWeek.AvgPercent : null;
            measures.Add(new ChangeWatchMeasure("avg_cpu_percent", "%", cpuBefore.AvgPercent, cpuAfter.AvgPercent, week,
                CpuMoved(cpuBefore.AvgPercent, cpuAfter.AvgPercent) && (week is null || CpuMoved(week.Value, cpuAfter.AvgPercent))));
        }
        else
        {
            notMeasured.Add(new ChangeWatchNotMeasured("avg_cpu_percent",
                $"this target has no CPU samples in the {(cpuAfter.Samples == 0 ? "after" : "before")} window (no CPU source: only a target with Performance Insights supplies one)"));
        }

        /* Statement duration per query id: the top 20 by total execution time in the after-window, each compared with the same query in the
           hour before and the week-earlier window (average duration at least doubled, 10 or more calls in each window compared). */
        var statementRowsAfter = await CountRowsAsync(StatementRowsSql, id, afterStart, afterEnd, cancellationToken);
        var statementRowsBefore = await CountRowsAsync(StatementRowsSql, id, beforeStart, afterStart, cancellationToken);
        if (statementRowsAfter > 0 && statementRowsBefore > 0)
        {
            var afterQueries = await ReadStatementsAsync(id, afterStart, afterEnd, TopQueries, cancellationToken);
            if (afterQueries.Count > 0)
            {
                var beforeQueries = await ReadStatementsAsync(id, beforeStart, afterStart, PostgresComparisonRows, cancellationToken);
                var weekQueries = await ReadStatementsAsync(id, weekStart, weekEnd, PostgresComparisonRows, cancellationToken);
                foreach (var (key, after) in afterQueries)
                {
                    if (!beforeQueries.TryGetValue(key, out var before))
                    {
                        continue;
                    }

                    hadData = true;
                    if (before.Calls < MinExecutions || after.Calls < MinExecutions)
                    {
                        continue;
                    }

                    var beforeAvg = (double)before.TotalExecTimeMs / before.Calls;
                    var afterAvg = (double)after.TotalExecTimeMs / after.Calls;
                    double? weekAvg = weekQueries.TryGetValue(key, out var week) && week.Calls >= MinExecutions ? (double)week.TotalExecTimeMs / week.Calls : null;
                    if (DurationMoved(beforeAvg, afterAvg) && (weekAvg is null || DurationMoved(weekAvg.Value, afterAvg)))
                    {
                        measures.Add(new ChangeWatchMeasure($"query_avg_duration_ms:{key.DatabaseId}:{key.QueryId}", "ms", beforeAvg, afterAvg, weekAvg, true,
                            $"query id {key.QueryId} in database id {key.DatabaseId}; {before.Calls} -> {after.Calls} calls"));
                    }
                }
            }
        }
        else
        {
            notMeasured.Add(new ChangeWatchNotMeasured("query_avg_duration_ms",
                $"the statement collector stored no rows for this target in the {(statementRowsAfter == 0 ? "after" : "before")} window (pg_stat_statements is not installed or not collecting)"));
        }

        foreach (var n in notMeasured)
        {
            notes.Add($"not measured: {n.Measure}: {n.Reason}");
        }

        return new ChangeWatchEvaluation(hadData, measures, DetailText(watch, measures), ResultJson(watch, measures, hadData, notes, notMeasured), notes, notMeasured);
    }
}
