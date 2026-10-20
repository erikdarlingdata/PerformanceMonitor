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
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.ChangeWatch;

/// <summary>One measure's numbers in the three windows, and whether it moved (#6234).</summary>
public sealed record ChangeWatchMeasure(string Name, string Unit, double? Before, double After, double? WeekEarlier, bool Moved, string? Note = null);

/// <summary>A measure the target has no signal for (a PostgreSQL target with no CPU source, no wait collection, no statement statistics) and why (#6234). Never reported as flat.</summary>
public sealed record ChangeWatchNotMeasured(string Measure, string Reason);

/// <summary>What a watch window showed: every measure's numbers, which moved, and the text the investigation request carries.</summary>
public sealed record ChangeWatchEvaluation(
    bool HadData, IReadOnlyList<ChangeWatchMeasure> Measures, string DetailText, string ResultJson, IReadOnlyList<string>? Notes = null,
    IReadOnlyList<ChangeWatchNotMeasured>? NotMeasured = null)
{
    public bool Moved => Measures.Any(m => m.Moved);
}

/// <summary>
/// #6234: compares a watch's after-window with the 60 minutes before its first change, and, when the store has the same window 7 days
/// earlier, with that too. A measure counts as moved only when it moved against BOTH, so the daily load curve does not fire it.
/// <para>The thresholds are this feature's own: the analysis engine for SQL Server targets keeps no threshold for a rise in total wait
/// time per second, average CPU or a query's average duration between two windows, so none was reused.</para>
/// </summary>
public sealed partial class ChangeWatchEvaluator
{
    /// <summary>Total wait time per second must rise by this fraction of the before-window's...</summary>
    public const double WaitRiseFraction = 0.5;

    /// <summary>...and by at least this many milliseconds per second.</summary>
    public const double WaitRiseMinMsPerSecond = 500;

    /// <summary>Average CPU must rise by this many percentage points.</summary>
    public const double CpuRisePoints = 15;

    /// <summary>A query's average duration must grow by this factor...</summary>
    public const double DurationFactor = 2.0;

    /// <summary>...with at least this many executions in each window compared.</summary>
    public const long MinExecutions = 10;

    /// <summary>The queries read for a change that is not a plan change: the top by total duration in the after-window.</summary>
    public const int TopQueries = 20;

    private const string WaitSql = @"
SELECT coalesce(sum(delta_wait_time_ms), 0)::double precision, count(*)
FROM v_wait_stats
WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3 AND delta_wait_time_ms > 0";

    private const string CpuSql = @"
SELECT coalesce(avg(sqlserver_cpu_utilization), 0)::double precision, count(*)
FROM v_cpu_utilization_stats
WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3 AND sqlserver_cpu_utilization IS NOT NULL";

    /* $4 is the query-hash list (text[]) or NULL for the top-N read; $5 the databases (text[]) or NULL; $6 the row cap. */
    private const string QuerySql = @"
SELECT query_hash::text, max(database_name), sum(delta_execution_count)::bigint, sum(delta_elapsed_time)::double precision
FROM v_query_stats
WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3
  AND ($4::text[] IS NULL OR query_hash::text = ANY($4))
  AND ($5::text[] IS NULL OR database_name = ANY($5))
GROUP BY query_hash::text
HAVING sum(delta_execution_count) > 0
ORDER BY sum(delta_elapsed_time) DESC
LIMIT $6";

    /* $1 server, $2 database, $3 query_id, $4/$5 the window on the interval's start. The same dedup the Query Store duration trend runs: an
       interval is snapshotted many times, so each interval counts at its last (cumulative) snapshot. */
    private const string QueryStoreSql = @"
SELECT coalesce(sum(execution_count), 0)::bigint,
       coalesce(sum(execution_count::double precision * avg_duration_us::double precision), 0)::double precision,
       count(*)
FROM (
    SELECT execution_count, avg_duration_us,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                              ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM query_store_stats
    WHERE server_id = $1 AND database_name = $2 AND query_id = $3
      AND interval_start_time_utc >= $4 AND interval_start_time_utc < $5
      AND collection_time >= $4 - interval '1 day' AND collection_time <= $5 + interval '1 day'
) AS x
WHERE rn = 1";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    public ChangeWatchEvaluator(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    private static DateTime Naive(DateTime utc) => DateTime.SpecifyKind(utc, DateTimeKind.Unspecified);

    private NpgsqlCommand Command(NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(start) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        return command;
    }

    /// <summary>The wait rise test: a fraction AND an absolute rate, both against <paramref name="baseline"/>.</summary>
    public static bool WaitMoved(double baseline, double after) =>
        after >= baseline * (1 + WaitRiseFraction) && after - baseline >= WaitRiseMinMsPerSecond;

    /// <summary>The CPU rise test, in percentage points.</summary>
    public static bool CpuMoved(double baseline, double after) => after - baseline >= CpuRisePoints;

    /// <summary>The duration test: the after-window's average is at least <see cref="DurationFactor"/> times the baseline's.</summary>
    public static bool DurationMoved(double baselineAvgUs, double afterAvgUs) => baselineAvgUs > 0 && afterAvgUs >= baselineAvgUs * DurationFactor;

    private async Task<(double Value, long Rows)> ReadScalarAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, sql, serverId, start, end);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetDouble(0), reader.GetInt64(1));
    }

    private async Task<Dictionary<string, (string? Database, long Executions, double ElapsedUs)>> ReadQueriesAsync(
        NpgsqlConnection connection, int serverId, DateTime start, DateTime end, string[]? hashes, string[]? databases, int limit,
        CancellationToken cancellationToken)
    {
        await using var command = Command(connection, QuerySql, serverId, start, end);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = (object?)hashes ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = (object?)databases ?? DBNull.Value });
        command.Parameters.Add(new NpgsqlParameter { Value = limit });
        var rows = new Dictionary<string, (string?, long, double)>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows[reader.GetString(0)] = (reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetInt64(2), reader.GetDouble(3));
        }

        return rows;
    }

    /// <summary>Reads the three windows of <paramref name="watch"/> and decides each measure. The after-window runs from the first change to the window's end.</summary>
    public async Task<ChangeWatchEvaluation> EvaluateAsync(ChangeWatchRow watch, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(watch);
        var afterStart = watch.FirstChangeUtc;
        var afterEnd = watch.WindowEndsUtc;
        var beforeStart = afterStart - ChangeWatchStore.Window;
        var weekStart = afterStart.AddDays(-7);
        var weekEnd = afterEnd.AddDays(-7);
        var afterSeconds = Math.Max(1, (afterEnd - afterStart).TotalSeconds);
        var beforeSeconds = Math.Max(1, (afterStart - beforeStart).TotalSeconds);

        if (await IsPostgresTargetAsync(watch.ServerId, cancellationToken))
        {
            return await EvaluatePostgresAsync(watch, cancellationToken);
        }

        await using var connection = await _dataSource.OpenConnectionAsync(cancellationToken);
        var measures = new List<ChangeWatchMeasure>();
        var hadData = false;

        /* Total wait time per second. */
        var waitAfter = await ReadScalarAsync(connection, WaitSql, watch.ServerId, afterStart, afterEnd, cancellationToken);
        var waitBefore = await ReadScalarAsync(connection, WaitSql, watch.ServerId, beforeStart, afterStart, cancellationToken);
        var waitWeek = await ReadScalarAsync(connection, WaitSql, watch.ServerId, weekStart, weekEnd, cancellationToken);
        if (waitAfter.Rows > 0 && waitBefore.Rows > 0)
        {
            hadData = true;
            var after = waitAfter.Value / afterSeconds;
            var before = waitBefore.Value / beforeSeconds;
            double? week = waitWeek.Rows > 0 ? waitWeek.Value / afterSeconds : null;
            measures.Add(new ChangeWatchMeasure("total_wait_ms_per_second", "ms/s", before, after, week,
                WaitMoved(before, after) && (week is null || WaitMoved(week.Value, after))));
        }

        /* Average CPU. */
        var cpuAfter = await ReadScalarAsync(connection, CpuSql, watch.ServerId, afterStart, afterEnd, cancellationToken);
        var cpuBefore = await ReadScalarAsync(connection, CpuSql, watch.ServerId, beforeStart, afterStart, cancellationToken);
        var cpuWeek = await ReadScalarAsync(connection, CpuSql, watch.ServerId, weekStart, weekEnd, cancellationToken);
        if (cpuAfter.Rows > 0 && cpuBefore.Rows > 0)
        {
            hadData = true;
            double? week = cpuWeek.Rows > 0 ? cpuWeek.Value : null;
            measures.Add(new ChangeWatchMeasure("avg_cpu_percent", "%", cpuBefore.Value, cpuAfter.Value, week,
                CpuMoved(cpuBefore.Value, cpuAfter.Value) && (week is null || CpuMoved(week.Value, cpuAfter.Value))));
        }

        /* Query duration. A watch of plan changes alone measures THAT query (Query Store's own runtime intervals, the source the duration
           trend reads, keyed by database and query_id); where Query Store has no intervals for it in a window, the top 20 by total duration
           of the changed databases are measured instead and the result says so. Any other mix reads the top 20 server-wide. */
        var planDatabases = watch.Changes.Where(c => c.Kind == "plan_force" && c.DatabaseName is not null)
            .Select(c => c.DatabaseName!).Distinct(StringComparer.Ordinal).ToArray();
        var onlyPlan = watch.Changes.Count > 0 && watch.Changes.All(c => c.Kind == "plan_force");
        var scope = onlyPlan && planDatabases.Length > 0 ? planDatabases : null;
        var notes = new List<string>();
        var needTopQueries = true;
        if (onlyPlan)
        {
            var planQueries = watch.Changes.Where(c => c.DatabaseName is not null && c.QueryId is not null)
                .Select(c => (Database: c.DatabaseName!, QueryId: c.QueryId!.Value)).Distinct().ToList();
            if (planQueries.Count > 0)
            {
                needTopQueries = false;
                foreach (var (database, queryId) in planQueries)
                {
                    var queryAfter = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, afterStart, afterEnd, cancellationToken);
                    var queryBefore = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, beforeStart, afterStart, cancellationToken);
                    if (queryAfter.Intervals == 0 || queryBefore.Intervals == 0)
                    {
                        needTopQueries = true;
                        notes.Add($"Query Store has no intervals for query {queryId} in {database} in the {(queryAfter.Intervals == 0 ? "after" : "before")} window, so the top {TopQueries} queries by total duration in the changed databases were measured instead");
                        continue;
                    }

                    hadData = true;
                    if (queryBefore.Executions < MinExecutions || queryAfter.Executions < MinExecutions)
                    {
                        continue;
                    }

                    var queryWeek = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, weekStart, weekEnd, cancellationToken);
                    var queryBeforeAvg = queryBefore.ElapsedUs / queryBefore.Executions;
                    var queryAfterAvg = queryAfter.ElapsedUs / queryAfter.Executions;
                    double? queryWeekAvg = queryWeek.Executions >= MinExecutions ? queryWeek.ElapsedUs / queryWeek.Executions : null;
                    if (DurationMoved(queryBeforeAvg, queryAfterAvg) && (queryWeekAvg is null || DurationMoved(queryWeekAvg.Value, queryAfterAvg)))
                    {
                        measures.Add(new ChangeWatchMeasure($"query_avg_duration_ms:{database}:{queryId}", "ms", queryBeforeAvg / 1000, queryAfterAvg / 1000,
                            queryWeekAvg / 1000, true, $"Query Store query {queryId} in {database}; {queryBefore.Executions} -> {queryAfter.Executions} executions"));
                    }
                }
            }
        }

        var afterQueries = needTopQueries
            ? await ReadQueriesAsync(connection, watch.ServerId, afterStart, afterEnd, null, scope, TopQueries, cancellationToken)
            : new Dictionary<string, (string? Database, long Executions, double ElapsedUs)>(StringComparer.Ordinal);
        if (afterQueries.Count > 0)
        {
            var hashes = afterQueries.Keys.ToArray();
            var beforeQueries = await ReadQueriesAsync(connection, watch.ServerId, beforeStart, afterStart, hashes, null, hashes.Length, cancellationToken);
            var weekQueries = await ReadQueriesAsync(connection, watch.ServerId, weekStart, weekEnd, hashes, null, hashes.Length, cancellationToken);
            foreach (var (hash, after) in afterQueries)
            {
                if (!beforeQueries.TryGetValue(hash, out var before))
                {
                    continue;
                }

                hadData = true;
                if (before.Executions < MinExecutions || after.Executions < MinExecutions)
                {
                    continue;
                }

                var beforeAvg = before.ElapsedUs / before.Executions;
                var afterAvg = after.ElapsedUs / after.Executions;
                double? weekAvg = weekQueries.TryGetValue(hash, out var week) && week.Executions >= MinExecutions ? week.ElapsedUs / week.Executions : null;
                if (DurationMoved(beforeAvg, afterAvg) && (weekAvg is null || DurationMoved(weekAvg.Value, afterAvg)))
                {
                    measures.Add(new ChangeWatchMeasure("query_avg_duration_ms:" + hash, "ms", beforeAvg / 1000, afterAvg / 1000,
                        weekAvg / 1000, true, $"database {after.Database}; {before.Executions} -> {after.Executions} executions"));
                }
            }
        }

        return new ChangeWatchEvaluation(hadData, measures, DetailText(watch, measures), ResultJson(watch, measures, hadData, notes), notes);
    }

    /// <summary>One query's Query Store runtime in a window: executions, total elapsed microseconds, and how many intervals it had.
    /// Each interval counts once, at its final cumulative snapshot, the way the duration trend counts it, and is placed by its start.</summary>
    private async Task<(long Executions, double ElapsedUs, long Intervals)> ReadQueryStoreAsync(
        NpgsqlConnection connection, int serverId, string database, long queryId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(QueryStoreSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(start) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetInt64(0), reader.GetDouble(1), reader.GetInt64(2));
    }

    private static string Num(double? value) => value is { } v ? v.ToString("0.##", CultureInfo.InvariantCulture) : "n/a";

    /// <summary>The text the request's alert row carries: the changes, the window and every measure that moved with its three numbers. Held under the brief's 1,000-character field cap.</summary>
    internal static string DetailText(ChangeWatchRow watch, IReadOnlyList<ChangeWatchMeasure> measures)
    {
        var text = new StringBuilder();
        text.Append("After a recorded change (watch ").Append(watch.FirstChangeUtc.ToString("yyyy-MM-dd HH:mm", CultureInfo.InvariantCulture))
            .Append(" to ").Append(watch.WindowEndsUtc.ToString("HH:mm", CultureInfo.InvariantCulture)).Append(" UTC): ");
        foreach (var change in watch.Changes.Take(5))
        {
            text.Append(change.ChangeTimeUtc.ToString("HH:mm", CultureInfo.InvariantCulture)).Append(' ').Append(change.Description).Append("; ");
        }

        if (watch.Changes.Count > 5)
        {
            text.Append('+').Append(watch.Changes.Count - 5).Append(" more; ");
        }

        foreach (var m in measures.Where(m => m.Moved).Take(5))
        {
            text.Append(m.Name).Append(" before ").Append(Num(m.Before)).Append(" after ").Append(Num(m.After))
                .Append(" week earlier ").Append(Num(m.WeekEarlier)).Append(' ').Append(m.Unit).Append("; ");
        }

        var result = text.ToString().TrimEnd(' ', ';');
        return result.Length <= 990 ? result : result[..987] + "...";
    }

    internal static string ResultJson(
        ChangeWatchRow watch, IReadOnlyList<ChangeWatchMeasure> measures, bool hadData, IReadOnlyList<string>? notes = null,
        IReadOnlyList<ChangeWatchNotMeasured>? notMeasured = null) =>
        JsonSerializer.Serialize(new
        {
            had_data = hadData,
            notes = notes ?? [],
            not_measured = (notMeasured ?? []).Select(n => new { measure = n.Measure, reason = n.Reason }),
            window = new { before_start = watch.FirstChangeUtc - ChangeWatchStore.Window, after_start = watch.FirstChangeUtc, after_end = watch.WindowEndsUtc },
            changes = watch.Changes.Select(c => new { time = c.ChangeTimeUtc, kind = c.Kind, what = c.Description }),
            measures = measures.Select(m => new { measure = m.Name, unit = m.Unit, before = m.Before, after = m.After, week_earlier = m.WeekEarlier, moved = m.Moved, note = m.Note }),
        });
}
