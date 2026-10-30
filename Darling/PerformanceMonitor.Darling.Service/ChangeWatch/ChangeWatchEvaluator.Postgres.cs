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
/// evaluation, over the PostgreSQL readers: wait load from <c>pg_wait_stats</c> (Aurora) or <see cref="DarlingPgWaitSamplingReader"/> (pg_wait_sampling), CPU from
/// <see cref="DarlingPgCpuUtilizationReader"/>, and statement duration from <see cref="DarlingPgStatementReader"/>.
/// <para>A target that lacks a signal (no CPU source, no wait collection, no statement statistics) reports that measure as NOT
/// MEASURED with the reason, in <see cref="ChangeWatchEvaluation.NotMeasured"/>: absent data is never read as flat.</para>
/// </summary>
public sealed partial class ChangeWatchEvaluator
{
    /// <summary>The statements compared in the before and week-earlier windows: more than the top <see cref="TopQueries"/> of the after-window, so a query that was lighter then is still found.</summary>
    private const int PostgresComparisonRows = 1000;

    private const string EngineSql = "SELECT engine_kind FROM servers WHERE server_id = $1";

    /// <summary>The most (event type, event, query) series read from the sampled profile for one window: far more than a server holds.</summary>
    private const int PostgresWaitSamplingRows = 100000;

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

    /* The Aurora wait collector's wait time and the seconds it covered in a window, and how many collections it holds (#6234): one interval per
       collection_time (every wait row of a collection carries the same sample_interval_seconds; 0 means no delta was knowable, NULL a legacy
       row that falls back to the gap since the previous collection). A collection whose interval began before the window's start counts only
       the part inside the window: its seconds are clipped to the time since the start and its wait time is scaled by the same share, so a
       collection after a long gap does not carry the gap's waits into the window or inflate its coverage. Columns: wait time in ms (positive
       deltas only), seconds covered, collections. */
    private const string PgWaitStatsSql = @"
WITH c AS (
    SELECT collection_time, max(sample_interval_seconds) AS stored_seconds,
           sum(CASE WHEN delta_wait_time_us > 0 THEN delta_wait_time_us ELSE 0 END)::double precision / 1000.0 AS wait_ms
    FROM pg_wait_stats
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY collection_time
), d AS (
    SELECT collection_time, wait_ms,
           coalesce(stored_seconds::double precision,
                    extract(epoch FROM collection_time - lag(collection_time) OVER (ORDER BY collection_time))::double precision) AS seconds
    FROM c
), e AS (
    SELECT wait_ms * f.share AS wait_ms, seconds * f.share AS seconds
    FROM d
    CROSS JOIN LATERAL (SELECT CASE WHEN seconds > 0
                                    THEN least(1.0, extract(epoch FROM collection_time - $2)::double precision / seconds)
                                    ELSE 1.0 END AS share) f
)
SELECT coalesce(sum(wait_ms) FILTER (WHERE seconds > 0), 0)::double precision,
       coalesce(sum(seconds) FILTER (WHERE seconds > 0), 0)::double precision,
       count(*)::bigint
FROM e";

    /* The pg_wait_sampling profile is cumulative and DarlingPgWaitSamplingReader differences its newest reading against its oldest in the
       window, so the figure covers the span between the window's first and last collection. The time the profile was OBSERVED over that span is
       each later collection's sampled_ms (the service-sampled arm watches only part of each cycle), or the gap since the previous collection
       where sampled_ms is NULL (the extension arm samples in-engine the whole time, and any row before V133). Columns: seconds observed, span
       seconds, collections. */
    private const string PgWaitSamplingCoverageSql = @"
WITH c AS (
    SELECT collection_time, max(sampled_ms) AS sampled_ms
    FROM pg_wait_sampling
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3
    GROUP BY collection_time
), d AS (
    SELECT collection_time, sampled_ms,
           extract(epoch FROM collection_time - lag(collection_time) OVER (ORDER BY collection_time))::double precision AS gap_seconds
    FROM c
)
SELECT coalesce(sum(coalesce(sampled_ms / 1000.0, gap_seconds)) FILTER (WHERE gap_seconds IS NOT NULL), 0)::double precision,
       coalesce(extract(epoch FROM max(collection_time) - min(collection_time)), 0)::double precision,
       count(*)::bigint
FROM d";

    private async Task<(double First, double Second, long Rows)> ReadCoverageAsync(
        string sql, int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(Open(start)) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetDouble(0), reader.GetDouble(1), reader.GetInt64(2));
    }

    /// <summary>Total wait time per second from the Aurora wait collector (<c>pg_wait_stats</c>), over the part of each collection that lies inside the window.</summary>
    private async Task<RateSample> ReadPgWaitStatsRateAsync(int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var (totalMs, seconds, rows) = await ReadCoverageAsync(PgWaitStatsSql, serverId, start, end, cancellationToken);
        if (rows == 0)
        {
            return default;
        }

        return new RateSample(seconds > 0 ? totalMs / seconds : 0, seconds, rows);
    }

    /// <summary>Total wait time per second from the sampled profile (<c>pg_wait_sampling</c>), through the reader <c>get_pg_wait_sampling</c> uses.
    /// Time on the CPU is not waiting, so the <c>CPU</c> event type is left out.</summary>
    private async Task<RateSample> ReadPgWaitSamplingRateAsync(int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        var coverage = await ReadCoverageAsync(PgWaitSamplingCoverageSql, serverId, start, end, cancellationToken);
        if (coverage.Rows == 0)
        {
            return default;
        }

        var page = await DarlingPgWaitSamplingReader.GetPgWaitSamplingPageAsync(_dataSource, serverId, Open(start), end, PostgresWaitSamplingRows, cancellationToken);
        var waitedMs = page.Rows.Where(r => !string.Equals(r.EventType, "CPU", StringComparison.OrdinalIgnoreCase)).Sum(r => (double)r.EstimatedWaitMs);
        return new RateSample(coverage.First > 0 ? waitedMs / coverage.First : 0, coverage.Second, coverage.Rows);
    }

    private async Task<RateSample> ReadCpuAsync(int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        /* The reader filters on collection_time, and a sample is collected after it was taken (the collector runs every few minutes, and a backlog
           after a gap arrives later still), so the scan runs a day past the window's end, as the SQL Server read does, and the sample's own
           instant decides below (#6234). */
        var all = await DarlingPgCpuUtilizationReader.GetHistoryAsync(_dataSource, serverId, Open(start), end.AddDays(1), cancellationToken);
        /* A sample counts in the window its own instant falls in: a backlog ingested after a gap carries the later collection_time. */
        var samples = all.Where(s => s.SampleTimeUtc > start && s.SampleTimeUtc <= end).ToList();
        if (samples.Count == 0)
        {
            return default;
        }

        var minutes = samples.Select(s => s.SampleTimeUtc.Ticks / TimeSpan.TicksPerMinute).Distinct().Count();
        return new RateSample(samples.Average(s => s.CpuPercent), minutes * 60.0, samples.Count);
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

        /* Wait load: pg_wait_stats where the target is an Aurora one (Performance Insights), pg_wait_sampling where it is stock PostgreSQL with
           the extension. Whichever source holds rows for this target is the one read; with neither, the reason names both. A window with no
           rows or too little coverage is not "no waits". */
        var waitAfter = await ReadPgWaitStatsRateAsync(id, afterStart, afterEnd, cancellationToken);
        var waitBefore = await ReadPgWaitStatsRateAsync(id, beforeStart, afterStart, cancellationToken);
        RateSample waitWeek;
        string waitSource;
        if (waitAfter.Rows > 0 || waitBefore.Rows > 0)
        {
            waitSource = "the Aurora wait collector (pg_wait_stats)";
            waitWeek = await ReadPgWaitStatsRateAsync(id, weekStart, weekEnd, cancellationToken);
        }
        else
        {
            waitAfter = await ReadPgWaitSamplingRateAsync(id, afterStart, afterEnd, cancellationToken);
            waitBefore = await ReadPgWaitSamplingRateAsync(id, beforeStart, afterStart, cancellationToken);
            waitWeek = await ReadPgWaitSamplingRateAsync(id, weekStart, weekEnd, cancellationToken);
            waitSource = waitAfter.Rows > 0 || waitBefore.Rows > 0 ? "the pg_wait_sampling collector" : string.Empty;
        }

        hadData |= AddRateMeasure("total_wait_ms_per_second", "ms/s", waitAfter, afterSeconds, waitBefore, beforeSeconds, waitWeek, afterSeconds,
            WaitMoved,
            window => waitSource.Length > 0
                ? $"{waitSource} stored no rows for this target in the {window} window"
                : $"no wait source stored rows for this target in the {window} window: pg_wait_stats (the Aurora wait collector) and pg_wait_sampling (the extension's collector) are both empty, so the extension is not installed or not collecting",
            measures, notMeasured, notes);

        /* CPU: only a target with a CPU source (Performance Insights) has samples. */
        var cpuAfter = await ReadCpuAsync(id, afterStart, afterEnd, cancellationToken);
        var cpuBefore = await ReadCpuAsync(id, beforeStart, afterStart, cancellationToken);
        var cpuWeek = await ReadCpuAsync(id, weekStart, weekEnd, cancellationToken);
        hadData |= AddRateMeasure("avg_cpu_percent", "%", cpuAfter, afterSeconds, cpuBefore, beforeSeconds, cpuWeek, afterSeconds,
            CpuMoved,
            window => $"this target has no CPU samples in the {window} window (no CPU source: only a target with Performance Insights supplies one)",
            measures, notMeasured, notes);

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
                var missingBefore = 0;
                var belowFloor = 0;
                foreach (var (key, after) in afterQueries)
                {
                    if (!beforeQueries.TryGetValue(key, out var before))
                    {
                        missingBefore++;
                        continue;
                    }

                    hadData = true;
                    if (before.Calls < MinExecutions || after.Calls < MinExecutions)
                    {
                        belowFloor++;
                        continue;
                    }

                    var beforeAvg = (double)before.TotalExecTimeMs / before.Calls;
                    var afterAvg = (double)after.TotalExecTimeMs / after.Calls;
                    double? weekAvg = weekQueries.TryGetValue(key, out var week) && week.Calls >= MinExecutions ? (double)week.TotalExecTimeMs / week.Calls : null;
                    if (DurationMoved(beforeAvg, afterAvg) && (weekAvg is null || DurationMoved(weekAvg.Value, afterAvg)))
                    {
                        measures.Add(new ChangeWatchMeasure($"query_avg_duration_ms:{key.DatabaseId}:{key.QueryId}", "ms", beforeAvg, afterAvg, weekAvg, true,
                            $"read from pg_stat_statements: query id {key.QueryId} in database id {key.DatabaseId}; {before.Calls} -> {after.Calls} calls"));
                    }
                }

                /* pg_stat_statements_reset() or a restart empties the before window's statements: every top query is then "new". */
                AddQuerySkips(notMeasured, afterQueries.Count, missingBefore, belowFloor, "pg_stat_statements");
            }
            else
            {
                notMeasured.Add(new ChangeWatchNotMeasured("query_avg_duration_ms", "no statement with calls was recorded in the after window"));
            }
        }
        else
        {
            notMeasured.Add(new ChangeWatchNotMeasured("query_avg_duration_ms",
                $"the statement collector stored no rows for this target in the {(statementRowsAfter == 0 ? "after" : "before")} window (pg_stat_statements is not installed or not collecting)"));
        }

        return Complete(watch, measures, hadData, notes, notMeasured);
    }
}
