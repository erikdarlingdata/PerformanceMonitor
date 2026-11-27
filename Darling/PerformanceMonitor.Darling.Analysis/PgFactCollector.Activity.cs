/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /* #5320: no statement text. The fact is keyed on the query hash and carries only numbers, so the read used to
       fetch a 200-character cut of the text for nothing, and a cut text read here would have been judged as a prefix.
       The text is read, judged whole and then cut where an answer prints it (the BAD_ACTOR drill-down).
       #8403: every row of the window counts, including a row whose execution delta is 0 while its CPU and reads
       deltas are not. The store holds such rows, and the BAD_ACTOR drill-down and the totals already sum them, so
       the fact sums the same rows and the finding's headline average and the drill-down beside it describe the
       same work. The execution bar in HAVING still gates on the execution sum, and the ordering guards its divisor. */
    public const string BadActorSql = @"
SELECT
    database_name,
    query_hash,
    SUM(delta_execution_count)::BIGINT AS exec_count,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_worker_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_elapsed_time)::DOUBLE PRECISION / SUM(delta_execution_count) / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN SUM(delta_execution_count) > 0
         THEN SUM(delta_logical_reads)::DOUBLE PRECISION / SUM(delta_execution_count)
         ELSE 0 END AS avg_reads,
    SUM(delta_worker_time)::BIGINT AS total_cpu_us,
    SUM(delta_logical_reads)::BIGINT AS total_reads,
    SUM(delta_spills)::BIGINT AS total_spills,
    MAX(max_dop) AS max_dop
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
GROUP BY database_name, query_hash
HAVING SUM(delta_execution_count) >= 100
ORDER BY SUM(delta_worker_time)::DOUBLE PRECISION / GREATEST(SUM(delta_execution_count), 1) *
         LN(GREATEST(SUM(delta_execution_count), 1)) DESC
LIMIT 5";

    /// <summary>
    /// #9879: <see cref="BadActorSql"/>'s ranking and figures, with the whole hours before the window's last UTC day read
    /// from the hourly rollup (<c>collect.query_stats_io_hourly</c>) instead of the raw rows. $1 server, $2 window start,
    /// $3 window end, $4 the first hour line at or after the start, $5 the cut (an hour line, see
    /// <see cref="ChooseBadActorRead"/>). The four arms hold each raw row of the closed window [$2, $3] exactly once:
    /// <list type="number">
    /// <item>raw rows in [$2, $4): the part hour before the first hour line. The first arm does NOT own $4.</item>
    /// <item>rollup buckets in [$4, $5): the rollup arm owns $4 and does not own $5. A bucket holds every raw row of
    /// its hour whose <c>sample_interval_seconds</c> is not 0.</item>
    /// <item>raw rows in [$4, $5) whose <c>sample_interval_seconds</c> is 0: the rows the rollup leaves out. The
    /// collector stores a 0 interval beside all-zero deltas except when two passes land under one second apart, and
    /// this arm keeps the sums equal to the raw read's in that case too.</item>
    /// <item>raw rows in [$5, $3]: the last raw arm owns $5 and the closed window end.</item>
    /// </list>
    /// The four sums rank exactly as <see cref="BadActorSql"/> ranks them. The rollup has no spills or DOP column, so
    /// those two figures come from the raw rows of the five ranked queries, over the same closed window.
    /// </summary>
    public const string BadActorHourlySplitSql = @"
WITH arms AS
(
    SELECT
        database_name,
        query_hash,
        delta_execution_count AS executions,
        delta_worker_time AS worker_time,
        delta_elapsed_time AS elapsed_time,
        delta_logical_reads AS logical_reads
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time < $4
    UNION ALL
    SELECT
        database_name,
        query_hash,
        execution_count_sum,
        worker_time_sum,
        elapsed_time_sum,
        logical_reads_sum
    FROM collect.query_stats_io_hourly
    WHERE server_id = $1
    AND   bucket >= $4
    AND   bucket < $5
    UNION ALL
    SELECT
        database_name,
        query_hash,
        delta_execution_count,
        delta_worker_time,
        delta_elapsed_time,
        delta_logical_reads
    FROM v_query_stats
    WHERE server_id = $1
    AND   sample_interval_seconds = 0
    AND   collection_time >= $4
    AND   collection_time < $5
    UNION ALL
    SELECT
        database_name,
        query_hash,
        delta_execution_count,
        delta_worker_time,
        delta_elapsed_time,
        delta_logical_reads
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $5
    AND   collection_time <= $3
),
ranked AS
(
    SELECT
        database_name,
        query_hash,
        SUM(executions) AS executions,
        SUM(worker_time) AS worker_time,
        SUM(elapsed_time) AS elapsed_time,
        SUM(logical_reads) AS logical_reads
    FROM arms
    GROUP BY database_name, query_hash
    HAVING SUM(executions) >= 100
    ORDER BY SUM(worker_time)::DOUBLE PRECISION / GREATEST(SUM(executions), 1) *
             LN(GREATEST(SUM(executions), 1)) DESC
    LIMIT 5
)
SELECT
    r.database_name,
    r.query_hash,
    r.executions::BIGINT AS exec_count,
    CASE WHEN r.executions > 0
         THEN r.worker_time::DOUBLE PRECISION / r.executions / 1000.0
         ELSE 0 END AS avg_cpu_ms,
    CASE WHEN r.executions > 0
         THEN r.elapsed_time::DOUBLE PRECISION / r.executions / 1000.0
         ELSE 0 END AS avg_elapsed_ms,
    CASE WHEN r.executions > 0
         THEN r.logical_reads::DOUBLE PRECISION / r.executions
         ELSE 0 END AS avg_reads,
    r.worker_time::BIGINT AS total_cpu_us,
    r.logical_reads::BIGINT AS total_reads,
    d.total_spills,
    d.max_dop
FROM ranked AS r
LEFT JOIN LATERAL
(
    SELECT
        SUM(x.delta_spills)::BIGINT AS total_spills,
        MAX(x.max_dop) AS max_dop
    FROM
    (
        SELECT qs.delta_spills, qs.max_dop
        FROM v_query_stats AS qs
        WHERE qs.server_id = $1
        AND   qs.query_hash = r.query_hash
        AND   qs.database_name IS NOT DISTINCT FROM r.database_name
        AND   qs.collection_time >= $2
        AND   qs.collection_time <= $3
        UNION ALL
        SELECT qs.delta_spills, qs.max_dop
        FROM v_query_stats AS qs
        WHERE r.query_hash IS NULL
        AND   qs.query_hash IS NULL
        AND   qs.server_id = $1
        AND   qs.database_name IS NOT DISTINCT FROM r.database_name
        AND   qs.collection_time >= $2
        AND   qs.collection_time <= $3
    ) AS x
) AS d ON TRUE
ORDER BY r.worker_time::DOUBLE PRECISION / GREATEST(r.executions, 1) *
         LN(GREATEST(r.executions, 1)) DESC";

    /// <summary>#9879: the statement the bad actor read runs and, for the split, its two extra bounds.</summary>
    internal readonly record struct BadActorRead(string Sql, DateTime? FirstHour, DateTime? Cut)
    {
        public bool UsesRollup => FirstHour.HasValue && Cut.HasValue;
    }

    /// <summary>
    /// #9879: picks the bad actor read for a window. The split serves when the rollup's floor is at or before the first
    /// hour line at or after the window start, and the cut is past that line. The cut is the lesser of the window end's
    /// UTC midnight and the rollup's materialization ceiling taken down to its hour, so the window's last day and every
    /// hour the rollup has not materialized are read from raw rows. An unknown floor or ceiling, a floor after the first
    /// hour line, or an empty rollup arm takes <see cref="BadActorSql"/> over the whole window.
    /// </summary>
    internal static BadActorRead ChooseBadActorRead(
        DateTime windowStart, DateTime windowEnd, DateTime? rollupFloor, DateTime? rollupCeiling)
    {
        if (rollupFloor is not DateTime floor || rollupCeiling is not DateTime ceiling)
        {
            return new BadActorRead(BadActorSql, null, null);
        }

        var startHour = HourOf(windowStart);
        var firstHour = startHour == windowStart ? windowStart : startHour.AddHours(1);
        var endMidnight = windowEnd.Date;
        var ceilingHour = HourOf(ceiling);
        var cut = endMidnight < ceilingHour ? endMidnight : ceilingHour;

        if (floor > firstHour || cut <= firstHour)
        {
            return new BadActorRead(BadActorSql, null, null);
        }

        return new BadActorRead(BadActorHourlySplitSql, firstHour, cut);
    }

    private static DateTime HourOf(DateTime value) =>
        new(value.Ticks - (value.Ticks % TimeSpan.TicksPerHour), value.Kind);

    /// <summary>
    /// #9879: the bad actor read for this pass's window, from the store's measured rollup coverage
    /// (<c>TimescaleSupport.DetectRollupCoverageAsync</c>).
    /// A store with no rollup answers no floor, which is the raw read.
    /// </summary>
    private async Task<BadActorRead> ChooseBadActorReadAsync(AnalysisContext context)
    {
        var rollups = await TimescaleSupport.DetectRollupsAsync(_postgres, context.CancellationToken);
        var coverage = await TimescaleSupport.DetectRollupCoverageAsync(_postgres, rollups, context.CancellationToken);
        return ChooseBadActorRead(
            AsNaive(context.TimeRangeStart),
            AsNaive(context.TimeRangeEnd),
            coverage.FloorOf(TimescaleSupport.QueryStatsIoHourlyView),
            coverage.CeilingOf(TimescaleSupport.QueryStatsIoHourlyView));
    }

    /// <summary>
    /// Identifies individual queries that are consistently terrible ("bad actors").
    /// These queries don't necessarily cause server-level symptoms but waste resources
    /// on every execution. Detection uses execution count tiers x per-execution impact.
    /// Top 5 worst offenders become individual BAD_ACTOR facts.
    /// </summary>
    private async Task CollectBadActorFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            /* #9879: the rollup serves the whole hours before the window's last UTC day when it reaches them. The coverage
               is read before this pass's connection opens, so the read holds one store connection at a time. */
            var read = await ChooseBadActorReadAsync(context);

            await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);

            using var cmd = new NpgsqlCommand(read.Sql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            if (read.FirstHour is DateTime firstHour && read.Cut is DateTime cut)
            {
                cmd.Parameters.AddWithValue(AsNaive(firstHour));
                cmd.Parameters.AddWithValue(AsNaive(cut));
            }

            /* #8317: the read ranks (database, hash) pairs, so one hash running in several databases can take
               more than one of the five rows, and the key carries the hash alone. One fact per hash, for the
               first row the ranking returns that is worth a fact: the highest-scoring database. */
            var seenHashes = new HashSet<string>(StringComparer.Ordinal);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var dbName = reader.IsDBNull(0) ? "" : reader.GetString(0);
                var queryHash = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var execCount = reader.IsDBNull(2) ? 0L : Convert.ToInt64(reader.GetValue(2));
                var avgCpuMs = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                var avgElapsedMs = reader.IsDBNull(4) ? 0.0 : Convert.ToDouble(reader.GetValue(4));
                var avgReads = reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5));
                var totalCpuUs = reader.IsDBNull(6) ? 0L : Convert.ToInt64(reader.GetValue(6));
                var totalReads = reader.IsDBNull(7) ? 0L : Convert.ToInt64(reader.GetValue(7));
                var totalSpills = reader.IsDBNull(8) ? 0L : Convert.ToInt64(reader.GetValue(8));
                var maxDop = reader.IsDBNull(9) ? 0 : Convert.ToInt32(reader.GetValue(9));

                // Skip low-impact queries — need meaningful per-execution cost
                if (avgCpuMs < 10 && avgReads < 1000) continue;
                if (!seenHashes.Add(queryHash)) continue;

                facts.Add(new Fact
                {
                    Source = "bad_actor",
                    Key = $"BAD_ACTOR_{queryHash}",
                    Value = avgCpuMs, // Primary scoring dimension
                    ServerId = context.ServerId,
                    DatabaseName = dbName,
                    Metadata = new Dictionary<string, double>
                    {
                        ["execution_count"] = execCount,
                        ["avg_cpu_ms"] = avgCpuMs,
                        ["avg_elapsed_ms"] = avgElapsedMs,
                        ["avg_reads"] = avgReads,
                        ["total_cpu_us"] = totalCpuUs,
                        ["total_reads"] = totalReads,
                        ["total_spills"] = totalSpills,
                        ["max_dop"] = maxDop
                    }
                });
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string ActiveQuerySql = $@"
SELECT
    COUNT(*) AS total_snapshots,
    COUNT(CASE WHEN total_elapsed_time_ms > 30000 THEN 1 END) AS long_running_count,
    COUNT(CASE WHEN blocking_session_id > 0 THEN 1 END) AS blocked_count,
    MAX(total_elapsed_time_ms) AS max_elapsed_ms,
    COUNT(CASE WHEN dop > 1 THEN 1 END) AS parallel_count,
    MAX(dop) AS max_dop,
    COUNT(DISTINCT session_id) AS distinct_sessions
FROM query_snapshots
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   {PerformanceMonitor.Common.MonitorApplicationNames.StoreNotMonitorProgramNamePredicate}";

    /// <summary>
    /// Collects active query snapshot facts: long-running queries, blocked sessions, high DOP.
    /// </summary>
    private async Task CollectActiveQueryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);

            using var cmd = new NpgsqlCommand(ActiveQuerySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalSnapshots = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (totalSnapshots == 0) return;

            var longRunning = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var blocked = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var maxElapsed = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var parallel = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var maxDop = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));
            var distinctSessions = reader.IsDBNull(6) ? 0L : ToInt64(reader.GetValue(6));

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "ACTIVE_QUERIES",
                Value = longRunning,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["total_snapshots"] = totalSnapshots,
                    ["long_running_count"] = longRunning,
                    ["blocked_count"] = blocked,
                    ["max_elapsed_ms"] = maxElapsed,
                    ["parallel_count"] = parallel,
                    ["max_dop"] = maxDop,
                    ["distinct_sessions"] = distinctSessions
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /* #3653 (A9): the fifth column names the job. The four aggregates say how MANY jobs overran and by
       how much; they cannot say WHICH, so the story, the advice card and the persisted finding all read
       "1 Agent job running long" and sent the operator to the Running Jobs view to learn the name. One
       name, chosen among the rows that were running long (the FILTER — a job merely present in the
       window is not the subject), by percent_of_average first because "running long" is defined
       against the job's OWN history and the furthest past its own average is the story's subject;
       current_duration_seconds breaks a tie the way the collector's own ORDER BY does; job_name last
       so two rows equal on both still yield the same answer on every pass — the composed advice is
       frozen into the finding's StoryText, and a pick that flipped between passes would rewrite the
       finding's text over an unchanged window. A job running long across several collection ticks
       appears in several rows; that is harmless for the name (the same name sorts first each time).
       The counts are #7883: distinct job RUNS (job id plus the run's start time), not rows, because one
       row lands per job per collection tick and the advice and the severity ramp both read the count as
       a number of jobs.
       ARRAY_AGG … FILTER … [1] rather than a second scan or a subquery: one round trip, one row, and
       the same clause runs unchanged on DuckDB (1-based lists, FILTER, ordered aggregates), so the Lite
       twin carries it verbatim over v_running_jobs. NULL when nothing ran long — the fact still emits (running_count > 0)
       and the advice keeps its unnamed sentence. */
    public const string RunningJobsSql = @"
SELECT
    COUNT(DISTINCT COALESCE(job_id, job_name, '') || '|' || COALESCE(start_time::text, '')) AS running_count,
    COUNT(DISTINCT COALESCE(job_id, job_name, '') || '|' || COALESCE(start_time::text, '')) FILTER (WHERE is_running_long) AS running_long_count,
    MAX(percent_of_average) AS max_percent_of_avg,
    MAX(current_duration_seconds) AS max_duration_seconds,
    (ARRAY_AGG(job_name ORDER BY percent_of_average DESC NULLS LAST, current_duration_seconds DESC, job_name)
        FILTER (WHERE is_running_long))[1] AS worst_long_job_name
FROM running_jobs
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

    /// <summary>
    /// Collects running job facts: jobs currently running long vs historical averages, and (#3653) the
    /// name of the one furthest past its own history, carried on the fact's ObjectName.
    /// </summary>
    private async Task CollectRunningJobFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);

            using var cmd = new NpgsqlCommand(RunningJobsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var runningCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (runningCount == 0) return;

            var runningLong = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var maxPctAvg = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
            var maxDuration = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            /* #3653: NULL (no row was running long) stays null — the composer's "present" test is
               IsNullOrEmpty, and an empty string would read as a job with no name. */
            var worstLongJobName = reader.IsDBNull(4) ? null : reader.GetString(4);

            facts.Add(new Fact
            {
                Source = "jobs",
                Key = "RUNNING_JOBS",
                Value = runningLong,
                ServerId = context.ServerId,
                /* #3653: the job's NAME rides in ObjectName — the string slot the object-scoped facts
                   use for what their source query selected — and never in Metadata, which is
                   Dictionary<string, double> by contract: every reader (scorer, advice, MCP, the finding
                   stores' JSON) treats a metadata value as a number, so an identifier there is either a
                   compile error or a NaN. The counts are of distinct job runs (#7883), the maxima are
                   over ALL rows in the window, and the name alone is scoped to the running-long rows. */
                ObjectName = worstLongJobName,
                Metadata = new Dictionary<string, double>
                {
                    ["running_count"] = runningCount,
                    ["running_long_count"] = runningLong,
                    ["max_percent_of_average"] = maxPctAvg,
                    ["max_duration_seconds"] = maxDuration
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string SessionStatsSql = @"
WITH latest AS (
    SELECT program_name, connection_count, running_count, sleeping_count, dormant_count,
           ROW_NUMBER() OVER (PARTITION BY program_name ORDER BY collection_time DESC) AS rn
    FROM session_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
)
SELECT
    SUM(connection_count) AS total_connections,
    SUM(running_count) AS total_running,
    SUM(sleeping_count) AS total_sleeping,
    SUM(dormant_count) AS total_dormant,
    COUNT(*) AS distinct_apps,
    MAX(connection_count) AS max_app_connections
FROM latest WHERE rn = 1";

    /// <summary>
    /// Collects session stats: connection counts per application, total connections.
    /// </summary>
    private async Task CollectSessionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(StoreSessionNames.AnalysisFacts, context.CancellationToken);

            using var cmd = new NpgsqlCommand(SessionStatsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalConns = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            if (totalConns == 0) return;

            var totalRunning = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalSleeping = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalDormant = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var distinctApps = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));
            var maxAppConns = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));

            facts.Add(new Fact
            {
                Source = "sessions",
                Key = "SESSION_STATS",
                Value = totalConns,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["total_connections"] = totalConns,
                    ["total_running"] = totalRunning,
                    ["total_sleeping"] = totalSleeping,
                    ["total_dormant"] = totalDormant,
                    ["distinct_applications"] = distinctApps,
                    ["max_app_connections"] = maxAppConns
                }
            });
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

}
