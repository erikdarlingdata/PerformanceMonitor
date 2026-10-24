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

    /* Total wait time per second over the time actually collected (#6234). wait_stats holds one row per wait type per collection, every one
       carrying the collection's sample_interval_seconds, so the interval counts once per collection_time. A collection whose stored interval
       is 0 (the first sighting or a counter reset: no delta is knowable) adds nothing, and a legacy row with no stored interval falls back
       to the gap since the previous collection. The rate is the window's delta over those seconds, so a gap in collection thins the window's
       coverage instead of its rate, and the first row after a gap, which carries the whole gap's delta over the gap's seconds, stays a rate.
       A collection whose interval began before the window's start counts only the part inside the window: its seconds are clipped to the time
       since the start and its wait time is scaled by the same share (the delta is taken as spread evenly over the interval), so a collection
       that follows a long gap does not carry the gap's waits into the window or inflate its coverage.
       Columns: the rate, the seconds covered, the collections read. */
    private const string WaitSql = @"
WITH c AS (
    SELECT collection_time,
           sum(CASE WHEN delta_wait_time_ms > 0 THEN delta_wait_time_ms ELSE 0 END)::double precision AS wait_ms,
           max(sample_interval_seconds) AS stored_seconds
    FROM v_wait_stats
    WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3
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
SELECT CASE WHEN coalesce(sum(seconds) FILTER (WHERE seconds > 0), 0) > 0
            THEN (sum(wait_ms) FILTER (WHERE seconds > 0) / sum(seconds) FILTER (WHERE seconds > 0))::double precision
            ELSE 0::double precision END,
       coalesce(sum(seconds) FILTER (WHERE seconds > 0), 0)::double precision,
       count(*)::bigint
FROM e";

    /* Average CPU over the samples whose own instant is in the window (#6234): the ring-buffer collector ingests a backlog of up to hours of
       samples under the post-gap collection_time, so the window rides sample_time_utc (collection_time on a row from before V134 that has
       none). collection_time only bounds the scan: a sample is never collected before it was taken. The ring buffer holds one sample per
       minute, so the covered seconds are the distinct minutes seen times 60. Columns: the average, the seconds covered, the samples read. */
    private const string CpuSql = @"
SELECT coalesce(avg(sqlserver_cpu_utilization), 0)::double precision,
       (count(DISTINCT date_trunc('minute', coalesce(sample_time_utc, collection_time))) * 60)::double precision,
       count(*)::bigint
FROM v_cpu_utilization_stats
WHERE server_id = $1
  AND collection_time > $2 AND collection_time <= $3 + interval '1 day'
  AND coalesce(sample_time_utc, collection_time) > $2 AND coalesce(sample_time_utc, collection_time) <= $3
  AND sqlserver_cpu_utilization IS NOT NULL";

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

    /* $1 server, $2 database, $3 query_id, $4/$5 the window. The same dedup the Query Store duration trend runs: an interval is snapshotted
       many times, so each interval counts at its last (cumulative) snapshot. {placement} puts an interval in the window (#6234): by its
       START for the after-window and the week-earlier window, and by its END for the before-window, so the baseline holds only intervals
       that ended at or before the change. An interval is as long as Query Store's interval length (60 minutes by default), so one that
       straddles the change holds executions from both sides and belongs to neither. A row from before V155 has no stored end; its end is its
       start plus the interval length the database last reported (v_query_store_health), and an interval whose end cannot be known is left out
       of the before-window rather than guessed at. */
    private const string QueryStoreSqlTemplate = @"
WITH len AS (
    SELECT max(interval_length_minutes) AS minutes
    FROM v_query_store_health
    WHERE server_id = $1 AND database_name = $2 AND interval_length_minutes > 0
      AND capture_time >= $4 - interval '2 days' AND capture_time <= $5 + interval '1 day'
)
SELECT coalesce(sum(execution_count), 0)::bigint,
       coalesce(sum(execution_count::double precision * avg_duration_us::double precision), 0)::double precision,
       count(*)
FROM (
    SELECT execution_count, avg_duration_us,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                              ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM query_store_stats
    WHERE server_id = $1 AND database_name = $2 AND query_id = $3
      AND {placement}
      AND collection_time >= $4 - interval '1 day' AND collection_time <= $5 + interval '1 day'
) AS x
WHERE rn = 1";

    private const string IntervalEndSql = "coalesce(interval_end_time_utc, interval_start_time_utc + (SELECT minutes FROM len) * interval '1 minute')";

    private static readonly string QueryStoreByStartSql =
        QueryStoreSqlTemplate.Replace("{placement}", "interval_start_time_utc >= $4 AND interval_start_time_utc < $5", StringComparison.Ordinal);

    private static readonly string QueryStoreByEndSql =
        QueryStoreSqlTemplate.Replace("{placement}", IntervalEndSql + " > $4 AND " + IntervalEndSql + " <= $5", StringComparison.Ordinal);

    /* #6555: the same read as QueryStoreSqlTemplate, one row per plan (plan_id, executions, total elapsed microseconds). A query that ran several plans at
       once (parameter sensitivity, automatic plan correction churn) has an average that moves with the plan mix, so a duration that looks regressed is
       compared again at the before-window's mix (AdjustAfterToMix). Read only for a query whose whole-query average moved. */
    private const string QueryStorePlansSqlTemplate = @"
WITH len AS (
    SELECT max(interval_length_minutes) AS minutes
    FROM v_query_store_health
    WHERE server_id = $1 AND database_name = $2 AND interval_length_minutes > 0
      AND capture_time >= $4 - interval '2 days' AND capture_time <= $5 + interval '1 day'
)
SELECT coalesce(plan_id::text, ''), sum(execution_count)::bigint, coalesce(sum(execution_count::double precision * avg_duration_us::double precision), 0)::double precision
FROM (
    SELECT plan_id, execution_count, avg_duration_us,
           ROW_NUMBER() OVER (PARTITION BY database_name, query_id, plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role
                              ORDER BY collection_time DESC, execution_count DESC) AS rn
    FROM query_store_stats
    WHERE server_id = $1 AND database_name = $2 AND query_id = $3
      AND {placement}
      AND collection_time >= $4 - interval '1 day' AND collection_time <= $5 + interval '1 day'
) AS x
WHERE rn = 1
GROUP BY coalesce(plan_id::text, '')
HAVING sum(execution_count) > 0";

    private static readonly string QueryStorePlansByStartSql =
        QueryStorePlansSqlTemplate.Replace("{placement}", "interval_start_time_utc >= $4 AND interval_start_time_utc < $5", StringComparison.Ordinal);

    private static readonly string QueryStorePlansByEndSql =
        QueryStorePlansSqlTemplate.Replace("{placement}", IntervalEndSql + " > $4 AND " + IntervalEndSql + " <= $5", StringComparison.Ordinal);

    /* #6555: one query hash's plans in a window (the plan cache's query_plan_hash), the per-plan counterpart of QuerySql. $5 is the same database
       list QuerySql takes (NULL for none), so the plans describe the population the measured averages do. */
    private const string QueryPlansSql = @"
SELECT coalesce(query_plan_hash, ''), sum(delta_execution_count)::bigint, coalesce(sum(delta_elapsed_time), 0)::double precision
FROM v_query_stats
WHERE server_id = $1 AND collection_time > $2 AND collection_time <= $3 AND query_hash::text = $4
  AND ($5::text[] IS NULL OR database_name = ANY($5))
GROUP BY coalesce(query_plan_hash, '')
HAVING sum(delta_execution_count) > 0";

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

    /// <summary>#6581: a change that names one Query Store query (a forced or unforced plan, a Query Store hint set, changed or cleared). A watch of only these measures those queries.</summary>
    internal static bool IsQueryScopedKind(string kind) => kind is "plan_force" or "query_store_hint";

    /// <summary>One plan's runtime in a window (#6555): executions and total elapsed microseconds.</summary>
    internal readonly record struct PlanRuntime(long Executions, double ElapsedUs);

    /// <summary>
    /// #6555: the after-window's average duration, in microseconds, at the plan mix of <paramref name="reference"/> (a before or week-earlier window), so a shift
    /// in which plan ran more does not read as a change in how fast the query runs. A plan present in both windows keeps its after-window average but takes its
    /// share of the shared executions from the reference window; the shares of the shared plans are scaled to the share of the after-window's executions
    /// that ran on shared plans. A plan that ran only after (a plan change, the thing a watch is about) keeps its own contribution, and a plan that ran only
    /// before is ignored. With no shared plan, or one plan in each window, the result is the plain after-window average.
    /// </summary>
    internal static double AdjustAfterToMix(IReadOnlyDictionary<string, PlanRuntime> reference, IReadOnlyDictionary<string, PlanRuntime> after)
    {
        double afterExecutions = 0, afterElapsed = 0;
        foreach (var plan in after.Values)
        {
            afterExecutions += plan.Executions;
            afterElapsed += plan.ElapsedUs;
        }

        if (afterExecutions <= 0)
        {
            return 0;
        }

        var plain = afterElapsed / afterExecutions;
        if (reference.Count <= 1 && after.Count <= 1)
        {
            return plain;
        }

        double sharedReferenceExecutions = 0, sharedAfterExecutions = 0, sharedAfterElapsed = 0;
        foreach (var (plan, runtime) in after)
        {
            if (reference.TryGetValue(plan, out var reference1) && reference1.Executions > 0 && runtime.Executions > 0)
            {
                sharedReferenceExecutions += reference1.Executions;
                sharedAfterExecutions += runtime.Executions;
                sharedAfterElapsed += runtime.ElapsedUs;
            }
        }

        if (sharedReferenceExecutions <= 0)
        {
            return plain;
        }

        var sharedShare = sharedAfterExecutions / afterExecutions;
        double adjustedShared = 0;
        foreach (var (plan, runtime) in after)
        {
            if (reference.TryGetValue(plan, out var reference2) && reference2.Executions > 0 && runtime.Executions > 0)
            {
                adjustedShared += sharedShare * (reference2.Executions / sharedReferenceExecutions) * (runtime.ElapsedUs / runtime.Executions);
            }
        }

        return adjustedShared + ((afterElapsed - sharedAfterElapsed) / afterExecutions);
    }

    /// <summary>
    /// #6555: whether any plan the query ran in both windows got slower on its own: it ran at least <see cref="MinExecutions"/> times in each window and its
    /// own average passes <see cref="DurationMoved"/>. The mix adjustment answers "did the average move once the mix is held", which a plan that was a minority
    /// before and now carries most of the executions can slip under (its slowdown is weighted by the share it used to have), so this is the second question.
    /// </summary>
    internal static bool AnySharedPlanMoved(IReadOnlyDictionary<string, PlanRuntime> reference, IReadOnlyDictionary<string, PlanRuntime> after)
    {
        foreach (var (plan, runtime) in after)
        {
            if (reference.TryGetValue(plan, out var before) && before.Executions >= MinExecutions && runtime.Executions >= MinExecutions
                && DurationMoved(before.ElapsedUs / before.Executions, runtime.ElapsedUs / runtime.Executions))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>The busiest plans of the after-window with their share of the executions and their average, before then after, so a note says which plan moved.</summary>
    internal static string PlanBreakdown(IReadOnlyDictionary<string, PlanRuntime> before, IReadOnlyDictionary<string, PlanRuntime> after)
    {
        double beforeTotal = before.Values.Sum(p => p.Executions), afterTotal = after.Values.Sum(p => p.Executions);
        var parts = new List<string>();
        foreach (var (plan, runtime) in after.OrderByDescending(p => p.Value.Executions).ThenBy(p => p.Key, StringComparer.Ordinal).Take(3))
        {
            var afterText = string.Create(CultureInfo.InvariantCulture, $"{runtime.Executions * 100 / afterTotal:0}% at {runtime.ElapsedUs / runtime.Executions / 1000:0.##} ms");
            var beforeText = before.TryGetValue(plan, out var was) && was.Executions > 0 && beforeTotal > 0
                ? string.Create(CultureInfo.InvariantCulture, $"{was.Executions * 100 / beforeTotal:0}% at {was.ElapsedUs / was.Executions / 1000:0.##} ms")
                : "not run";
            parts.Add($"plan {plan}: {beforeText} before, {afterText} after");
        }

        return string.Join("; ", parts);
    }

    /// <summary>
    /// #6555: decides one query's duration measure, given the plan-level reads of the three windows. The whole-query averages are the first test; for a
    /// query that ran more than one plan the after average is then compared again at the before-window's plan mix (and at the week-earlier window's), and a
    /// plan that got slower on its own keeps the measure moved whatever the mix did (<see cref="AnySharedPlanMoved"/>), so a shift in the mix alone is not a
    /// regression. <paramref name="note"/> says what the plans were; it is set whenever more than one plan ran in a window.
    /// </summary>
    internal static bool QueryDurationMoved(
        double beforeAvgUs, double afterAvgUs, double? weekAvgUs,
        IReadOnlyDictionary<string, PlanRuntime> beforePlans, IReadOnlyDictionary<string, PlanRuntime> afterPlans, IReadOnlyDictionary<string, PlanRuntime> weekPlans,
        out string? note)
    {
        note = null;
        var adjusted = AdjustAfterToMix(beforePlans, afterPlans);
        var adjustedWeek = weekAvgUs is null ? (double?)null : AdjustAfterToMix(weekPlans, afterPlans);
        var movedBefore = DurationMoved(beforeAvgUs, adjusted) || AnySharedPlanMoved(beforePlans, afterPlans);
        var movedWeek = weekAvgUs is null || DurationMoved(weekAvgUs.Value, adjustedWeek!.Value) || AnySharedPlanMoved(weekPlans, afterPlans);
        if (Math.Max(beforePlans.Count, afterPlans.Count) > 1)
        {
            note = string.Create(CultureInfo.InvariantCulture,
                $"{beforePlans.Count} plan(s) before, {afterPlans.Count} after; the average moves with the plan mix, so it is compared at the before-window's mix: {adjusted / 1000:0.##} ms there against {afterAvgUs / 1000:0.##} ms as measured; {PlanBreakdown(beforePlans, afterPlans)}");
        }

        return movedBefore && movedWeek;
    }

    /// <summary>One window's rate from a statement that returns (rate, seconds covered, rows read).</summary>
    private async Task<RateSample> ReadRateAsync(
        NpgsqlConnection connection, string sql, int serverId, DateTime start, DateTime end, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, sql, serverId, start, end);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return new RateSample(reader.GetDouble(0), reader.GetDouble(1), reader.GetInt64(2));
    }

    /// <summary>One window's reading of a rate (#6234): the rate over the time collected, the seconds the collection actually covered, and how many collections held rows.</summary>
    internal readonly record struct RateSample(double Rate, double CoveredSeconds, long Rows);

    /// <summary>A window is compared only when at least this fraction of its length was collected (#6234).</summary>
    public const double MinCoverage = 0.5;

    internal static double Coverage(RateSample sample, double nominalSeconds) => Math.Min(1, sample.CoveredSeconds / Math.Max(1, nominalSeconds));

    private static string Percent(double fraction) => (fraction * 100).ToString("0", CultureInfo.InvariantCulture) + "%";

    /// <summary>
    /// Decides one rate measure (#6234), the same way for both engines. The after and before windows must each hold rows AND cover at least
    /// <see cref="MinCoverage"/> of their length, or the measure is listed under not-measured with the coverage and never read as flat. A
    /// week-earlier window below that coverage is treated as absent (the measure is compared with the before-window alone) and a note says so.
    /// </summary>
    internal static bool AddRateMeasure(
        string name, string unit, RateSample after, double afterSeconds, RateSample before, double beforeSeconds,
        RateSample week, double weekSeconds, Func<double, double, bool> moved, Func<string, string> noRows,
        List<ChangeWatchMeasure> measures, List<ChangeWatchNotMeasured> notMeasured, List<string> notes)
    {
        var problems = new List<string>();
        Check("after", after, afterSeconds);
        Check("before", before, beforeSeconds);
        if (problems.Count > 0)
        {
            notMeasured.Add(new ChangeWatchNotMeasured(name, string.Join("; ", problems)));
            return false;
        }

        double? weekRate = null;
        if (week.Rows > 0)
        {
            var weekCoverage = Coverage(week, weekSeconds);
            if (weekCoverage >= MinCoverage)
            {
                weekRate = week.Rate;
            }
            else
            {
                notes.Add($"{name}: the week-earlier window was collected for {Percent(weekCoverage)} of its length (under {Percent(MinCoverage)}), so it was not compared");
            }
        }

        measures.Add(new ChangeWatchMeasure(name, unit, before.Rate, after.Rate, weekRate,
            moved(before.Rate, after.Rate) && (weekRate is null || moved(weekRate.Value, after.Rate))));
        return true;

        void Check(string label, RateSample sample, double nominal)
        {
            if (sample.Rows == 0)
            {
                problems.Add(noRows(label));
                return;
            }

            var coverage = Coverage(sample, nominal);
            if (coverage < MinCoverage)
            {
                problems.Add($"the {label} window was collected for {Percent(coverage)} of its length (at least {Percent(MinCoverage)} is needed to compare)");
            }
        }
    }

    /// <summary>Folds each not-measured entry into the notes and builds the evaluation; both engines end here so they answer in one shape.</summary>
    internal static ChangeWatchEvaluation Complete(
        ChangeWatchRow watch, List<ChangeWatchMeasure> measures, bool hadData, List<string> notes, List<ChangeWatchNotMeasured> notMeasured)
    {
        foreach (var n in notMeasured)
        {
            notes.Add($"not measured: {n.Measure}: {n.Reason}");
        }

        return new ChangeWatchEvaluation(hadData, measures, DetailText(watch, measures), ResultJson(watch, measures, hadData, notes, notMeasured), notes, notMeasured);
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
        var notMeasured = new List<ChangeWatchNotMeasured>();
        var notes = new List<string>();
        var hadData = false;

        /* Total wait time per second, over the time the wait collector actually covered in each window. */
        var waitAfter = await ReadRateAsync(connection, WaitSql, watch.ServerId, afterStart, afterEnd, cancellationToken);
        var waitBefore = await ReadRateAsync(connection, WaitSql, watch.ServerId, beforeStart, afterStart, cancellationToken);
        var waitWeek = await ReadRateAsync(connection, WaitSql, watch.ServerId, weekStart, weekEnd, cancellationToken);
        hadData |= AddRateMeasure("total_wait_ms_per_second", "ms/s", waitAfter, afterSeconds, waitBefore, beforeSeconds, waitWeek, afterSeconds,
            WaitMoved, window => $"the wait_stats collector stored no rows for this target in the {window} window", measures, notMeasured, notes);

        /* Average CPU. */
        var cpuAfter = await ReadRateAsync(connection, CpuSql, watch.ServerId, afterStart, afterEnd, cancellationToken);
        var cpuBefore = await ReadRateAsync(connection, CpuSql, watch.ServerId, beforeStart, afterStart, cancellationToken);
        var cpuWeek = await ReadRateAsync(connection, CpuSql, watch.ServerId, weekStart, weekEnd, cancellationToken);
        hadData |= AddRateMeasure("avg_cpu_percent", "%", cpuAfter, afterSeconds, cpuBefore, beforeSeconds, cpuWeek, afterSeconds,
            CpuMoved, window => $"the cpu_utilization_stats collector stored no samples for this target in the {window} window", measures, notMeasured, notes);

        /* Query duration. A watch of plan or Query Store hint changes alone measures THAT query (Query Store's own runtime intervals, the source the duration
           trend reads, keyed by database and query_id); where Query Store has no usable intervals for it, the top 20 by total duration
           of the changed databases are measured instead and the result says so. Any other mix reads the top 20 server-wide.
           The before-window holds only intervals that ENDED at or before the change and the after-window only intervals that STARTED at or
           after it (QueryStoreByEndSql, QueryStoreByStartSql): at Query Store's default 60-minute interval an interval that straddles the change
           holds executions from both sides, and counting it as "before" compared the regressed plan with itself. */
        var planDatabases = watch.Changes.Where(c => IsQueryScopedKind(c.Kind) && c.DatabaseName is not null)
            .Select(c => c.DatabaseName!).Distinct(StringComparer.Ordinal).ToArray();
        var onlyPlan = watch.Changes.Count > 0 && watch.Changes.All(c => IsQueryScopedKind(c.Kind));
        var scope = onlyPlan && planDatabases.Length > 0 ? planDatabases : null;
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
                    var measureName = $"query_avg_duration_ms:{database}:{queryId}";
                    var queryAfter = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, afterStart, afterEnd, false, cancellationToken);
                    var queryBefore = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, beforeStart, afterStart, true, cancellationToken);
                    if (queryAfter.Intervals == 0 || queryBefore.Intervals == 0)
                    {
                        needTopQueries = true;
                        var reason = queryBefore.Intervals == 0
                            ? $"Query Store holds no interval for query {queryId} in {database} that ended at or before the change (the interval length leaves no whole interval in the hour before it), so there is no pre-change baseline"
                            : $"Query Store holds no interval for query {queryId} in {database} that started at or after the change yet, so there is no post-change measure";
                        notMeasured.Add(new ChangeWatchNotMeasured(measureName, reason));
                        /* The reason is in the notes through Complete (every not-measured entry is); only the fallback is added here (#6239 round 3, L9). */
                        notes.Add($"{measureName}: the top {TopQueries} queries by total duration in the changed databases were measured instead");
                        continue;
                    }

                    hadData = true;
                    if (queryBefore.Executions < MinExecutions || queryAfter.Executions < MinExecutions)
                    {
                        notMeasured.Add(new ChangeWatchNotMeasured(measureName,
                            $"fewer than {MinExecutions} executions in the before or after window ({queryBefore.Executions} before, {queryAfter.Executions} after)"));
                        continue;
                    }

                    var queryWeek = await ReadQueryStoreAsync(connection, watch.ServerId, database, queryId, weekStart, weekEnd, false, cancellationToken);
                    var queryBeforeAvg = queryBefore.ElapsedUs / queryBefore.Executions;
                    var queryAfterAvg = queryAfter.ElapsedUs / queryAfter.Executions;
                    double? queryWeekAvg = queryWeek.Executions >= MinExecutions ? queryWeek.ElapsedUs / queryWeek.Executions : null;
                    if (DurationMoved(queryBeforeAvg, queryAfterAvg) && (queryWeekAvg is null || DurationMoved(queryWeekAvg.Value, queryAfterAvg)))
                    {
                        /* #6555: a query that ran several plans is compared per plan (at the before-window's mix), not by the average over all of them. */
                        var beforePlans = await ReadQueryStorePlansAsync(connection, watch.ServerId, database, queryId, beforeStart, afterStart, true, cancellationToken);
                        var afterPlans = await ReadQueryStorePlansAsync(connection, watch.ServerId, database, queryId, afterStart, afterEnd, false, cancellationToken);
                        var weekPlans = queryWeekAvg is null
                            ? new Dictionary<string, PlanRuntime>(StringComparer.Ordinal)
                            : await ReadQueryStorePlansAsync(connection, watch.ServerId, database, queryId, weekStart, weekEnd, false, cancellationToken);
                        var moved = QueryDurationMoved(queryBeforeAvg, queryAfterAvg, queryWeekAvg, beforePlans, afterPlans, weekPlans, out var mixNote);
                        var where = $"read from Query Store: query_id {queryId} in database {database}; {queryBefore.Executions} -> {queryAfter.Executions} executions";
                        if (moved)
                        {
                            measures.Add(new ChangeWatchMeasure(measureName, "ms", queryBeforeAvg / 1000, queryAfterAvg / 1000,
                                queryWeekAvg / 1000, true, mixNote is null ? where : where + "; " + mixNote));
                        }
                        else
                        {
                            notes.Add($"{measureName}: the average duration moved from {Num(queryBeforeAvg / 1000)} ms to {Num(queryAfterAvg / 1000)} ms, but only with the plan mix, so it is not a regression ({where}{(mixNote is null ? "" : "; " + mixNote)})");
                        }
                    }
                }
            }
        }

        var afterQueries = needTopQueries
            ? await ReadQueriesAsync(connection, watch.ServerId, afterStart, afterEnd, null, scope, TopQueries, cancellationToken)
            : new Dictionary<string, (string? Database, long Executions, double ElapsedUs)>(StringComparer.Ordinal);
        if (needTopQueries && afterQueries.Count == 0)
        {
            notMeasured.Add(new ChangeWatchNotMeasured("query_avg_duration_ms", "the query_stats collector stored no query with executions in the after window"));
        }
        else if (afterQueries.Count > 0)
        {
            var hashes = afterQueries.Keys.ToArray();
            var beforeQueries = await ReadQueriesAsync(connection, watch.ServerId, beforeStart, afterStart, hashes, scope, hashes.Length, cancellationToken);
            var weekQueries = await ReadQueriesAsync(connection, watch.ServerId, weekStart, weekEnd, hashes, scope, hashes.Length, cancellationToken);
            var missingBefore = 0;
            var belowFloor = 0;
            foreach (var (hash, after) in afterQueries)
            {
                if (!beforeQueries.TryGetValue(hash, out var before))
                {
                    missingBefore++;
                    continue;
                }

                hadData = true;
                if (before.Executions < MinExecutions || after.Executions < MinExecutions)
                {
                    belowFloor++;
                    continue;
                }

                var beforeAvg = before.ElapsedUs / before.Executions;
                var afterAvg = after.ElapsedUs / after.Executions;
                double? weekAvg = weekQueries.TryGetValue(hash, out var week) && week.Executions >= MinExecutions ? week.ElapsedUs / week.Executions : null;
                if (DurationMoved(beforeAvg, afterAvg) && (weekAvg is null || DurationMoved(weekAvg.Value, afterAvg)))
                {
                    /* #6555: a query hash that ran several plans is compared per plan (at the before-window's mix), not by the average over all of them. */
                    var beforePlans = await ReadQueryPlansAsync(connection, watch.ServerId, hash, beforeStart, afterStart, scope, cancellationToken);
                    var afterPlans = await ReadQueryPlansAsync(connection, watch.ServerId, hash, afterStart, afterEnd, scope, cancellationToken);
                    var weekPlans = weekAvg is null
                        ? new Dictionary<string, PlanRuntime>(StringComparer.Ordinal)
                        : await ReadQueryPlansAsync(connection, watch.ServerId, hash, weekStart, weekEnd, scope, cancellationToken);
                    var moved = QueryDurationMoved(beforeAvg, afterAvg, weekAvg, beforePlans, afterPlans, weekPlans, out var mixNote);
                    var where = $"read from the plan cache (query_stats), query_hash {hash} in database {after.Database ?? "(unknown)"}; {before.Executions} -> {after.Executions} executions";
                    if (moved)
                    {
                        measures.Add(new ChangeWatchMeasure("query_avg_duration_ms:" + hash, "ms", beforeAvg / 1000, afterAvg / 1000,
                            weekAvg / 1000, true, mixNote is null ? where : where + "; " + mixNote));
                    }
                    else
                    {
                        notes.Add($"query_avg_duration_ms:{hash}: the average duration moved from {Num(beforeAvg / 1000)} ms to {Num(afterAvg / 1000)} ms, but only with the plan mix, so it is not a regression ({where}{(mixNote is null ? "" : "; " + mixNote)})");
                    }
                }
            }

            AddQuerySkips(notMeasured, afterQueries.Count, missingBefore, belowFloor, "query_stats");
        }

        return Complete(watch, measures, hadData, notes, notMeasured);
    }

    /// <summary>The queries of the top list that could not be compared, one entry per engine pass (#6234): none of their executions are in the before window, or fewer than <see cref="MinExecutions"/> in either window.</summary>
    internal static void AddQuerySkips(List<ChangeWatchNotMeasured> notMeasured, int top, int missingBefore, int belowFloor, string source)
    {
        var reasons = new List<string>();
        if (missingBefore > 0)
        {
            reasons.Add($"{missingBefore} of the {top} top queries have no {source} rows in the before window (new since then, or the statistics were reset), so they were not compared");
        }

        if (belowFloor > 0)
        {
            reasons.Add($"{belowFloor} of the {top} top queries ran fewer than {MinExecutions} times in the before or after window, so they were not compared");
        }

        if (reasons.Count > 0)
        {
            notMeasured.Add(new ChangeWatchNotMeasured("query_avg_duration_ms", string.Join("; ", reasons)));
        }
    }

    /// <summary>One query's Query Store runtime in a window: executions, total elapsed microseconds, and how many intervals it had.
    /// Each interval counts once, at its final cumulative snapshot, the way the duration trend counts it. <paramref name="byEnd"/> places an
    /// interval by its end (the before-window) instead of its start.</summary>
    private async Task<(long Executions, double ElapsedUs, long Intervals)> ReadQueryStoreAsync(
        NpgsqlConnection connection, int serverId, string database, long queryId, DateTime start, DateTime end, bool byEnd, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(byEnd ? QueryStoreByEndSql : QueryStoreByStartSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(start) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        await reader.ReadAsync(cancellationToken);
        return (reader.GetInt64(0), reader.GetDouble(1), reader.GetInt64(2));
    }

    /// <summary>#6555: <see cref="ReadQueryStoreAsync"/> split by plan: the executions and total elapsed microseconds of each plan_id the query ran in the window.</summary>
    private async Task<Dictionary<string, PlanRuntime>> ReadQueryStorePlansAsync(
        NpgsqlConnection connection, int serverId, string database, long queryId, DateTime start, DateTime end, bool byEnd, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(byEnd ? QueryStorePlansByEndSql : QueryStorePlansByStartSql, connection) { CommandTimeout = _commandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = database });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = queryId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(start) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = Naive(end) });
        return await ReadPlansAsync(command, cancellationToken);
    }

    /// <summary>#6555: the plans (query_plan_hash) one query hash ran in the window, from the plan cache's statistics.</summary>
    private async Task<Dictionary<string, PlanRuntime>> ReadQueryPlansAsync(
        NpgsqlConnection connection, int serverId, string queryHash, DateTime start, DateTime end, string[]? databases, CancellationToken cancellationToken)
    {
        await using var command = Command(connection, QueryPlansSql, serverId, start, end);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = queryHash });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = (object?)databases ?? DBNull.Value });
        return await ReadPlansAsync(command, cancellationToken);
    }

    private static async Task<Dictionary<string, PlanRuntime>> ReadPlansAsync(NpgsqlCommand command, CancellationToken cancellationToken)
    {
        var plans = new Dictionary<string, PlanRuntime>(StringComparer.Ordinal);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            plans[reader.GetString(0)] = new PlanRuntime(reader.GetInt64(1), reader.GetDouble(2));
        }

        return plans;
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

        var moved = measures.Where(m => m.Moved).Take(5).ToList();
        foreach (var m in moved)
        {
            text.Append(m.Name).Append(" before ").Append(Num(m.Before)).Append(" after ").Append(Num(m.After))
                .Append(" week earlier ").Append(Num(m.WeekEarlier)).Append(' ').Append(m.Unit).Append("; ");
        }

        /* #6551: a query measure's note names the database and the read (plan cache or Query Store) it came from, so an agent can call the
           matching trend read for the right database without guessing it from another change line. The notes come after every measure line:
           the cut below drops the end of the text, and a note must never push a moved measure out of it. */
        foreach (var m in moved.Where(m => !string.IsNullOrEmpty(m.Note)))
        {
            text.Append(m.Name).Append(": ").Append(m.Note).Append("; ");
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
