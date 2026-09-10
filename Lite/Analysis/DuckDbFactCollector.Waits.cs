using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Collects wait stats facts — one Fact per significant wait type.
    /// Value is wait_time_ms / period_duration_ms (fraction of examined period).
    /// </summary>
    private async Task CollectWaitStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    wait_type,
    SUM(delta_waiting_tasks) AS total_waiting_tasks,
    SUM(delta_wait_time_ms) AS total_wait_time_ms,
    SUM(delta_signal_wait_time_ms) AS total_signal_wait_time_ms
FROM v_wait_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_wait_time_ms > 0
GROUP BY wait_type
ORDER BY SUM(delta_wait_time_ms) DESC";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        while (await reader.ReadAsync(context.CancellationToken))
        {
            var waitType = reader.GetString(0);
            var waitingTasks = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var waitTimeMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var signalWaitTimeMs = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));

            if (waitTimeMs <= 0) continue;

            var fractionOfPeriod = waitTimeMs / context.PeriodDurationMs;
            var avgMsPerWait = waitingTasks > 0 ? (double)waitTimeMs / waitingTasks : 0;

            facts.Add(new Fact
            {
                Source = "waits",
                Key = waitType,
                Value = fractionOfPeriod,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["wait_time_ms"] = waitTimeMs,
                    ["waiting_tasks_count"] = waitingTasks,
                    ["signal_wait_time_ms"] = signalWaitTimeMs,
                    ["resource_wait_time_ms"] = waitTimeMs - signalWaitTimeMs,
                    ["avg_ms_per_wait"] = avgMsPerWait,
                    ["period_duration_ms"] = context.PeriodDurationMs
                }
            });
        }
    }

    /// <summary>
    /// Collects blocking facts from blocked_process_reports.
    /// Produces a single BLOCKING_EVENTS fact with event count, rate, and details.
    /// Value is events per hour for threshold comparison.
    /// </summary>
    private async Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT
    COUNT(*) AS event_count,
    AVG(wait_time_ms) AS avg_wait_time_ms,
    MAX(wait_time_ms) AS max_wait_time_ms,
    COUNT(DISTINCT blocking_spid) AS distinct_head_blockers,
    COUNT(CASE WHEN blocking_status = 'sleeping' THEN 1 END) AS sleeping_blocker_count
FROM v_blocked_process_reports
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return;

        var eventCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        if (eventCount <= 0) return;

        var avgWaitTimeMs = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
        var maxWaitTimeMs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
        var distinctHeadBlockers = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
        var sleepingBlockerCount = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

        var periodHours = context.PeriodDurationMs / 3_600_000.0;
        var eventsPerHour = periodHours > 0 ? eventCount / periodHours : 0;

        facts.Add(new Fact
        {
            Source = "blocking",
            Key = "BLOCKING_EVENTS",
            Value = eventsPerHour,
            ServerId = context.ServerId,
            Metadata = new Dictionary<string, double>
            {
                ["event_count"] = eventCount,
                ["events_per_hour"] = eventsPerHour,
                ["avg_wait_time_ms"] = avgWaitTimeMs,
                ["max_wait_time_ms"] = maxWaitTimeMs,
                ["distinct_head_blockers"] = distinctHeadBlockers,
                ["sleeping_blocker_count"] = sleepingBlockerCount,
                ["period_hours"] = periodHours
            }
        });
    }

    /// <summary>
    /// Collects deadlock facts from the deadlocks table.
    /// Produces a single DEADLOCKS fact with count and rate.
    /// Value is deadlocks per hour for threshold comparison.
    /// </summary>
    private async Task CollectDeadlockFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COUNT(*) AS deadlock_count
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken)) return;

        var deadlockCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        if (deadlockCount <= 0) return;

        var periodHours = context.PeriodDurationMs / 3_600_000.0;
        var deadlocksPerHour = periodHours > 0 ? deadlockCount / periodHours : 0;

        facts.Add(new Fact
        {
            Source = "blocking",
            Key = "DEADLOCKS",
            Value = deadlocksPerHour,
            ServerId = context.ServerId,
            Metadata = new Dictionary<string, double>
            {
                ["deadlock_count"] = deadlockCount,
                ["deadlocks_per_hour"] = deadlocksPerHour,
                ["period_hours"] = periodHours
            }
        });
    }

    /// <summary>
    /// Reconstructs blocking chains from blocked_process_reports (per-pair rows) and emits
    /// one aggregate BLOCKING_CHAIN fact describing the worst chain — apex head blocker,
    /// depth, and transitive victim count — structure the BLOCKING_EVENTS rate is blind to.
    /// </summary>
    private async Task CollectBlockingChainFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        const int maxPairs = 5000;
        const int maxDepth = 50;
        const int stepBudget = 100_000;

        try
        {
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var command = connection.CreateCommand();
            // SpidFilter keeps this in lockstep with the drill-down + viewer fetch on the apex
            // (Lite maps a missing blocker to spid 0 — see BlockingPairRowQuery).
            command.CommandText = $@"
SELECT
    {BlockingPairRowQuery.LeadingColumns},
    blocked_sql_text,
    blocking_sql_text,
    {BlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {BlockingPairRowQuery.TrailingIdentityColumns}
FROM v_blocked_process_reports
WHERE server_id = $1
AND   event_time >= $2
AND   event_time <= $3
{BlockingPairRowQuery.SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

            command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            var rows = new List<BlockingPairRow>();
            using (var reader = await command.ExecuteReaderAsync(context.CancellationToken))
            {
                while (await reader.ReadAsync(context.CancellationToken))
                    rows.Add(BlockingPairRowQuery.Read(reader));
            }

            // Always-on DMV blocking snapshot fallback (works when the blocked-process-report XE is empty,
            // e.g. AWS RDS). Merge BEFORE the empty check so DMV-only blocking still produces facts.
            await BlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
                connection.CreateCommand, rows, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd,
                context.CancellationToken);

            if (rows.Count == 0) return;

            var reconstruction = BlockingChainReconstructor.Reconstruct(rows, maxDepth, maxPairs, stepBudget, scopeByMonitorLoop: false);
            if (reconstruction.Chains.Count == 0) return;

            var worst = reconstruction.Chains[0];

            facts.Add(new Fact
            {
                Source = "blocking",
                Key = "BLOCKING_CHAIN",
                Value = worst.Depth,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["worst_chain_depth"] = worst.Depth,
                    ["worst_chain_victim_count"] = worst.VictimCount,
                    ["worst_apex_spid"] = worst.ApexSpid,
                    ["worst_apex_sleeping"] = worst.ApexSleeping ? 1 : 0,
                    ["worst_chain_max_wait_ms"] = worst.MaxWaitMs,
                    ["total_reconstructed_chains"] = reconstruction.Chains.Count,
                    ["deepest_chain_overall"] = reconstruction.Chains.Max(c => c.Depth),
                    ["max_victim_count_overall"] = reconstruction.Chains.Max(c => c.VictimCount),
                    ["depth_capped"] = reconstruction.DepthCapped ? 1 : 0,
                    ["traversal_truncated"] = reconstruction.TraversalTruncated ? 1 : 0,
                    ["cycle_detected"] = reconstruction.CycleDetected ? 1 : 0
                }
            });
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

}
