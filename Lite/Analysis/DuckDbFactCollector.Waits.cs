using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>
    /// Step 0 of every pass (#3538 A2): stamps <see cref="AnalysisContext.Coverage"/> — how much of the
    /// window the collector actually observed, measured from the wait-stats collection series — and,
    /// when the window was only partly observed, emits the <see cref="WindowCoverage.FactKey"/> context
    /// fact beside the facts it qualifies. Every rate and fraction fact below divides by this stamp.
    /// Darling's <c>PgFactCollector.CollectObservedCoverageAsync</c> is the twin; the SQL is byte-identical
    /// (shared dialect: <c>EXTRACT(EPOCH FROM …)</c>, <c>LAG</c>, <c>GREATEST</c>, <c>COALESCE</c>) and
    /// <see cref="WindowCoverage"/> carries the measurement's argument, so it is not repeated here.
    ///
    /// <para>The parameters: <c>$1..$3</c> server and window as every windowed query binds them;
    /// <c>$4</c> the window start minus the gap policy, so the first in-window row can find its
    /// predecessor (the <c>GREATEST</c> then clips its interval to the window); <c>$5</c> the policy
    /// itself, <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>, bound rather than inlined so
    /// this read and the calculator that produced the deltas cannot disagree about where "observed"
    /// ends. <c>DISTINCT collection_time</c> because a collection writes one row per wait type and the
    /// interval belongs to the collection. The lead-in and tail gaps are finished in C# from the edge
    /// columns, because they are about the window's edges, which the row set cannot see.</para>
    ///
    /// <para>It lives in the collector rather than in <c>AnalysisService</c> so that every path that
    /// collects facts — the scheduled pass, <c>get_analysis_facts</c>, both windows of
    /// <c>compare_analysis</c>, and a test handing the collector a bare context — gets the stamp before
    /// the first division. Like the wait read it precedes, it carries no catch: this is the canary
    /// series, and a store that cannot answer it should fail the pass loudly rather than degrade into
    /// "unobserved", which would report a dead collector for a store that merely faulted.</para>
    /// </summary>
    private async Task CollectObservedCoverageAsync(AnalysisContext context, List<Fact> facts)
    {
        var nominalMs = context.PeriodDurationMs;
        if (nominalMs <= 0)
        {
            /* A zero-length or reversed window: no time to divide by and nothing the series could say. */
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = @"
WITH samples AS (
    SELECT DISTINCT collection_time
    FROM v_wait_stats
    WHERE server_id = $1
    AND   collection_time >= $4
    AND   collection_time <= $3
),
intervals AS (
    SELECT collection_time,
           LAG(collection_time) OVER (ORDER BY collection_time) AS previous_time
    FROM samples
)
SELECT
    COUNT(*) AS sample_count,
    COALESCE(SUM(CASE
        WHEN previous_time IS NULL THEN 0
        WHEN EXTRACT(EPOCH FROM (collection_time - previous_time)) > $5 THEN 0
        ELSE EXTRACT(EPOCH FROM (collection_time - GREATEST(previous_time, $2)))
    END), 0) AS observed_seconds,
    COALESCE(MAX(CASE
        WHEN previous_time IS NOT NULL AND EXTRACT(EPOCH FROM (collection_time - previous_time)) > $5
        THEN EXTRACT(EPOCH FROM (collection_time - GREATEST(previous_time, $2)))
        ELSE 0
    END), 0) AS largest_discarded_seconds,
    COALESCE(SUM(CASE WHEN previous_time IS NULL THEN 1 ELSE 0 END), 0) AS orphan_count,
    MIN(collection_time) AS first_sample,
    MAX(collection_time) AS last_sample
FROM intervals
WHERE collection_time >= $2";

        command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
        command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart.AddSeconds(-CollectorDeltaCalculator.DefaultMaxGapSeconds) });
        command.Parameters.Add(new DuckDBParameter { Value = CollectorDeltaCalculator.DefaultMaxGapSeconds });

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
        {
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

        /* DuckDB hands SUM over integers back as a HUGEINT (BigInteger) — hence ToInt64, the
           BigInteger-tolerant shared reader — and the epoch sums back as DOUBLE. */
        var sampleCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        var observedSeconds = reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1));
        var largestDiscardedSeconds = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
        var orphanCount = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
        DateTime? firstSample = reader.IsDBNull(4) ? null : reader.GetDateTime(4);
        DateTime? lastSample = reader.IsDBNull(5) ? null : reader.GetDateTime(5);

        context.Coverage = BuildCoverage(
            context, nominalMs, sampleCount, observedSeconds, largestDiscardedSeconds, orphanCount, firstSample, lastSample);

        if (context.Coverage.IsPartial)
            facts.Add(context.Coverage.ToGapFact(context.ServerId));
    }

    /// <summary>
    /// Finishes the coverage stamp from the query's row: the lead-in gap (window start to a first row
    /// that had no predecessor) and the tail gap (last row to window end — a collector that died
    /// mid-window leaves nothing after itself to LAG from) are edge properties of the window and are
    /// computed here; the in-series discarded stretch comes from the query. Largest gap is the longest
    /// of the three. Darling's <c>PgFactCollector.BuildCoverage</c> verbatim.
    /// </summary>
    private static WindowCoverage BuildCoverage(
        AnalysisContext context, double nominalMs, long sampleCount, double observedSeconds,
        double largestDiscardedSeconds, long orphanCount, DateTime? firstSample, DateTime? lastSample)
    {
        if (sampleCount == 0 || firstSample is null || lastSample is null)
            return WindowCoverage.Unobserved(nominalMs);

        var leadInMs = orphanCount > 0 ? Math.Max(0, (firstSample.Value - context.TimeRangeStart).TotalMilliseconds) : 0;
        var tailMs = Math.Max(0, (context.TimeRangeEnd - lastSample.Value).TotalMilliseconds);
        var largestGapMs = Math.Max(largestDiscardedSeconds * 1000.0, Math.Max(leadInMs, tailMs));

        return new WindowCoverage
        {
            NominalMs = nominalMs,
            ObservedMs = Math.Min(nominalMs, Math.Max(0, observedSeconds * 1000.0)),
            SampleCount = (int)Math.Min(int.MaxValue, sampleCount),
            LargestGapMs = Math.Min(nominalMs, largestGapMs)
        };
    }

    /// <summary>
    /// Collects wait stats facts — one Fact per significant wait type.
    /// Value is wait_time_ms / the OBSERVED collection time in the window
    /// (<see cref="AnalysisContext.ObservedDurationMs"/>), i.e. the fraction of the time the collector
    /// was actually up that this wait type was being waited on — not the fraction of the nominal window
    /// (#3538 A2). The metadata keeps <c>period_duration_ms</c> (the nominal window) and adds
    /// <c>coverage_fraction</c>, so the divisor is recoverable as their product.
    ///
    /// <para>Read the Value with its known scale-dependence in mind: <c>delta_wait_time_ms</c> sums the
    /// wait time of every CONCURRENT task, so a fraction above 1.0 is legal (many tasks waiting at once)
    /// and the same 25% means different things on 4 schedulers and on 64 — a handful of parallel
    /// queries on the big box, most of the machine on the small one. The thresholds that score it do not
    /// yet normalise for that; documented here rather than corrected, because the correction belongs
    /// with the threshold re-derivation (#3538 A5), not with the denominator fix.</para>
    ///
    /// <para>An unobserved window (coverage 0) emits NO wait facts: there is no time to divide by, and
    /// a fabricated fraction of 0 would read as a quiet server. The service turns that into the
    /// "unavailable" envelope; this method's job is only to not lie.</para>
    /// </summary>
    private async Task CollectWaitStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

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

            var fractionOfPeriod = waitTimeMs / context.ObservedDurationMs;
            var avgMsPerWait = waitingTasks > 0 ? (double)waitTimeMs / waitingTasks : 0;

            var metadata = new Dictionary<string, double>
            {
                ["wait_time_ms"] = waitTimeMs,
                ["waiting_tasks_count"] = waitingTasks,
                ["signal_wait_time_ms"] = signalWaitTimeMs,
                ["resource_wait_time_ms"] = waitTimeMs - signalWaitTimeMs,
                ["avg_ms_per_wait"] = avgMsPerWait,
                ["period_duration_ms"] = context.PeriodDurationMs
            };
            FactCollectorHelpers.AddCoverageFraction(metadata, context);

            facts.Add(new Fact
            {
                Source = "waits",
                Key = waitType,
                Value = fractionOfPeriod,
                ServerId = context.ServerId,
                Metadata = metadata
            });
        }
    }

    /// <summary>
    /// Collects blocking facts from blocked_process_reports.
    /// Produces a single BLOCKING_EVENTS fact with event count, rate, and details.
    /// Value is the busiest 4-hour sub-window's events per hour (#3871), the PG collector's shape
    /// exactly — see <c>PgFactCollector.CollectBlockingFactsAsync</c> for the grain argument (the same
    /// storm must not grade CRITICAL on a 4-hour pass and Information on a 24-hour one) and for why the
    /// divisor is <c>min(4, observed hours)</c> (#3538 A2's observed-time rule, and the ≤4-hour
    /// degeneracy). DuckDB's 3-arg <c>time_bucket</c> takes the same window-start origin PG's
    /// <c>date_bin</c> does. <c>events_per_hour</c> stays published as whole-window context.
    /// </summary>
    private const double PeakWindowHours = 4.0;

    private async Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(context.CancellationToken);

        using var command = connection.CreateCommand();
        command.CommandText = @"
WITH reports AS (
    SELECT
        wait_time_ms,
        blocking_spid,
        blocking_status,
        time_bucket(INTERVAL '4 hours', collection_time, $2) AS bucket_start
    FROM v_blocked_process_reports
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
),
buckets AS (
    SELECT COUNT(*) AS bucket_event_count
    FROM reports
    GROUP BY bucket_start
)
SELECT
    COUNT(*) AS event_count,
    AVG(wait_time_ms) AS avg_wait_time_ms,
    MAX(wait_time_ms) AS max_wait_time_ms,
    COUNT(DISTINCT blocking_spid) AS distinct_head_blockers,
    COUNT(CASE WHEN blocking_status = 'sleeping' THEN 1 END) AS sleeping_blocker_count,
    (SELECT COALESCE(MAX(bucket_event_count), 0) FROM buckets) AS peak_4h_event_count
FROM reports";

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
        var peakEventCount = reader.IsDBNull(5) ? 0L : ToInt64(reader.GetValue(5));

        var periodHours = context.PeriodDurationMs / 3_600_000.0;
        var observedHours = context.ObservedDurationMs / 3_600_000.0;
        var eventsPerHour = eventCount / observedHours;
        var peakEventsPerHour = peakEventCount / Math.Min(PeakWindowHours, observedHours);

        facts.Add(new Fact
        {
            Source = "blocking",
            Key = "BLOCKING_EVENTS",
            Value = peakEventsPerHour,
            ServerId = context.ServerId,
            Metadata = new Dictionary<string, double>
            {
                ["event_count"] = eventCount,
                ["events_per_hour"] = eventsPerHour,
                ["events_per_hour_peak_4h"] = peakEventsPerHour,
                ["peak_4h_event_count"] = peakEventCount,
                ["avg_wait_time_ms"] = avgWaitTimeMs,
                ["max_wait_time_ms"] = maxWaitTimeMs,
                ["distinct_head_blockers"] = distinctHeadBlockers,
                ["sleeping_blocker_count"] = sleepingBlockerCount,
                ["period_hours"] = periodHours,
                ["observed_hours"] = observedHours
            }
        });
    }

    /// <summary>
    /// Collects deadlock facts from the deadlocks table.
    /// Produces a single DEADLOCKS fact with count and rate.
    /// Value is deadlocks per OBSERVED hour (see <see cref="CollectBlockingFactsAsync"/> — same divisor,
    /// same reason, #3538 A2). <c>period_hours</c> nominal, <c>observed_hours</c> the divisor; an
    /// unobserved window emits no fact.
    /// </summary>
    private async Task CollectDeadlockFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

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
        var observedHours = context.ObservedDurationMs / 3_600_000.0;
        var deadlocksPerHour = deadlockCount / observedHours;

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
                ["period_hours"] = periodHours,
                ["observed_hours"] = observedHours
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
