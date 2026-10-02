/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /// <summary>
    /// The coverage witness (#3538 A2): how much of the window the collector actually observed, from the
    /// wait-stats collection series — the same rows <see cref="WaitStatsSql"/> sums, so numerator and
    /// denominator agree about when the collector was up. <see cref="WindowCoverage"/> carries the full
    /// argument for the three rules; the parameters are what make them concrete:
    /// <list type="bullet">
    /// <item><description><c>$1..$3</c> server and window, as every windowed fact query binds
    /// them.</description></item>
    /// <item><description><c>$4</c> = window start minus the gap policy: the scan reaches back ONE policy
    /// so the first in-window row can find its predecessor (its interval is then clipped to the window
    /// by the <c>GREATEST</c>). A predecessor further back than that would have exceeded the policy
    /// anyway, so nothing honest is lost by not looking further.</description></item>
    /// <item><description><c>$5</c> = <see cref="CollectorDeltaCalculator.DefaultMaxGapSeconds"/>, bound
    /// rather than inlined so this query and the calculator that produced the deltas can never disagree
    /// about where "observed" ends. An interval past it is credited as 0 (its delta was discarded) and is
    /// reported separately as a discarded stretch for the largest-gap figure.</description></item>
    /// </list>
    /// <c>DISTINCT collection_time</c> because a collection writes one row per wait type; the interval
    /// belongs to the collection, not to each of its hundred rows. Shared dialect, byte-identical to
    /// Lite's: <c>EXTRACT(EPOCH FROM …)</c>, <c>LAG</c>, <c>GREATEST</c> and <c>COALESCE</c> all run
    /// unchanged on DuckDB and Postgres (the anomaly detector's <c>WaitRateWindowSql</c> already leans on
    /// the first two). The lead-in and tail gaps are finished in C# from <c>first_sample</c> /
    /// <c>last_sample</c> / <c>orphan_count</c>, because they are about the WINDOW's edges, which the
    /// row set cannot see.
    /// </summary>
    public const string CoverageSql = @"
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

    /// <summary>
    /// Step 0 of every pass: stamps <see cref="AnalysisContext.Coverage"/> from <see cref="CoverageSql"/>
    /// and, when the window was only partly observed, emits the <see cref="WindowCoverage.FactKey"/>
    /// context fact beside the facts it qualifies (#3538 A2).
    ///
    /// <para>It lives in the collector rather than in the analysis service so that EVERY path that
    /// collects facts — the scheduled pass, <c>get_analysis_facts</c>, both windows of
    /// <c>compare_analysis</c>, and a test that hands the collector a bare context — gets the stamp
    /// before the first division, with no caller able to forget it. It runs BEFORE the wait read and,
    /// like that read, carries no catch: this is the canary series, and a store that cannot answer it
    /// should fail the pass loudly (the service's catch classifies and logs it) rather than degrade into
    /// "unobserved", which would report a dead collector for a store that merely timed out.</para>
    ///
    /// <para>A zero-length or reversed window skips the read and stamps unobserved: there is no time to
    /// divide by and nothing the series could say about it.</para>
    /// </summary>
    private async Task CollectObservedCoverageAsync(AnalysisContext context, List<Fact> facts)
    {
        var nominalMs = context.PeriodDurationMs;
        if (nominalMs <= 0)
        {
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var command = new NpgsqlCommand(CoverageSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart.AddSeconds(-CollectorDeltaCalculator.DefaultMaxGapSeconds)));
        command.Parameters.AddWithValue(CollectorDeltaCalculator.DefaultMaxGapSeconds);

        using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken))
        {
            context.Coverage = WindowCoverage.Unobserved(nominalMs);
            return;
        }

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
    /// that had no predecessor — the row itself is the earliest thing we know about) and the tail gap
    /// (last row to window end — a collector that died mid-window leaves nothing after itself to LAG
    /// from) are edge properties of the window and are computed here; the in-series discarded stretch
    /// comes from the query. Largest gap is the longest of the three.
    ///
    /// <para>Internal rather than private (#3542): <see cref="PgTargetFactCollector"/>'s coverage witness reads
    /// a different series (<c>pg_database_stats</c>) with the same five-column shape and finishes it through
    /// THIS method, so the two engines can never disagree about what a lead-in or a tail gap is.</para>
    /// </summary>
    internal static WindowCoverage BuildCoverage(
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

    public const string WaitStatsSql = @"
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

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        using var command = new NpgsqlCommand(WaitStatsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

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

    /* #3871: the busiest 4-hour sub-window rides out of the same statement as the whole-window
       aggregates. date_bin's ORIGIN is the window start ($2, reused — Npgsql positional params may
       repeat), NOT an epoch: with an epoch origin a scheduled 4-hour pass that does not begin on a
       4-hour epoch boundary splits across two buckets and its peak reads BELOW its own whole-window
       average, so the ≤4h degeneracy (a short window IS its own peak bucket) only holds with the
       window-start origin.

       the window is when the report HAPPENED (event_time), the same column the blocking grids and
       the chain reconstruction read, so the fact counts the events the grid shows. $4 is the
       EventWindowFloor for $2: blocked_process_reports is partitioned on collection_time, which an
       event_time bound gives the planner nothing to exclude chunks on. The bucket moves with the window:
       a bucket origin on one column over rows windowed on the other would split the peak across buckets. */
    private const string BlockingSqlHead = @"
WITH reports AS (
    SELECT
        wait_time_ms,
        blocking_spid,
        blocking_status,
        date_bin('4 hours', event_time, $2) AS bucket_start
    FROM blocked_process_reports
    WHERE server_id = $1
    AND   event_time >= $2
    AND   event_time <= $3";

    private const string BlockingSqlTail = @"
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

    public const string BlockingSql = BlockingSqlHead + @"
    AND   collection_time >= $4" + BlockingSqlTail;

    /// <summary>The same read for an Azure SQL Database master target: $4 is the names of the databases monitored
    /// as their own targets, whose events are skipped (a NULL database still counts), and $5 the event-window
    /// floor. The list keeps its number from the scoped read and the floor takes the next one, so the floor is
    /// $4 in <see cref="BlockingSql"/> and $5 here.</summary>
    public const string BlockingSqlSkippingSeparate = BlockingSqlHead + @"
    AND   collection_time >= $5
    AND   (database_name IS NULL OR NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x)))" + BlockingSqlTail;

    /// <summary>The peak sub-window's width in hours — the grain the (10, 50) grading pair was measured
    /// on (#3871). The peak rate divides by <c>min(this, observed hours)</c>, never by the constant
    /// alone: a one-hour read's peak bucket holds at most one observed hour of events, and dividing it
    /// by four would deflate the storm fourfold.</summary>
    private const double PeakWindowHours = 4.0;

    /// <summary>
    /// Collects blocking facts from blocked_process_reports.
    /// Produces a single BLOCKING_EVENTS fact with event count, rate, and details.
    /// Value is the busiest 4-hour sub-window's events per hour (#3871): the same storm must not grade
    /// CRITICAL on a 4-hour pass and Information on a 24-hour one, and a whole-window average dilutes a
    /// one-hour storm by however much quiet surrounds it, so the graded value is the peak 4-hour bucket
    /// — the grain the (10, 50) pair was measured on — while <c>events_per_hour</c> stays published as
    /// the whole-window context. The peak divides by <c>min(4, observed hours)</c>, keeping #3538 A2's
    /// observed-time rule (forty events in the one hour the collector saw is a 40/hr storm, not a 10/hr
    /// murmur) and making a ≤4-hour window grade exactly what it always did: with the window-start
    /// bucket origin such a window is one bucket, and the divisor is its observed hours.
    /// <c>period_hours</c> stays the nominal window; <c>observed_hours</c> the observed. An unobserved
    /// window emits no fact.
    /// </summary>
    private async Task CollectBlockingFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        var separate = SeparateDatabases(context);
        using var command = new NpgsqlCommand(separate is null ? BlockingSql : BlockingSqlSkippingSeparate, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        command.Parameters.AddWithValue(context.ServerId);
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
        command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
        /* The scoped read keeps its list at $4 and takes the floor as $5; the plain read has the floor at $4. */
        if (separate is not null) command.Parameters.AddWithValue(separate);
        command.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(context.TimeRangeStart));

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

    /// <summary>The window's deadlocks by when they HAPPENED (<c>deadlock_time</c>), as the deadlock grids read
    /// them. $4 is the <see cref="EventWindowFloor"/> for $2 — the partition-column bound the event
    /// window cannot supply, with no upper bound so a late-collected deadlock still counts.</summary>
    public const string DeadlocksSql = @"
SELECT COUNT(*) AS deadlock_count
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $4";

    /// <summary>Deadlocks whose row names a database that is not a separately monitored one (the event's database on the
    /// telemetry arm; a master stamp may be the connection's fallback, so it goes to the graph check): they cannot
    /// be all-in, so they count without their graphs being read. $4 is the raw list (both sides fold with lower()),
    /// $5 the event-window floor; the window is the event time, as <see cref="DeadlocksSql"/> reads it.</summary>
    public const string DeadlockOutsideCountSql = @"
SELECT COUNT(*)
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $5
AND   database_name IS NOT NULL
AND   lower(database_name) <> 'master'
AND   NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))";

    /// <summary>The graphs the every-process rule still has to decide: the row's database is unknown, is master, or is a
    /// separately monitored one. Windowed as <see cref="DeadlocksSql"/> ($4 the list, $5 the floor), so
    /// the arms count the same events.</summary>
    public const string DeadlockGraphsSql = @"
SELECT deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $5
AND   (database_name IS NULL OR lower(database_name) = 'master' OR lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))";

    /// <summary>The newest deadlock the outside rows hold: <see cref="DeadlockOutsideCountSql"/>'s predicate with its
    /// parameters ($4 the list, $5 the floor), reading the newest time instead of counting.</summary>
    public const string DeadlockOutsideNewestSql = @"
SELECT MAX(deadlock_time)
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $5
AND   database_name IS NOT NULL
AND   lower(database_name) <> 'master'
AND   NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))";

    /// <summary>The graphs <see cref="DeadlockGraphsSql"/> selects, newest first and only those newer than $6 (the
    /// outside rows' newest, or NULL for no bound), with their times: the first one the every-process rule counts
    /// is the newest the graph arm can contribute.</summary>
    public const string DeadlockGraphsNewestFirstSql = @"
SELECT deadlock_time, deadlock_graph_xml
FROM deadlocks
WHERE server_id = $1
AND   deadlock_time >= $2
AND   deadlock_time <= $3
AND   collection_time >= $5
AND   (database_name IS NULL OR lower(database_name) = 'master' OR lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))
AND   ($6::timestamp IS NULL OR deadlock_time > $6::timestamp)
ORDER BY deadlock_time DESC";

    /// <summary>The newest time among the window's deadlocks that do not belong wholly to the separately monitored
    /// databases (the same rule <see cref="CountDeadlocksSkippingSeparateAsync"/> counts by), or null when none
    /// count. The outside rows give a floor in SQL; the graphs newer than it are read newest first and the read stops
    /// at the first one that counts.</summary>
    internal static async Task<DateTime?> NewestDeadlockSkippingSeparateAsync(
        NpgsqlConnection connection, int serverId, DateTime start, DateTime end,
        IReadOnlyList<string> separate, System.Threading.CancellationToken ct, int commandTimeoutSeconds = FactCommandTimeoutSeconds)
    {
        var bound = separate.ToArray();
        DateTime? newest;
        using (var outsideCommand = new NpgsqlCommand(DeadlockOutsideNewestSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            outsideCommand.Parameters.AddWithValue(serverId);
            outsideCommand.Parameters.AddWithValue(AsNaive(start));
            outsideCommand.Parameters.AddWithValue(AsNaive(end));
            outsideCommand.Parameters.AddWithValue(bound);
            outsideCommand.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(start));
            var value = await outsideCommand.ExecuteScalarAsync(ct);
            newest = value is DateTime t ? t : null;
        }

        using var command = new NpgsqlCommand(DeadlockGraphsNewestFirstSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(AsNaive(start));
        command.Parameters.AddWithValue(AsNaive(end));
        command.Parameters.AddWithValue(bound);
        command.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(start));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = (object?)newest ?? DBNull.Value });
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var xml = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (!PerformanceMonitor.Common.DeadlockGraphDatabases.AllIn(xml, separate))
            {
                return reader.GetDateTime(0);
            }
        }

        return newest;
    }

    /// <summary>The newest collection time the server has stored: the upper bound of one incremental last-deadlock
    /// read, so a row that arrives while it runs is picked up by the next one.</summary>
    public const string DeadlockNewestCollectionSql = @"
SELECT MAX(collection_time)
FROM v_deadlocks
WHERE server_id = $1";

    /// <summary>The newest deadlock among the outside rows COLLECTED in ($2, $3], whatever their <c>deadlock_time</c>:
    /// <see cref="DeadlockOutsideNewestSql"/>'s predicate on collection time instead of event time. $4 is the raw list.</summary>
    public const string DeadlockOutsideNewestSinceSql = @"
SELECT MAX(deadlock_time)
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time > $2
AND   collection_time <= $3
AND   database_name IS NOT NULL
AND   lower(database_name) <> 'master'
AND   NOT (lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))";

    /// <summary>The graphs <see cref="DeadlockGraphsSql"/> would still decide, for rows COLLECTED in ($2, $3] and newer
    /// than $5 (the best time known so far, or NULL), newest first.</summary>
    public const string DeadlockGraphsNewestSinceSql = @"
SELECT deadlock_time, deadlock_graph_xml
FROM v_deadlocks
WHERE server_id = $1
AND   collection_time > $2
AND   collection_time <= $3
AND   (database_name IS NULL OR lower(database_name) = 'master' OR lower(database_name) = ANY(SELECT lower(x) FROM unnest($4::text[]) x))
AND   ($5::timestamp IS NULL OR deadlock_time > $5::timestamp)
ORDER BY deadlock_time DESC";

    /// <summary>
    /// The newest counting deadlock among the rows collected after <paramref name="checkedThrough"/> (up to the server's
    /// newest collection time), merged with <paramref name="cachedLast"/> by taking the later of the two; the same rule as
    /// <see cref="NewestDeadlockSkippingSeparateAsync"/>, over only the new rows. Returns the merged time and the
    /// collection time the answer is now exact through, or the unchanged pair, without reading any graph, when nothing
    /// has been collected since. Pass <see cref="DateTime.MinValue"/> and null to read everything.
    /// </summary>
    internal static async Task<(DateTime? Last, DateTime CheckedThrough, bool ReadGraphs)> NewestDeadlockSinceAsync(
        NpgsqlConnection connection, int serverId, DateTime checkedThrough, DateTime? cachedLast,
        IReadOnlyList<string> separate, System.Threading.CancellationToken ct, int commandTimeoutSeconds = FactCommandTimeoutSeconds)
    {
        DateTime? upper;
        using (var upperCommand = new NpgsqlCommand(DeadlockNewestCollectionSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            upperCommand.Parameters.AddWithValue(serverId);
            upper = await upperCommand.ExecuteScalarAsync(ct) is DateTime u ? u : null;
        }

        if (upper is null || upper.Value <= checkedThrough)
        {
            return (cachedLast, checkedThrough, false);
        }

        var bound = separate.ToArray();
        var since = AsNaive(checkedThrough);
        var through = AsNaive(upper.Value);
        var best = cachedLast;
        using (var outsideCommand = new NpgsqlCommand(DeadlockOutsideNewestSinceSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            outsideCommand.Parameters.AddWithValue(serverId);
            outsideCommand.Parameters.AddWithValue(since);
            outsideCommand.Parameters.AddWithValue(through);
            outsideCommand.Parameters.AddWithValue(bound);
            if (await outsideCommand.ExecuteScalarAsync(ct) is DateTime t && (best is null || t > best))
            {
                best = t;
            }
        }

        using var command = new NpgsqlCommand(DeadlockGraphsNewestSinceSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(since);
        command.Parameters.AddWithValue(through);
        command.Parameters.AddWithValue(bound);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Timestamp, Value = (object?)best ?? DBNull.Value });
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var xml = reader.IsDBNull(1) ? null : reader.GetString(1);
            if (!PerformanceMonitor.Common.DeadlockGraphDatabases.AllIn(xml, separate))
            {
                best = reader.GetDateTime(0);
                break;
            }
        }

        return (best, upper.Value, true);
    }

    /// <summary>The separately monitored databases as the SQL arm binds them (raw: each statement folds both
    /// sides with one lower()), or null when the context names none.</summary>
    internal static string[]? SeparateDatabases(AnalysisContext context)
    {
        var list = context.SeparatelyMonitoredDatabases;
        return list is not null && list.Count > 0 ? list.ToArray() : null;
    }

    /// <summary>Counts the window's deadlocks that do not belong wholly to the separately monitored databases
    /// (the engine's every-process rule, shared with the alert sweep). Deadlocks whose row's database is
    /// not separately monitored are counted in SQL; only the rest are read as graphs and parsed.</summary>
    internal static async Task<long> CountDeadlocksSkippingSeparateAsync(
        NpgsqlConnection connection, string outsideCountSql, string graphsSql, int serverId, DateTime start, DateTime end,
        IReadOnlyList<string> separate, System.Threading.CancellationToken ct, int commandTimeoutSeconds = FactCommandTimeoutSeconds)
    {
        var bound = separate.ToArray();
        long count;
        using (var countCommand = new NpgsqlCommand(outsideCountSql, connection) { CommandTimeout = commandTimeoutSeconds })
        {
            countCommand.Parameters.AddWithValue(serverId);
            countCommand.Parameters.AddWithValue(AsNaive(start));
            countCommand.Parameters.AddWithValue(AsNaive(end));
            countCommand.Parameters.AddWithValue(bound);
            countCommand.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(start));
            count = Convert.ToInt64(await countCommand.ExecuteScalarAsync(ct) ?? 0L);
        }

        using var command = new NpgsqlCommand(graphsSql, connection) { CommandTimeout = commandTimeoutSeconds };
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(AsNaive(start));
        command.Parameters.AddWithValue(AsNaive(end));
        command.Parameters.AddWithValue(bound);
        command.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(start));
        using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var xml = reader.IsDBNull(0) ? null : reader.GetString(0);
            if (!PerformanceMonitor.Common.DeadlockGraphDatabases.AllIn(xml, separate)) count++;
        }
        return count;
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

        await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

        long deadlockCount;
        if (context.SeparatelyMonitoredDatabases is { Count: > 0 } separate)
        {
            deadlockCount = await CountDeadlocksSkippingSeparateAsync(
                connection, DeadlockOutsideCountSql, DeadlockGraphsSql, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd,
                separate, context.CancellationToken);
        }
        else
        {
            using var command = new NpgsqlCommand(DeadlocksSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            command.Parameters.AddWithValue(context.ServerId);
            command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            command.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(context.TimeRangeStart));

            using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;
            deadlockCount = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
        }
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

    // SpidFilter keeps this in lockstep with the drill-down + viewer fetch on the apex
    // (Lite maps a missing blocker to spid 0 — see PgBlockingPairRowQuery).
    public const string BlockingChainSql = $@"
SELECT
    {PgBlockingPairRowQuery.LeadingColumns},
    blocked_sql_text,
    blocking_sql_text,
    {PgBlockingPairRowQuery.IdentityColumns},
    contentious_object,
    {PgBlockingPairRowQuery.TrailingIdentityColumns}
FROM v_blocked_process_reports
WHERE server_id = $1
AND   event_time >= $2
AND   event_time <= $3
AND   collection_time >= $4
{PgBlockingPairRowQuery.SpidFilter}
ORDER BY event_time DESC
LIMIT 5000";

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
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var command = new NpgsqlCommand(BlockingChainSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            command.Parameters.AddWithValue(context.ServerId);
            command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
            /* The partition-column floor, so a late-collected report still counts and old chunks stay closed. */
            command.Parameters.AddWithValue(PerformanceMonitor.Darling.Storage.EventWindowFloor.For(context.TimeRangeStart));

            var rows = new List<BlockingPairRow>();
            using (var reader = await command.ExecuteReaderAsync(context.CancellationToken))
            {
                while (await reader.ReadAsync(context.CancellationToken))
                    rows.Add(PgBlockingPairRowQuery.Read(reader));
            }

            // Always-on DMV blocking snapshot fallback (works when the blocked-process-report XE is empty,
            // e.g. AWS RDS). Merge BEFORE the empty check so DMV-only blocking still produces facts.
            /* A FACTORY that stamps the deadline, not the bare `connection.CreateCommand` method
               group (#2874). AppendDmvSnapshotRowsAsync sets only CommandText on what this returns, so
               a deadline set HERE is the only one that command can get - and a method group matches
               neither of the census regexes (there is no `(` after it), which is how this site read as
               clean through #2810's sweep of this collector's thirty-one commands and #2871's of the
               assembly's other thirty-two.

               FactCommandTimeoutSeconds, not a new number: this read is one of THIS collector's
               commands and shares the same 120s pass budget the other thirty-one were derived against,
               and its own doc comment already claims every command here. The value was not copied from
               the drill-down's 30 - that constant governs a different pass and #2871 left it alone
               deliberately. */
            await PgBlockingPairRowQuery.AppendDmvSnapshotRowsAsync(
                () =>
                {
                    var dmvCommand = connection.CreateCommand();
                    dmvCommand.CommandTimeout = FactCommandTimeoutSeconds;
                    return dmvCommand;
                },
                rows, context.ServerId, context.TimeRangeStart, context.TimeRangeEnd,
                context.CancellationToken);

            /* A master target leaves out the pairs of databases monitored as their own targets (the same
               rule the BLOCKING_EVENTS fact applies), so one chain does not page from both targets. */
            if (context.SeparatelyMonitoredDatabases is { Count: > 0 } separateDatabases)
                rows.RemoveAll(r => !string.IsNullOrEmpty(r.DatabaseName)
                    && separateDatabases.Contains(r.DatabaseName, StringComparer.OrdinalIgnoreCase));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

}
