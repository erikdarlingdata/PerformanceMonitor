using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Database;

using PerformanceMonitorLite;
using PerformanceMonitorLite.Services;
namespace PerformanceMonitorLite.Analysis;

public partial class DuckDbFactCollector
{
    /// <summary>#6640: the queries behind QUERY_SPILLS, most spilled pages first. Same window, filter and (database, query_hash)
    /// identity as the aggregate, so the names are the queries its spilling_query_count counts. Run only when spills exist.</summary>
    private static readonly string QuerySpillsRankedSql = @"
SELECT
    v.database_name,
    v.query_hash,
    SUM(v.delta_spills) AS spills
FROM v_query_stats AS v
WHERE v.server_id = $1
AND   v.collection_time >= $2
AND   v.collection_time <= $3
AND   v.delta_execution_count > 0
AND   v.delta_spills > 0
GROUP BY v.database_name, v.query_hash
ORDER BY spills DESC, v.database_name, v.query_hash
LIMIT " + FactRanked.MaxObjects + ";";

    /// <summary>
    /// Collects query-level aggregate facts from query_stats.
    /// Focuses on spills (memory grant misestimates) and high-parallelism queries.
    /// </summary>
    private async Task CollectQueryStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var plan = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", context.TimeRangeStart, context.CancellationToken);
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            /* #2705/#2999: max_dop on v_query_stats is sys.dm_exec_query_stats' lifetime-max for the
               plan's time in cache (QueryStatExtremes.cs's doctrine — same semantics as
               max/min_cpu_ms), so a plan compiled before 'max degree of parallelism' was lowered
               keeps reporting the old higher DOP until it is evicted or recompiled.
               get_top_queries_by_cpu's parallel_only description carries this caveat for a human
               reader; current_maxdop applies the same provable-tell reasoning QueryStatExtremes uses
               for CPU/elapsed: a max_dop reading that EXCEEDS what the server's current maxdop
               setting can produce right now is impossible under today's configuration, so it provably
               predates whatever change set that configuration and must not be counted. Lowering
               MAXDOP is the ordinary remediation for this very finding, so without the cross-check
               the finding survives its own fix. A current_maxdop of 0 (unlimited) or unknown (no
               v_server_config row yet) makes no configuration impossible, so the count is unchanged
               in both of those cases. The setting is the newest capture at or before $3, the analysis
               window's end, so a pass over an earlier window is held to the setting that window had.
               LEFT JOIN ... ON true rather than a correlated subquery: the CTE is at most one row, so
               the join stays one-to-one and the config read is evaluated once, not per query row. */
            cmd.CommandText = @"
WITH current_maxdop AS
(
    SELECT value_in_use
    FROM v_server_config
    WHERE server_id = $1
    AND   configuration_name = 'max degree of parallelism'
    AND   capture_time <= $3
    ORDER BY capture_time DESC
    LIMIT 1
)
SELECT
    SUM(v.delta_spills) AS total_spills,
    /* #5633: DISTINCT (database, query_hash), like spilling_queries below. This count drives the QUERY_HIGH_DOP threshold (5/20) and
       its N-queries text, and v_query_stats holds one row per query per snapshot, so one high-DOP query seen in 20 collections
       scored at full severity as 20 queries. A NULL hash collapses to one per database, as for spilling_queries. */
" + QueryHighDopSignature.HighDopQueriesCountSql + @"
    COUNT(DISTINCT CASE WHEN v.delta_spills > 0
                        THEN COALESCE(v.database_name, '') || '/' || COALESCE(v.query_hash, '') END) AS spilling_queries,
    SUM(v.delta_execution_count) AS total_executions,
    SUM(v.delta_worker_time) AS total_cpu_time_us
FROM v_query_stats AS v
LEFT JOIN current_maxdop AS m ON true
WHERE v.server_id = $1
AND   v.collection_time >= $2
AND   v.collection_time <= $3
AND   v.delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            cmd.CommandText = plan.Apply(cmd.CommandText);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

            /* #6640: SUM(BIGINT) is a HUGEINT, which DuckDB.NET returns as BigInteger: Convert.ToDouble throws on it, so read it with ToInt64.
               Name the spilling queries, most spilled pages first. A second small read on the same connection,
               only when the aggregate found spills. The aggregate's figures above are unchanged. */
            var spillRanked = new List<RankedObject>();
            if (totalSpills > 0)
            {
                await reader.CloseAsync();
                /* Its own try: a throw here (the HUGEINT read cost every QUERY_STATS fact once, run 39981348396) loses the names
                   and nothing else. record: false logs it without marking the family unread, since the facts below are whole. */
                try
                {
                    using var rankCmd = connection.CreateCommand();
                    rankCmd.CommandText = QuerySpillsRankedSql;
                    rankCmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                    rankCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                    rankCmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
                    rankCmd.CommandText = plan.Apply(rankCmd.CommandText);
                    using var rankReader = await rankCmd.ExecuteReaderAsync(context.CancellationToken);
                    while (await rankReader.ReadAsync(context.CancellationToken))
                    {
                        var db = rankReader.IsDBNull(0) ? null : rankReader.GetString(0);
                        var hash = rankReader.IsDBNull(1) ? null : rankReader.GetString(1);
                        var pages = rankReader.IsDBNull(2) ? 0.0 : ToInt64(rankReader.GetValue(2));
                        spillRanked.Add(new RankedObject(FactRanked.QueryName(db, hash), string.IsNullOrEmpty(db) ? null : db, pages));
                    }
                }
                catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
                {
                    spillRanked.Clear();
                    ReportCollectionFailure(ex, context, record: false);
                }
            }

            if (totalSpills > 0)
            {
                /* #7884: the period and coverage stamps the wait facts carry, so the scorer grades pages per OBSERVED hour
                   (FactScorer.ObservedHours) and the same spill rate reads the same at every hours_back. */
                var spillMetadata = new Dictionary<string, double>
                {
                    ["total_spills"] = totalSpills,
                    /* #5633: DISTINCT queries (database + query_hash) with a spill, not snapshot rows. */
                    ["spilling_query_count"] = spillingQueries,
                    ["total_executions"] = totalExecutions,
                    ["period_duration_ms"] = context.PeriodDurationMs
                };
                FactCollectorHelpers.AddCoverageFraction(spillMetadata, context);

                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_SPILLS",
                    Value = totalSpills,
                    ServerId = context.ServerId,
                    ObjectName = spillRanked.Count > 0 ? spillRanked[0].ObjectName : null,
                    Ranked = spillRanked,
                    Metadata = spillMetadata
                });
            }

            if (highDopQueries > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_HIGH_DOP",
                    Value = highDopQueries,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["high_dop_query_count"] = highDopQueries,
                        ["total_cpu_time_us"] = totalCpuTimeUs,
                        ["total_executions"] = totalExecutions
                    }
                });
            }
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Detects parameter-sensitive cached plans: a single query_plan_hash whose
    /// per-execution worker time varies wildly — one plan serving very different
    /// parameter values. Emits one aggregate PARAMETER_SENSITIVITY fact.
    /// Note min_*/max_* are cumulative over the plan's cached lifetime, so the
    /// finding means "this plan, active now, has a history of widely varying cost".
    /// </summary>
    private async Task CollectParameterSensitivityFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var plan = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", context.TimeRangeStart, context.CancellationToken);
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
WITH svr AS
(
    -- creation_time is the MONITORED SERVER's local wall clock -- QueryStatsCollector ships the
    -- dm_exec_query_stats value verbatim -- while the window bound is naive UTC off DateTime.UtcNow.
    -- De-skewing the column by the collected offset is what lets the compiled-before-the-window test
    -- compare a single frame. Untranslated, a negative offset admits exactly the in-window plans this
    -- predicate exists to exclude, and a positive one discards plans that legitimately predate the
    -- window; either way a wide min/max worker-time spread stops being evidence of parameter
    -- sensitivity and becomes an artefact of plan age. COALESCE to 0 because server_properties is an
    -- on-load collector, so an absent offset is the state every server passes through on its first
    -- cycle -- refusing the read there would pre-empt the two answers that outrank any window. The
    -- CTE returns exactly one row, so no plan is lost to it.
    --
    -- #4821: this offset is now only the ROUGH first filter. One offset for every row is an hour off for a
    -- plan compiled before the last daylight-saving change, so the read also returns the raw creation_time
    -- beside the zone (time_zone_id, SQL Server 2022 and later, from the same newest row) and the exact
    -- test runs in C# with the offset in force when each plan was compiled (PlanCreationClock).
    SELECT
        COALESCE(p.utc_offset_minutes, 0) AS offset_minutes,
        p.time_zone_id
    FROM (SELECT 1) AS one
    LEFT JOIN
    (
        SELECT utc_offset_minutes, time_zone_id
        FROM v_server_properties
        WHERE server_id = $1
        AND   utc_offset_minutes IS NOT NULL
        ORDER BY collection_time DESC
        LIMIT 1
    ) AS p
      ON 1 = 1
),
latest AS
(
    -- #9195: each plan's newest row by one hash aggregate, not ROW_NUMBER() over the window. The window sorted every row of the
    -- analysis window, so its time and memory grew with the window (a week's rows against a day's). The newest row is the
    -- largest (collection_time, collection_id), the old ORDER BY collection_time DESC, collection_id DESC, and the grouping
    -- is the old PARTITION BY (a NULL key groups with its own kind in both). Its fields ride in the struct, so they are the winner row's.
    SELECT
        g.query_hash,
        g.query_plan_hash,
        g.database_name,
        g.w.execution_count AS execution_count,
        g.w.creation_time AS creation_time,
        g.w.creation_time - svr.offset_minutes * INTERVAL '1' MINUTE AS creation_time_utc,
        svr.offset_minutes AS server_offset_minutes,
        svr.time_zone_id AS server_time_zone_id,
        g.w.min_worker_time AS min_worker_time,
        g.w.max_worker_time AS max_worker_time,
        g.w.min_grant_kb AS min_grant_kb,
        g.w.max_grant_kb AS max_grant_kb,
        g.w.min_spills AS min_spills,
        g.w.max_spills AS max_spills
    FROM
    (
        SELECT
            database_name,
            query_hash,
            query_plan_hash,
            max(
            {
                'collection_time': collection_time,
                'collection_id': collection_id,
                'execution_count': execution_count,
                'creation_time': creation_time,
                'min_worker_time': min_worker_time,
                'max_worker_time': max_worker_time,
                'min_grant_kb': min_grant_kb,
                'max_grant_kb': max_grant_kb,
                'min_spills': min_spills,
                'max_spills': max_spills
            }) AS w
        FROM v_query_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   delta_execution_count > 0
        GROUP BY database_name, query_hash, query_plan_hash
    ) AS g, svr
)
SELECT
    min_worker_time,
    max_worker_time,
    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE PRECISION / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    creation_time,
    server_offset_minutes,
    server_time_zone_id,
    database_name,
    query_hash,
    query_plan_hash
FROM latest
WHERE 1 = 1
" + ParameterSensitivitySignature.PredicateSql("creation_time_utc <= $4") + @"
ORDER BY worker_ratio DESC, database_name, query_hash, query_plan_hash";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });
            /* $4: the first filter's bound, opened by an hour (#4821). The exact test is made below, per row. */
            cmd.Parameters.Add(new DuckDBParameter { Value = PlanCreationClock.RoughBound(context.TimeRangeStart) });

            var offenderCount = 0;
            var worstRatio = 0.0;
            var worstMinWorker = 0L;
            var worstMaxWorker = 0L;
            var worstGrantRatio = 0.0;
            var worstSpillDivergence = 0;
            /* #6640: the kept offenders, worst first, at most FactRanked.MaxObjects. The name is database/query_hash/
               query_plan_hash: the plan hash is in the name because Figures holds numbers only, and one query with
               two plans is two offenders. */
            var ranked = new List<RankedObject>();

            cmd.CommandText = plan.Apply(cmd.CommandText);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                /* The exact compiled-before-the-window test, with the offset in force when the plan was
                   created (#4821). The SQL's own filter is only the rough first pass, so the cap of twenty
                   is applied here, after it. */
                if (reader.IsDBNull(5)
                    || !PlanCreationClock.CompiledBeforeWindow(
                        PlanCreationClock.ClockFrom(reader, 6, 7), reader.GetDateTime(5), context.TimeRangeStart))
                {
                    continue;
                }

                if (offenderCount >= FactThresholds.ParameterSensitivity.OffenderCap)
                {
                    break;
                }

                // Rows arrive ordered by worker_ratio DESC — the first row is the worst offender.
                if (offenderCount == 0)
                {
                    worstMinWorker = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
                    worstMaxWorker = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
                    worstRatio = reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2));
                    worstGrantRatio = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3));
                    worstSpillDivergence = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4));
                }
                if (ranked.Count < FactRanked.MaxObjects)
                {
                    var db = reader.IsDBNull(8) ? null : reader.GetString(8);
                    var hash = reader.IsDBNull(9) ? null : reader.GetString(9);
                    var planHash = reader.IsDBNull(10) ? string.Empty : reader.GetString(10);
                    ranked.Add(new RankedObject(
                        FactRanked.QueryName(db, hash, planHash),
                        string.IsNullOrEmpty(db) ? null : db,
                        reader.IsDBNull(2) ? 0.0 : Convert.ToDouble(reader.GetValue(2)),
                        new Dictionary<string, double>
                        {
                            ["min_worker_us"] = reader.IsDBNull(0) ? 0.0 : ToInt64(reader.GetValue(0)),
                            ["max_worker_us"] = reader.IsDBNull(1) ? 0.0 : ToInt64(reader.GetValue(1)),
                            ["grant_ratio"] = reader.IsDBNull(3) ? 0.0 : Convert.ToDouble(reader.GetValue(3)),
                            ["spill_divergence"] = reader.IsDBNull(4) ? 0 : Convert.ToInt32(reader.GetValue(4)),
                        }));
                }
                offenderCount++;
            }

            if (offenderCount == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PARAMETER_SENSITIVITY",
                Value = worstRatio,
                ServerId = context.ServerId,
                ObjectName = ranked[0].ObjectName,
                Ranked = ranked,
                Metadata = new Dictionary<string, double>
                {
                    ["offender_count"] = offenderCount,
                    ["worst_ratio"] = worstRatio,
                    ["worst_min_worker_us"] = worstMinWorker,
                    ["worst_max_worker_us"] = worstMaxWorker,
                    ["worst_grant_ratio"] = worstGrantRatio,
                    ["grant_divergence"] = worstGrantRatio >= FactThresholds.ParameterSensitivity.GrantDivergenceRatio ? 1 : 0,
                    ["spill_divergence"] = worstSpillDivergence
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

    /// <summary>One statement of the plan regression pass: the wait for its read lock, the statement alone, the whole hold and the rows it returned.</summary>
    internal struct PlanRegressionStatementTime
    {
        public TimeSpan Wait;
        public TimeSpan Statement;
        public TimeSpan Held;
        public int Rows;

        /// <summary>#9888: the rows the statement returned beyond the cost cap, which the read did not price further.</summary>
        public int LeftOut;

        /// <summary>#9888: true when the rows came from the kept result of an earlier read, so no statement ran.</summary>
        public bool FromKeep;

        /// <summary>#9957: on a read that ran the statement, how many reads the keep answered since the previous statement was stored (the hit rate, since a keep answer is too fast to log).</summary>
        public int KeepHitsSincePreviousStatement;
    }

    /// <summary>
    /// A test seam: runs after the failures read has let go of the store's read lock and before the cost read asks for it, so a
    /// test can see what the lock allows in between (#9353). Null in production.
    /// </summary>
    internal Action? AfterPlanRegressionFailuresReadForTests { get; set; }

    /// <summary>
    /// A test seam: runs before each database's cost read after the first, with the store's read lock let go, so a test can see that a
    /// writer gets in between the databases (#9353). Null in production.
    /// </summary>
    internal Action? AfterPlanRegressionDatabaseReadForTests { get; set; }

    /// <summary>A test seam: the longest hold a batch of databases aims for (#9353), so a test can make every database a batch of its own (zero) or none. Null in production.</summary>
    internal TimeSpan? PlanRegressionHoldTargetForTests { get; set; }

    /// <summary>
    /// #9195, #9353: one Information line when the plan regression pass took over <see cref="ReadLockWaitTally.SlowReadThreshold"/>,
    /// with the time split between its steps: the archive read plan, each statement (its read lock wait, its own time and its rows),
    /// the longest single hold of the read lock, and the plan check that follows once the lock is let go with the plan fetches it
    /// made. A fast pass logs nothing. Counts and times only.
    /// </summary>
    private static void ReportSlowPlanRegressionPass(
        AnalysisContext context, TimeSpan total, TimeSpan archivePlan, PlanRegressionStatementTime failures, PlanRegressionStatementTime cost,
        TimeSpan verify, int planFetches, DuckDbProfileTrail profiles)
    {
        if (total <= ReadLockWaitTally.SlowReadThreshold) return;

        static long Ms(TimeSpan t) => (long)t.TotalMilliseconds;
        var longestHold = failures.Held > cost.Held ? failures.Held : cost.Held;
        AppLogger.Info("DuckDbFactCollector",
            string.Create(CultureInfo.InvariantCulture,
                $"Plan regression read took {Ms(total)} ms for {context.ServerName} (server {context.ServerId}): archive read plan {Ms(archivePlan)} ms, " +
                $"failures read {Ms(failures.Statement)} ms for {failures.Rows} rows (read lock wait {Ms(failures.Wait)} ms), " +
                $"cost read {Ms(cost.Statement)} ms for {cost.Rows} candidates{DescribeCostRead(cost)} (read lock wait {Ms(cost.Wait)} ms), " +
                $"longest single read lock hold {Ms(longestHold)} ms, plan check {Ms(verify)} ms with {planFetches} plan fetches{profiles.Describe()}"));
    }

    /// <summary>#9888: the reply's sentence for a cost read that found no candidate: the statement still scanned the window, and that is its time.</summary>
    private static string PlanRegressionCostFloorNote(PlanRegressionStatementTime cost) =>
        string.Create(CultureInfo.InvariantCulture,
            $"The plan regression cost read found no candidate and its scan of the archive window took {cost.Statement.TotalMilliseconds:0} ms, a floor that stays on a server with nothing to price.");

    /// <summary>#9888: what the cost read did beyond its time: the rows left out by the cap, an answer from the kept result, or a read that found nothing.</summary>
    private static string DescribeCostRead(PlanRegressionStatementTime cost) =>
        (cost.FromKeep ? " answered from an earlier read" : "")
        + (cost.KeepHitsSincePreviousStatement > 0 ? $" (the keep answered {cost.KeepHitsSincePreviousStatement} reads since the previous statement)" : "")
        + (cost.LeftOut > 0 ? $", {cost.LeftOut} left out past the cap of {PlanRegressionCostCap}" : "")
        + (cost.Rows == 0 && !cost.FromKeep ? " (found no candidate, so this is the floor of the statement's scan)" : "");

    /// <summary>
    /// Detects plan regressions: a query whose currently-active plan has per-execution
    /// cost >= 2x the best plan that query is known to perform well with. Emits one
    /// aggregate PLAN_REGRESSION fact. Sourced from Query Store (v_query_store_stats);
    /// no fact when Query Store is not enabled on the monitored databases.
    /// Unlike other collectors this windows on last_execution_time (14-day comparison
    /// window), NOT collection_time — see plan note.
    /// #5630/#5631: a candidate whose statement carries OPTION (RECOMPILE), whose newer plan only returns more rows,
    /// or whose two plans differ in compiled values alone, is dropped before the fact
    /// (<see cref="VerifyPlanRegressionInputsAsync"/>).
    /// </summary>
    internal async Task CollectPlanRegressionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* #3902: cleared first, so a read that fails below leaves "not known" for the drill-down rather
           than a list some earlier pass stamped on a reused context. */
        context.PlanRegressionOffenders = null;
        context.PlanRegressionFailures = null;
        context.PlanRegressionCostNote = null;

        try
        {
            var passStarted = Stopwatch.GetTimestamp();
            /* #9195: the profile files this pass writes when DuckDbReadProfile is on, named on the slow-read line. */
            using var profiles = DuckDbProfileTrail.Begin();
            var candidates = new List<PlanRegressionCandidate>();
            var failures = new List<PlanRegressionFailureRule.Failure>();
            var failuresRead = default(PlanRegressionStatementTime);
            var costRead = default(PlanRegressionStatementTime);

            /* #9957: the cost statement's window first, because the archive plan must be taken for its start: a read whose end is the clock
               ends at the newest row the store holds, so its start is earlier than the context's. */
            var costWindow = await ResolvePlanRegressionCostWindowAsync(context);
            var plan = await _duckDb.PlanArchiveReadAsync("query_store_stats", "last_execution_time", costWindow.ScanStart, context.CancellationToken);
            var archivePlanTime = Stopwatch.GetElapsedTime(passStarted);

            /* #9353: one lock acquisition and one connection per statement, not one across both. The two reads are independent
               facts, so a write that lands between them changes neither's meaning, and a housekeeping writer waiting on its 5 s
               budget now gets in between the statements instead of waiting out both. */
            {
                var waitStarted = Stopwatch.GetTimestamp();
                using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
                var heldFrom = Stopwatch.GetTimestamp();
                failuresRead.Wait = Stopwatch.GetElapsedTime(waitStarted, heldFrom);
                using var connection = _duckDb.CreateConnection();
                await connection.OpenAsync(context.CancellationToken);

                /* #6267: the queries whose timeouts or errors rose. The cost read below counts Regular runs only, so a plan
                   that regresses into timeouts or errors is found here. A failed read costs the failure half only. */
                var statementFrom = Stopwatch.GetTimestamp();
                try
                {
                    failures = await ReadPlanRegressionFailuresAsync(connection, context, plan);
                }
                catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
                {
                    ReportCollectionFailure(ex, context);
                }

                failuresRead.Statement = Stopwatch.GetElapsedTime(statementFrom);
                failuresRead.Rows = failures.Count;
                failuresRead.Held = Stopwatch.GetElapsedTime(heldFrom);
            }

            AfterPlanRegressionFailuresReadForTests?.Invoke();

            /* #9353: the cost statement is the pass's longest hold of the read lock, so it is read one database at a time. */
            (candidates, costRead) = await ReadPlanRegressionCostAsync(context, plan, costWindow);
            costRead.Rows = candidates.Count;

            /* #9888: a read that found nothing raises no fact, so the reply carries the floor of its scan through the context. */
            if (candidates.Count == 0 && !costRead.FromKeep)
                context.PlanRegressionCostNote = PlanRegressionCostFloorNote(costRead);

            /* #5630: the lock and the connection are released before any plan is fetched: a fetch is a network
               call to the monitored server and must not hold the store's read lock. */
            var verifyStarted = Stopwatch.GetTimestamp();
            var verification = await VerifyPlanRegressionInputsAsync(context, candidates);
            ReportSlowPlanRegressionPass(
                context, Stopwatch.GetElapsedTime(passStarted), archivePlanTime, failuresRead, costRead,
                Stopwatch.GetElapsedTime(verifyStarted), verification.PlanFetches, profiles);
            var kept = verification.Kept;

            var offenderCount = kept.Count;
            var offenders = new List<PlanRegressionOffender>();
            var worstFactor = 0.0;
            var worstQueryId = 0L;
            var worstLatestCpu = 0.0;
            var worstBestCpu = 0.0;
            var worstDimension = 1;
            var worstLatestForced = 0;
            var worstForceFailures = 0L;
            DateTime? worstBestLastExec = null;

            for (var i = 0; i < kept.Count; i++)
            {
                var row = kept[i];

                // Rows arrive ordered by the CPU the regression lost (#7417), the factor second — the first kept row is the worst offender.
                if (i == 0)
                {
                    worstQueryId = row.QueryId;
                    worstLatestForced = row.LatestIsForced;
                    worstForceFailures = row.ForceFailureCount;
                    worstFactor = row.RegressionFactor;
                    worstLatestCpu = row.LatestCpu;
                    worstBestCpu = row.BestCpu;
                    worstBestLastExec = row.BestLastExec;
                    // Which CASE branch fired, not which raw ratio is larger (#2138):
                    // CPU has PRECEDENCE in the scoring, so a row with cpu 2.5x and duration 10x is a
                    // CPU-detected regression at 2.5 — comparing magnitudes would mislabel it duration.
                    var cpuRatio = row.BestCpu > 0 ? row.LatestCpu / row.BestCpu : 0.0;
                    worstDimension = cpuRatio >= 2 ? 1 : 2; // 1 = cpu, 2 = duration
                }

                /* #3902: both keys are join keys of the comparison above, so neither is ever NULL here. Two
                   replicas of one query are two rows and one offender. */
                var offender = new PlanRegressionOffender(row.DatabaseName, row.QueryId);
                if (!offenders.Contains(offender))
                    offenders.Add(offender);
            }

            context.PlanRegressionOffenders = offenders;
            context.PlanRegressionFailures = failures;

            if (offenderCount == 0 && failures.Count == 0) return;

            var fact = new Fact
            {
                Source = "queries",
                Key = "PLAN_REGRESSION",
                Value = worstFactor,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["offender_count"] = offenderCount,
                    ["latest_is_forced"] = worstLatestForced,
                    ["force_failure_count"] = worstForceFailures,
                    /* #3953 parity: Lite reads its raw slice (0); Darling's 1 is the interval table. */
                    ["plan_regression_source"] = 0,
                    /* #5630, the names Darling's fact carries: candidates dropped for any of the three reasons (the
                       statement carries OPTION (RECOMPILE), the newer plan returns far more rows, or the plans differ
                       in compiled values over the same statistics), and kept
                       candidates whose inputs could not be compared (no plan fetched, or the plans carry no
                       compiled values). Only counts leave the comparison, never a plan or a value. */
                    ["cross_input_excluded_count"] = verification.CrossInputExcluded,
                    ["inputs_unverified_count"] = verification.InputsUnverified,
                    /* #5631: how the worst kept offender's inputs were judged: 1 same compiled values, 2 statistics
                       changed between the compiles, 0 not checked. The advice reads it so it claims no cause the
                       data does not show. */
                    ["worst_input_basis"] = offenderCount > 0 ? kept[0].InputBasis : 0,
                    /* #9888: the cost read's candidates this fact priced, and the rows it left out past the cap. */
                    ["cost_candidates_priced"] = costRead.Rows,
                    ["cost_candidates_left_out"] = costRead.LeftOut,
                }
            };

            /* The cost figures exist only when a plan's cost regressed: a fact raised by failures alone carries none, so the
               advice reads their absence as "no cost regression" and not as a zero factor (#6267). */
            if (offenderCount > 0)
            {
                /* #7417: the LEAD offender's factor (the one that lost the most CPU), the figure the advice pairs with the
                   lead's own per-execution costs below. Another kept row can have a larger factor. */
                fact.Metadata["lead_regression_factor"] = worstFactor;
                fact.Metadata["worst_query_id"] = worstQueryId;
                fact.Metadata["latest_cpu_per_exec_us"] = worstLatestCpu;
                fact.Metadata["best_cpu_per_exec_us"] = worstBestCpu;
                fact.Metadata["regressed_dimension"] = worstDimension;
            }

            /* #6267: the failure figures, and the larger of the two values. */
            fact.Value = Math.Max(worstFactor, PlanRegressionFailureRule.AddMetadata(fact.Metadata, failures));

            /* #3953: the worst offender's best plan's age at the window's end, in days. Absent only if the read
               returned no timestamp, which a plan with executions in the window cannot do. */
            if (worstBestLastExec is DateTime bestLastExec)
            {
                fact.Metadata["best_plan_age_days"] = Math.Max(0.0, (context.TimeRangeEnd - bestLastExec).TotalDays);
            }

            facts.Add(fact);
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var plan = await _duckDb.PlanArchiveReadAsync("procedure_stats", "collection_time", context.TimeRangeStart, context.CancellationToken);
            using var readLock = _duckDb.AcquireReadLock(context.CancellationToken);
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(context.CancellationToken);

            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us,
    SUM(delta_elapsed_time) AS total_elapsed_time_us,
    SUM(delta_logical_reads) AS total_logical_reads
FROM v_procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

            cmd.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
            cmd.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

            cmd.CommandText = plan.Apply(cmd.CommandText);
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var distinctProcs = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var totalExecs = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var totalCpuUs = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalElapsedUs = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalReads = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

            if (totalExecs == 0) return;

            facts.Add(new Fact
            {
                Source = "queries",
                Key = "PROCEDURE_STATS",
                Value = totalCpuUs,
                ServerId = context.ServerId,
                Metadata = new Dictionary<string, double>
                {
                    ["distinct_procedures"] = distinctProcs,
                    ["total_executions"] = totalExecs,
                    ["total_cpu_time_us"] = totalCpuUs,
                    ["total_elapsed_time_us"] = totalElapsedUs,
                    ["total_logical_reads"] = totalReads,
                    ["avg_cpu_per_exec_us"] = totalExecs > 0 ? (double)totalCpuUs / totalExecs : 0
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

    /// <summary>
    /// The plan fetch of the plan advisory fact (<see cref="CollectPlanAdvisoryFactsAsync"/>) and of its drill-down: $1 server, $2 window start, $3 window end.
    /// #8570: the rank reads only thin columns (the row's collection_id and its sort keys), and the ten winning rows' plan XML is read by collection_id after the
    /// limit. The earlier statement carried every in-window row's plan XML through the window's sort, so a window of a few hundred thousand rows sorted gigabytes of
    /// plan text to keep ten plans. The plan column is only tested for NULL in the rank. The result is the same ten plans in the same order (the Darling twin is
    /// <c>PgFactCollector.PlanAdvisorySql</c>).
    /// </summary>
    internal const string PlanAdvisorySql = @"
SELECT w.query_plan_xml
FROM
(
    SELECT collection_id, delta_worker_time
    FROM
    (
        SELECT
            collection_id,
            delta_worker_time,
            /* #7886: one row per plan. v_query_stats holds one row per query per snapshot, and every snapshot
               row of a cached plan resolves to the same plan XML. Without this rank the hottest plan fills all
               ten slots and is parsed once per snapshot, so PLAN_WARNING's warning_count and critical_count
               multiply by its snapshot count and the two facts cover one to three plans instead of ten. The
               plan's costliest snapshot row stands for it, and the limit below counts distinct plans. A plan is
               a statement's plan in one database: query_plan_hash is a shape hash that two statements with one
               shape share, as does one statement in two databases, and a hashless row's plan_handle is shared by
               every statement in its batch, so database_name and query_hash stay in the key. */
            ROW_NUMBER() OVER (PARTITION BY database_name, query_hash, COALESCE(query_plan_hash, plan_handle) ORDER BY delta_worker_time DESC) AS plan_rank
        FROM v_query_stats
        WHERE server_id = $1
        AND   collection_time >= $2
        AND   collection_time <= $3
        AND   query_plan_xml IS NOT NULL
    ) AS ranked
    WHERE plan_rank = 1
    ORDER BY delta_worker_time DESC
    LIMIT 10
) AS top
JOIN v_query_stats AS w
  ON w.collection_id = top.collection_id
WHERE w.server_id = $1
AND   w.collection_time >= $2
AND   w.collection_time <= $3
ORDER BY top.delta_worker_time DESC";

    /// <summary>
    /// WS4: plan-XML advisories. Parses the already-collected query plans of the top queries by
    /// cost with the shared ShowPlanParser/PlanAnalyzer and emits two advise-only facts —
    /// MISSING_INDEX (Value = distinct suggested indexes) and PLAN_WARNING (Value = actionable
    /// warnings). The specifics ride in the finding drill-down (DrillDownCollector); Fact.Metadata
    /// is numeric only. Mirrors the Dashboard SqlServerFactCollector against query_stats.query_plan_xml.
    /// </summary>
    private async Task CollectPlanAdvisoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var planXmls = new List<string>();

            var plan = await _duckDb.PlanArchiveReadAsync("query_stats", "collection_time", context.TimeRangeStart, context.CancellationToken);
            using (var readLock = _duckDb.AcquireReadLock(context.CancellationToken))
            using (var connection = _duckDb.CreateConnection())
            {
                await connection.OpenAsync(context.CancellationToken);

                using var command = connection.CreateCommand();
                command.CommandText = DuckDbFactCollector.PlanAdvisorySql;
                command.Parameters.Add(new DuckDBParameter { Value = context.ServerId });
                command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeStart });
                command.Parameters.Add(new DuckDBParameter { Value = context.TimeRangeEnd });

                command.CommandText = plan.Apply(command.CommandText);
                using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    if (!reader.IsDBNull(0))
                        planXmls.Add(reader.GetString(0));
                }
            }

            // Read lock released above; parse off the lock (CPU-only, no DB).
            if (planXmls.Count == 0)
                return;

            var summary = PlanAdvisoryAggregator.SummarizeCancellable(planXmls, App.AnalyzerConfig, context.CancellationToken);

            if (summary.MissingIndexCount > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "MISSING_INDEX",
                    Value = summary.MissingIndexCount,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["index_count"] = summary.MissingIndexCount,
                        ["max_impact"] = summary.MaxImpact
                    }
                });
            }

            if (summary.WarningCount > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "PLAN_WARNING",
                    Value = summary.WarningCount,
                    ServerId = context.ServerId,
                    Metadata = new Dictionary<string, double>
                    {
                        ["warning_count"] = summary.WarningCount,
                        ["critical_count"] = summary.CriticalCount
                    }
                });
            }
        }
        catch (Exception ex) when (!AnalysisAbandon.IsExpected(ex, context.CancellationToken))
        {
            // query_stats / plan parse may be unavailable — skip, the advisory is best-effort.
            // An abandonment is NOT swallowed here (#2443).
            ReportCollectionFailure(ex, context);
        }
    }

}
