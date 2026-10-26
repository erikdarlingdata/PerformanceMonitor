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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Analysis.Baselines;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.PlanAnalysis;

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgFactCollector
{
    /* #2705: max_dop on v_query_stats is sys.dm_exec_query_stats' lifetime-max for the plan's time in
       cache (QueryStatExtremes.cs's doctrine — same semantics as max/min_cpu_ms), so a plan compiled
       before 'max degree of parallelism' was lowered keeps reporting the old higher DOP until it is
       evicted or recompiled. get_top_queries_by_cpu's tool description has always carried this caveat
       for a human reader; this fact collector had no equivalent guard and fed the raw lifetime value
       into a QUERY_HIGH_DOP finding at full confidence. current_maxdop applies the same provable-tell
       reasoning QueryStatExtremes uses for CPU/elapsed: a max_dop reading that EXCEEDS what the
       server's current maxdop setting can produce right now is impossible under today's configuration,
       so it provably predates whatever change set that configuration and must not be counted. A
       current_maxdop of 0 (unlimited) or unknown (no server_config row yet) makes no configuration
       impossible, so the count is unchanged in both of those cases. */
    public const string QueryStatsSql = @"
WITH current_maxdop AS
(
    SELECT value_in_use
    FROM server_config
    WHERE server_id = $1
    AND   configuration_name = 'max degree of parallelism'
    ORDER BY capture_time DESC
    LIMIT 1
)
SELECT
    SUM(v.delta_spills) AS total_spills,
    /* #5633: DISTINCT (database, query_hash), like spilling_queries below. This count drives the QUERY_HIGH_DOP threshold (5/20) and
       its N-queries text, and v_query_stats holds one row per query per snapshot, so one high-DOP query seen in 20 collections
       scored at full severity as 20 queries. A NULL hash collapses to one per database, as for spilling_queries. */
    COUNT(DISTINCT CASE WHEN v.max_dop > 8
                         AND (m.value_in_use IS NULL OR m.value_in_use = 0 OR v.max_dop <= m.value_in_use)
                        THEN COALESCE(v.database_name, '') || '/' || COALESCE(v.query_hash, '') END) AS high_dop_queries,
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

    /// <summary>#6640: the queries behind QUERY_SPILLS, most spilled pages first. Same window, same filter and same
    /// (database, query_hash) identity as the aggregate above, so the names are exactly the queries its spilling_query_count counts.
    /// Run only when the aggregate found spills, and capped at the ranked-list cap.</summary>
    internal static readonly string QuerySpillsRankedSql = @"
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
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(QueryStatsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            if (!await reader.ReadAsync(context.CancellationToken)) return;

            var totalSpills = reader.IsDBNull(0) ? 0L : ToInt64(reader.GetValue(0));
            var highDopQueries = reader.IsDBNull(1) ? 0L : ToInt64(reader.GetValue(1));
            var spillingQueries = reader.IsDBNull(2) ? 0L : ToInt64(reader.GetValue(2));
            var totalExecutions = reader.IsDBNull(3) ? 0L : ToInt64(reader.GetValue(3));
            var totalCpuTimeUs = reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4));

            /* #6640: name the spilling queries, most spilled pages first. A second small read on the same connection,
               only when the aggregate found spills. The aggregate's figures above are unchanged. */
            var spillRanked = new List<RankedObject>();
            if (totalSpills > 0)
            {
                await reader.CloseAsync();
                /* Its own try, as in Lite: a throw here loses the names and nothing else (record: false, the facts below are whole). */
                try
                {
                    using var rankCmd = new NpgsqlCommand(QuerySpillsRankedSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
                    rankCmd.Parameters.AddWithValue(context.ServerId);
                    rankCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                    rankCmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));
                    using var rankReader = await rankCmd.ExecuteReaderAsync(context.CancellationToken);
                    while (await rankReader.ReadAsync(context.CancellationToken))
                    {
                        var db = rankReader.IsDBNull(0) ? null : rankReader.GetString(0);
                        var hash = rankReader.IsDBNull(1) ? null : rankReader.GetString(1);
                        var pages = rankReader.IsDBNull(2) ? 0.0 : Convert.ToDouble(rankReader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture);
                        spillRanked.Add(new RankedObject(FactRanked.QueryName(db, hash), string.IsNullOrEmpty(db) ? null : db, pages));
                    }
                }
                catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
                {
                    spillRanked.Clear();
                    ReportCollectionFailure(ex, context, record: false);
                }
            }

            if (totalSpills > 0)
            {
                facts.Add(new Fact
                {
                    Source = "queries",
                    Key = "QUERY_SPILLS",
                    Value = totalSpills,
                    ServerId = context.ServerId,
                    ObjectName = spillRanked.Count > 0 ? spillRanked[0].ObjectName : null,
                    Ranked = spillRanked,
                    Metadata = new Dictionary<string, double>
                    {
                        ["total_spills"] = totalSpills,
                        /* #5633: DISTINCT queries (database + query_hash) with a spill, not snapshot rows. */
                        ["spilling_query_count"] = spillingQueries,
                        ["total_executions"] = totalExecutions
                    }
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>
    /// How many parameter-sensitive plans the fact counts and how many the read reports: the <c>LIMIT</c> this
    /// statement carried until the compiled-before-the-window test moved to the reader (#4821), which has to run
    /// BEFORE the cap for the cap to keep the same plans.
    /// </summary>
    internal const int ParameterSensitivityMaxOffenders = 20;

    public const string ParameterSensitivitySql = @"
WITH newest AS
(
    -- creation_time is the MONITORED SERVER's local wall clock -- QueryStatsCollector ships the
    -- dm_exec_query_stats value verbatim -- while the window bound is naive UTC off DateTime.UtcNow.
    -- The compiled-before-the-window test has to compare a single frame, so the reader converts each plan's
    -- creation_time to UTC through the server's ServerClock (#4821): the zone where the newest snapshot
    -- reports one, its fixed offset otherwise. One offset for every plan put a plan compiled before the
    -- zone's last daylight saving change an hour off, and a wide min/max worker-time spread stops being
    -- evidence of parameter sensitivity when plan age is off. This CTE is that newest snapshot's zone id
    -- and offset from the SAME server_properties row, handed back on every row. The SQL keeps the newest
    -- offset only as a rough filter an hour wider than the window bound; the reader applies the exact test
    -- and the cap. svr below is exactly one row even when server_properties has not
    -- been collected yet (an on-load collector, so an absent offset is the state every server passes
    -- through on its first cycle -- refusing the read there would pre-empt the two answers that outrank
    -- any window): offset 0 and no zone, which is UTC.
    SELECT sp.utc_offset_minutes, sp.time_zone_id
    FROM server_properties AS sp
    WHERE sp.server_id = $1
    AND   sp.utc_offset_minutes IS NOT NULL
    ORDER BY sp.collection_time DESC
    LIMIT 1
),
svr AS
(
    SELECT
        COALESCE((SELECT utc_offset_minutes FROM newest), 0) AS offset_minutes,
        (SELECT time_zone_id FROM newest) AS time_zone_id
),
latest AS
(
    SELECT
        query_hash,
        query_plan_hash,
        database_name,
        execution_count,
        creation_time,
        min_worker_time,
        max_worker_time,
        min_grant_kb,
        max_grant_kb,
        min_spills,
        max_spills,
        ROW_NUMBER() OVER
        (
            PARTITION BY database_name, query_hash, query_plan_hash
            ORDER BY collection_time DESC
        ) AS rn
    FROM v_query_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   delta_execution_count > 0
)
SELECT
    min_worker_time,
    max_worker_time,
    max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) AS worker_ratio,
    max_grant_kb::DOUBLE PRECISION / NULLIF(min_grant_kb, 0) AS grant_ratio,
    CASE WHEN max_spills > 0 AND min_spills = 0 THEN 1 ELSE 0 END AS spill_divergence,
    creation_time AS creation_time_local,
    svr.offset_minutes,
    svr.time_zone_id,
    database_name,
    query_hash,
    query_plan_hash
FROM latest, svr
WHERE rn = 1
AND   min_worker_time >= 10000
AND   max_worker_time >= 250000
AND   execution_count >= 20
AND   creation_time - make_interval(mins => svr.offset_minutes) <= $2 + interval '1 hour'
AND   max_worker_time::DOUBLE PRECISION / NULLIF(min_worker_time, 0) >= 10
ORDER BY worker_ratio DESC, database_name, query_hash, query_plan_hash";

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
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(ParameterSensitivitySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

            var windowStart = AsNaive(context.TimeRangeStart);
            ServerClock? clock = null;
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

            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                /* #4821: the compiled-before-the-window test on each plan's own converted creation time. Every
                   row carries the same newest-snapshot zone and offset, so the clock is built once. */
                clock ??= ServerLocalTimes.ClockFrom(
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.IsDBNull(6) ? null : reader.GetInt32(6));
                if (!ServerLocalTimes.CreatedByWindowStart(clock, reader.IsDBNull(5) ? null : reader.GetDateTime(5), windowStart))
                    continue;

                // Rows arrive ordered by worker_ratio DESC — the first row kept is the worst offender.
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
                    /* Ordinals 8-10 are the last three columns of ParameterSensitivitySql's SELECT list (database_name, query_hash,
                       query_plan_hash), re-read against it for #6641. FactsNameQueriesLiveTests runs them where DARLING_TEST_PG is set. */
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
                if (offenderCount >= ParameterSensitivityMaxOffenders)
                    break;
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
                    ["grant_divergence"] = worstGrantRatio >= 5 ? 1 : 0,
                    ["spill_divergence"] = worstSpillDivergence
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

    /// <summary>The PLAN_REGRESSION comparison window, in days — how far back a query's "best known" plan
    /// may have been observed. One definition with the builder's (<see cref="PlanRegressionDaily.WindowDays"/>): a window
    /// that outgrew the days the builder fills would leave every read short of built days and on the full scan.</summary>
    internal const int PlanRegressionWindowDays = PlanRegressionDaily.WindowDays;

    /// <summary>
    /// How far BELOW the comparison window the chunk-exclusion bound sits (#2387). `last_execution_time`
    /// is the monitored server's clock and `collection_time` is the store's; a monitored server running
    /// ahead can report an execution time later than the collection that carried it. A day absorbs any
    /// plausible drift while still excluding all but ~15 days of a store that may hold months. One definition with the
    /// builder's (<see cref="PlanRegressionDaily.PlanRegressionSkewMarginDays"/>), which closes a day by the same margin.
    /// </summary>
    internal const int PlanRegressionSkewMarginDays = PlanRegressionDaily.PlanRegressionSkewMarginDays;

    /* PG port: any_value() below is standard SQL:2023, in Postgres since 16 — the product's
       minimum supported PG is 16, so it stays verbatim (DuckDB and PG agree on its semantics:
       an arbitrary non-null value from the group). */
    public const string PlanRegressionSql = PlanRegressionRawPrefix + PlanRegressionSuffix;

    /// <summary>
    /// The raw read's head (#3953 split it off, byte-identical): the interval dedup over the server's raw Query Store
    /// slice, then <c>plan_agg</c> over it. <see cref="PlanRegressionSuffix"/> is shared with
    /// <see cref="PlanRegressionTableSql"/>, so the two reads cannot drift above <c>plan_agg</c>.
    /// </summary>
    private const string PlanRegressionRawPrefix = @"
WITH deduped AS
(
    -- Collapse incremental re-collections of the same open runtime-stats interval:
    -- keep only the latest collection_time row per logical interval.
    --
    -- #1850: replica_role is part of the interval's identity, not a passenger.
    -- sys.query_store_runtime_stats is keyed by (plan_id, interval, execution_type, replica_group), and
    -- on a SQL Server 2022+ AG with Query Store for secondary replicas enabled the primary holds ONE
    -- shared Query Store carrying every replica's rows. Two rows differing only in replica_role are
    -- distinct legitimate work, so a grouping key without it does not de-duplicate — it DISCARDS one
    -- replica's row when the group collapses. That is an under-count, which is strictly worse than the
    -- double-count this CTE exists to fix: a double-count is visible in the number, a dropped row is
    -- silent. Same reasoning, same key as the read side (#1845).
    -- execution_type_desc is correctly absent: the WHERE pins it to 'Regular', so it is constant here.
    --
    -- #2827: GROUP BY + ordered array_agg, not ROW_NUMBER() ... WHERE rn = 1. Both keep the same row --
    -- first by (collection_time DESC, execution_count DESC) within the partition key -- and the rewrite
    -- returns a byte-identical result set (md5 over the full ordered output) on six servers, one of them
    -- empty. What differs is the PLAN each form admits: the window function forces one global sort of the
    -- server's whole Query Store slice on all eight keys, spilling ~548MB; the GROUP BY lets the planner
    -- hash-aggregate at the interval grain and sort only within each small group. Measured in isolation
    -- on the busiest server, 23.4s -> 9.6s; end to end it is faster on every server measured and, for
    -- something running against a deadline, far less variable (38.9-40.9s against 33.6-83.9s).
    -- DISTINCT ON needs the same global sort and measured WORSE (49.8s). Raising work_mem is not the
    -- answer either -- 512MB took it from 25.6s to 59.3s. Do not revert to any of those.
    --
    -- ASSUMPTION, written down because PostgreSQL does not contract it: the eight aggregates below each
    -- sort their own group, so taking [1] from each is only coherent if they all resolve a TIE -- two rows
    -- equal on BOTH collection_time and execution_count -- to the same physical row. PostgreSQL shares one
    -- sort across aggregates carrying an identical ORDER BY, so in practice they do; that is implementation
    -- behaviour, not a documented guarantee, and a drifted ORDER BY on any one of them would break it. The
    -- window form this replaces could not blend at all, because it picks one physical row.
    --
    -- Safe here on two counts. A tie is the #1907 flushed/in-memory pair sharing a collection_time, and the
    -- collector has COMBINED those slices before storing since #1907, so newly collected rows cannot tie.
    -- And measured on the busiest server's 14-day slice, grouping by (dedup key, collection_time,
    -- execution_count) yields ZERO groups holding more than one row. Removing the assumption outright by
    -- aggregating the composite row once was tried and is far worse -- 26.4s -> 91.2s, slower than the
    -- window form -- so this risk is accepted deliberately rather than missed.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        runtime_stats_interval_id,
        first_execution_time,
        (array_agg(query_plan_hash ORDER BY collection_time DESC, execution_count DESC))[1] AS query_plan_hash,
        (array_agg(execution_count ORDER BY collection_time DESC, execution_count DESC))[1] AS execution_count,
        (array_agg(avg_cpu_time_us ORDER BY collection_time DESC, execution_count DESC))[1] AS avg_cpu_time_us,
        (array_agg(avg_duration_us ORDER BY collection_time DESC, execution_count DESC))[1] AS avg_duration_us,
        -- #5631: rows per call, for the output check; the same pick as the aggregates around it.
        (array_agg(avg_rowcount ORDER BY collection_time DESC, execution_count DESC))[1] AS avg_rowcount,
        (array_agg(last_execution_time ORDER BY collection_time DESC, execution_count DESC))[1] AS last_execution_time,
        (array_agg(is_forced_plan ORDER BY collection_time DESC, execution_count DESC))[1] AS is_forced_plan,
        (array_agg(force_failure_count ORDER BY collection_time DESC, execution_count DESC))[1] AS force_failure_count
    FROM v_query_store_stats
    WHERE server_id = $1
    AND   execution_type_desc = 'Regular'
    AND   last_execution_time >= $2
    -- #2387: the SEMANTIC window is last_execution_time above; this is a REDUNDANT bound on the
    -- partitioning column so TimescaleDB can exclude chunks. Without it this reads the server's whole
    -- history every analysis cycle, decompressing whatever is compressed, per server -- cost scaling with
    -- STORE SIZE rather than with the configured window, which is why no VM size fixes it. Redundant to
    -- the ANSWER because a row cannot be collected before the execution it reports, so
    -- last_execution_time >= X already implies collection_time >= X. $3 carries a skew margin below X
    -- because last_execution_time comes from the MONITORED server's clock and collection_time from the
    -- store's: a monitored server running fast would otherwise have its newest rows excluded here, which
    -- would be a silent under-count and a worse bug than the one this fixes. Do not delete as dead
    -- weight -- it is doing all the pruning.
    AND   collection_time >= $3
    GROUP BY database_name, query_id, plan_id, replica_role, runtime_stats_interval_id, first_execution_time
),
plan_agg AS
(
    -- Execution-weighted per-exec cost per plan_id, per replica. Keeping replica_role in the grain all
    -- the way down is what makes the wider dedup key an improvement rather than a blend: were it dropped
    -- here, both replicas' rows would survive the dedup and then be summed into one number, and primary
    -- and secondary workload would be indistinguishable in the output.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        -- #5631: rows returned per call, execution-weighted over the same intervals as cpu_per_exec. NULL when any
        -- interval has no avg_rowcount, so a missing count can never read as a small one.
        CASE WHEN COUNT(*) = COUNT(avg_rowcount)
             THEN SUM(avg_rowcount::numeric * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0)
        END AS rows_per_exec,
        MAX(last_execution_time) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM deduped
    GROUP BY database_name, query_id, plan_id, replica_role
),
";

    /// <summary>
    /// PLAN_REGRESSION over the latest-snapshot interval table (#3953): <see cref="PlanRegressionSql"/> with its
    /// <c>deduped</c> CTE deleted and <c>plan_agg</c> reading <c>query_store_interval_latest</c>. Everything from
    /// <c>plan_dedup</c> down is the shared <see cref="PlanRegressionSuffix"/>. Over the same snapshots it returns the
    /// raw read's result exactly; where the table holds intervals raw has purged, it reaches the full 14-day window
    /// the fact always described. Chosen per server and pass by <see cref="QueryStoreIntervalLatest.ReadsTableAsync"/>.
    /// </summary>
    public const string PlanRegressionTableSql = PlanRegressionTablePrefix + PlanRegressionSuffix;

    private const string PlanRegressionTablePrefix = @"
WITH plan_agg AS
(
    -- #3953: the table twin. query_store_interval_latest already holds each interval's latest snapshot -- the
    -- raw twin's deduped CTE, maintained as raw is written -- so this aggregates it directly. $2/$3 are the raw
    -- read's own bounds, kept so both reads are the same function of the same snapshots: per interval the raw read
    -- keeps the snapshot maximal in (collection_time, execution_count) among those passing them, snapshots are
    -- cumulative, so if any passes the maximal one does, and the maximal one is the table's row. $4 bounds the
    -- table's partitioning column (first_execution_time) for chunk exclusion, bare for #2387's reason; it is
    -- implied by last_execution_time >= $2, because one Query Store interval spans at most a day.
    -- Execution-weighted per-exec cost per plan_id, per replica. Keeping replica_role in the grain all
    -- the way down is what makes the wider dedup key an improvement rather than a blend: were it dropped
    -- here, both replicas' rows would survive the dedup and then be summed into one number, and primary
    -- and secondary workload would be indistinguishable in the output.
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execution_count) AS execs,
        SUM(avg_cpu_time_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS cpu_per_exec,
        SUM(avg_duration_us * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0) AS dur_per_exec,
        -- #5631: rows returned per call, execution-weighted over the same intervals as cpu_per_exec. NULL when any
        -- interval has no avg_rowcount, so a missing count can never read as a small one.
        CASE WHEN COUNT(*) = COUNT(avg_rowcount)
             THEN SUM(avg_rowcount::numeric * execution_count)::DOUBLE PRECISION / NULLIF(SUM(execution_count), 0)
        END AS rows_per_exec,
        MAX(last_execution_time) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM query_store_interval_latest
    WHERE server_id = $1
    AND   last_execution_time >= $2
    AND   collection_time >= $3
    AND   first_execution_time >= $4
    GROUP BY database_name, query_id, plan_id, replica_role
),
";

    /// <summary>
    /// PLAN_REGRESSION over per-day totals for the closed days and the interval table for the rest (#5448):
    /// <see cref="PlanRegressionTableSql"/> with its <c>plan_agg</c> replaced by the sum of two reads, so everything from
    /// <c>plan_dedup</c> down is the shared <see cref="PlanRegressionSuffix"/> and cannot drift. $1 server_id, $2 M, $3 the
    /// built days (<see cref="PlanRegressionDaily.BuiltDaysSql"/>, date[]), $4 the live floor and $5 M - 1 day, both
    /// timestamps (see <see cref="PlanRegressionLiveFloor"/>), $6 each built day's watermark (timestamp[], aligned with $3; the
    /// next midnight for a whole day, #5812).
    ///
    /// <para><b>The window edge moves in whole days.</b> This read's edge is M, the start of the day the exact edge falls
    /// in, for both halves; the exact-bound read it replaces cut mid-day. It is the same function of the same snapshots
    /// whenever the edge is day-aligned (the parity tests pin that), and differs by less than a day otherwise, which is
    /// why the drill-down reads the same M (<c>AnalysisContext.PlanRegressionWindowStart</c>). A built day was built from
    /// <c>last_execution_time</c> in the day, <c>first_execution_time</c> and <c>collection_time</c> a day below it, so the
    /// two routes differ only for a snapshot collected more than a day before its interval's last execution.</para>
    ///
    /// <para><b>One snapshot for both.</b> The built days and this read are two statements, and the trigger can mark a
    /// day late, and the builder can rebuild one, between them. The fact therefore runs both in one REPEATABLE READ READ
    /// ONLY transaction: they see the same built set and the same totals, so a day is read from the totals only if the
    /// totals the read sees are the ones that made it valid. The live floor stays a bound parameter ($4, worked out from
    /// the built days the first statement returned), so the planner knows it and a hypertable drops the old chunks while
    /// it plans, as the numbers in #5448 were measured. A late row that lands after the snapshot is not in it, and the
    /// next builder pass rebuilds the day.</para>
    /// </summary>
    internal const string PlanRegressionDailySql = PlanRegressionDailyPrefix + PlanRegressionSuffix;

    private const string PlanRegressionDailyPrefix = @"
WITH plan_agg AS
(
    -- #5448: built days from collect.plan_regression_daily (summed totals, so days recombine exactly), every other row
    -- from the interval table with the daily builder's own aggregate. A row is in exactly one half by its day
    -- (last_execution_time::date): a day not built is all live, and a built day keeps only its rows at or above its
    -- watermark ($6) live (#5812: an open day is built partially; a whole day's watermark is its next midnight).
    SELECT
        database_name,
        query_id,
        plan_id,
        replica_role,
        any_value(query_plan_hash) AS query_plan_hash,
        SUM(execs) AS execs,
        SUM(cpu_us_sum)::DOUBLE PRECISION / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_us_sum)::DOUBLE PRECISION / NULLIF(SUM(execs), 0) AS dur_per_exec,
        -- #5631: rows per call. A day with no rowcount_sum (an interval had none, or the day awaits its rebuild after the
        -- column was added) makes the whole plan's figure NULL, the same rule the other two reads apply.
        CASE WHEN COUNT(*) = COUNT(rowcount_sum)
             THEN SUM(rowcount_sum)::DOUBLE PRECISION / NULLIF(SUM(execs), 0)
        END AS rows_per_exec,
        MAX(last_exec) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count
    FROM
    (
        SELECT
            d.database_name, d.query_id, d.plan_id, d.replica_role, d.query_plan_hash,
            d.execs, d.cpu_us_sum, d.dur_us_sum, d.rowcount_sum, d.last_exec, d.is_forced_plan, d.force_failure_count
        FROM plan_regression_daily AS d
        WHERE d.server_id = $1::integer
        AND   d.day = ANY($3::date[])

        UNION ALL

        SELECT
            l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash,
            SUM(l.execution_count),
            SUM(l.avg_cpu_time_us::numeric * l.execution_count),
            SUM(l.avg_duration_us::numeric * l.execution_count),
            CASE WHEN COUNT(*) = COUNT(l.avg_rowcount) THEN COALESCE(SUM(l.avg_rowcount::numeric * l.execution_count), 0) END, -- #5631: COALESCE as in BuildDaySql, so a group whose execution counts are all NULL is 0 rows, not NULL.
            MAX(l.last_execution_time),
            bool_or(l.is_forced_plan),
            MAX(l.force_failure_count)
        FROM query_store_interval_latest AS l
        WHERE l.server_id = $1::integer
        AND   l.last_execution_time >= $2::timestamp
        AND   l.collection_time >= $5::timestamp
        AND   l.first_execution_time >= $4::timestamp
        -- #5812: a row is in the totals when its day is built and its first execution is below that day's watermark
        -- (an open day is built partially; a whole day's watermark is its next midnight), so the live half is the rest.
        -- A scalar lookup ($6 is aligned with $3; a day not built finds no watermark and keeps every row), so the test
        -- stays a filter on the scan, as #5448's <> ALL($3) was, and adds no join whose strategy rests on an array estimate.
        AND   l.first_execution_time >= COALESCE(($6::timestamp[])[array_position($3::date[], l.last_execution_time::date)], '-infinity'::timestamp)
        GROUP BY l.database_name, l.query_id, l.plan_id, l.replica_role, l.query_plan_hash
    ) AS u
    GROUP BY database_name, query_id, plan_id, replica_role
),
";

    /// <summary>
    /// The work memory for every PLAN_REGRESSION read (#5812, bound 2): the daily read's snapshot transaction and the short
    /// transaction each exact-bound read (table or raw) now runs in (M2). Set with <c>SET LOCAL</c> so it ends with
    /// that transaction and touches no other statement on the pooled connection. The daily read groups every built day's
    /// totals plus the live rows by plan. At the default 4MB the planner sorts them all, and on a planted busy server (15
    /// built days, about 384,000 rows) that sort went to disk (an external merge of about 15MB). From 32MB up it hashes in
    /// memory instead: two hash aggregates of about 35MB to 42MB each, under the hash limit of twice work_mem
    /// (<c>hash_mem_multiplier</c>). 64MB is the bound: 32MB also did not spill, but it is below its own peak (about 35MB)
    /// and fits only through that multiplier, so 64MB keeps the headroom. A hash or sort takes memory only as it grows, so
    /// a small server's read uses what it needs (<c>PlanRegressionDailyNoSpillRig</c> measures every bound on one plant).
    /// The figure is per node, not per read: the daily read has six sort or hash nodes, so one read may take up to 6 x 64MB,
    /// doubled for a hash through <c>hash_mem_multiplier</c> (768MB at its default of 2), though the plant peaked near 37MB
    /// in each of two hash aggregates. Across the fleet, scheduled reads run inside each server's sweep, so the ceiling is that
    /// times <c>MaxConcurrentSweeps</c> (4 by default, at most 16), and on-demand <c>analyze_server</c> and
    /// <c>get_analysis_facts</c> reads that miss the candidate cache run outside the sweep gate and add to it; size
    /// <c>MaxConcurrentSweeps</c> against the store's memory before raising this bound.
    /// </summary>
    internal const string PlanRegressionWorkMemSql = "SET LOCAL work_mem = '64MB'";

    /// <summary>
    /// M: the start of the day the comparison window's exact edge (<paramref name="rawWindowStart"/>) falls in (#5448).
    /// </summary>
    internal static DateTime PlanRegressionWindowFloor(DateTime rawWindowStart) =>
        DateTime.SpecifyKind(rawWindowStart.Date, DateTimeKind.Unspecified);

    /// <summary>
    /// The live read's <c>first_execution_time</c> floor (#5448): one day below the first day, from
    /// <paramref name="windowFloor"/> on, that is NOT built. A live row's day is that day or later, and one Query Store
    /// interval spans at most a day, so its first execution is at or above the floor. With every day from the window's
    /// start built up to some last one, the floor sits one day below the day after it, so a read still sees whatever
    /// comes after the built days (a skewed server's future-dated rows).
    ///
    /// <para>This is the whole-day case of <see cref="PlanRegressionLiveFloorWithWatermarks"/> (#5812): every day in
    /// <paramref name="builtDays"/> is taken as built whole, with no watermark. The read itself calls the watermark form,
    /// because a built day can be partial; only tests call this one.</para>
    /// </summary>
    internal static DateTime PlanRegressionLiveFloor(DateTime windowFloor, IReadOnlyCollection<DateOnly> builtDays) =>
        PlanRegressionLiveFloorWithWatermarks(windowFloor, builtDays.Select(d => new PlanRegressionDaily.BuiltDay(d, null)).ToList());

    /// <summary>
    /// The live floor with partial days (#5812): the lower of one day below the first day from <paramref name="windowFloor"/>
    /// on that is not built, and the lowest watermark of a built day before it that is partial. The builder builds through
    /// tomorrow, so on a store that is current the first unbuilt day is the day after tomorrow and the floor is today's
    /// watermark: the live half reads the last two or three hours of first executions, not the open days.
    /// </summary>
    internal static DateTime PlanRegressionLiveFloorWithWatermarks(DateTime windowFloor, IReadOnlyCollection<PlanRegressionDaily.BuiltDay> builtDays)
    {
        var through = new Dictionary<DateOnly, DateTime>();
        foreach (var b in builtDays)
        {
            through[b.Day] = b.ThroughOrDayEnd;
        }

        var cursor = DateOnly.FromDateTime(windowFloor);
        var floor = DateTime.MaxValue;
        while (through.TryGetValue(cursor, out var t))
        {
            if (t < cursor.AddDays(1).ToDateTime(TimeOnly.MinValue) && t < floor)
            {
                floor = t;
            }

            cursor = cursor.AddDays(1);
        }

        var unbuilt = cursor.ToDateTime(TimeOnly.MinValue).AddDays(-1);
        return DateTime.SpecifyKind(unbuilt < floor ? unbuilt : floor, DateTimeKind.Unspecified);
    }

    /// <summary>Test seam (#5448): runs between the PLAN_REGRESSION built-days read and the daily read, so a test can change the
    /// store from another connection at exactly the point a snapshot has to hold. Null in production.</summary>
    internal static readonly System.Threading.AsyncLocal<Func<System.Threading.CancellationToken, System.Threading.Tasks.Task>?> TestOnlyAfterBuiltDaysRead = new();

    /// <summary>Everything from <c>plan_dedup</c> down, shared by both PLAN_REGRESSION reads.</summary>
    private const string PlanRegressionSuffix = @"plan_dedup AS
(
    -- Collapse plan_ids that share a query_plan_hash (a recompile can produce an
    -- identical plan under a new plan_id); keep only plans with enough executions.
    SELECT
        database_name,
        query_id,
        replica_role,
        query_plan_hash,
        SUM(execs) AS execs,
        SUM(cpu_per_exec * execs) / NULLIF(SUM(execs), 0) AS cpu_per_exec,
        SUM(dur_per_exec * execs) / NULLIF(SUM(execs), 0) AS dur_per_exec,
        -- #5631: NULL when any plan_id of the hash group has no rows figure.
        CASE WHEN COUNT(*) = COUNT(rows_per_exec)
             THEN SUM(rows_per_exec * execs) / NULLIF(SUM(execs), 0)
        END AS rows_per_exec,
        MAX(last_exec) AS last_exec,
        bool_or(is_forced_plan) AS is_forced_plan,
        MAX(force_failure_count) AS force_failure_count,
        -- #5630: one plan_id per hash group, so the caller can read the two plans' compiled parameter values. The
        -- group's newest-run plan stands for the latest group, its most-executed plan for the best group; ties
        -- break on plan_id so the pick is the same on every read.
        (array_agg(plan_id ORDER BY last_exec DESC, plan_id DESC))[1] AS newest_plan_id,
        (array_agg(plan_id ORDER BY execs DESC, plan_id DESC))[1] AS busiest_plan_id
    FROM plan_agg
    /*SEC*/
    GROUP BY database_name, query_id, replica_role, query_plan_hash
    HAVING SUM(execs) >= 25
),
ranked AS
(
    -- Rank WITHIN a replica: a regression means this replica's current plan is worse than the best plan
    -- this replica has run, never a cross-replica comparison of two different workloads.
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY last_exec DESC) AS recency,
        ROW_NUMBER() OVER (PARTITION BY database_name, query_id, replica_role ORDER BY cpu_per_exec ASC) AS cheapness
    FROM plan_dedup
),
compared AS
(
    -- Latest active plan vs the best-performing plan for the same query on the same replica.
    -- IS NOT DISTINCT FROM, never =: replica_role is NULL on every standalone server, every non-AG
    -- server and everything below SQL Server 2022, and NULL = NULL is UNKNOWN — an equi-join here would
    -- match nothing and silently disable plan-regression detection for the overwhelming majority of
    -- installs. The NULL-safe operator groups those rows the same way the PARTITION BYs above do.
    SELECT
        l.query_id,
        l.cpu_per_exec AS latest_cpu,
        l.dur_per_exec AS latest_dur,
        l.is_forced_plan AS latest_is_forced,
        l.force_failure_count AS force_failure_count,
        b.cpu_per_exec AS best_cpu,
        b.dur_per_exec AS best_dur,
        -- #5631: rows per call of the two plans, for the output check.
        l.rows_per_exec AS latest_rows,
        b.rows_per_exec AS best_rows,
        -- #2138: CPU is the PRIMARY signal — duration alone is confounded by blocking, IO waits, and
        -- machine contention that no plan choice caused, so it must not fire a plan-regression verdict
        -- by itself. A CPU regression scores at its own ratio; a duration-dominant one fires only when
        -- EXTREME (>= 4x) AND corroborated by at least mild CPU worsening (>= 1.25x), scored at half
        -- the duration ratio so it competes honestly with CPU-detected rows. NULL when neither path
        -- fires — the >= 2 gate below drops it.
        CASE
            WHEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 2
                THEN l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0)
            WHEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) >= 4
             AND l.cpu_per_exec / NULLIF(b.cpu_per_exec, 0) >= 1.25
                THEN l.dur_per_exec / NULLIF(b.dur_per_exec, 0) / 2
        END AS regression_factor,
        -- The resource-expenditure half of the importance gate (#2138): total CPU the LATEST plan burned
        -- over the window. The exec-count floor above only counts; this weighs.
        l.execs * l.cpu_per_exec AS latest_total_cpu_us,
        l.database_name,
        -- #3953: when the best plan last ran, so the advice can state its age. The window reaches a full 14 days
        -- on both SKUs, so a best plan can be two weeks old.
        b.last_exec AS best_last_exec,
        -- #5630: the plans to compare, and the key for the statement text.
        l.newest_plan_id AS latest_plan_id,
        b.busiest_plan_id AS best_plan_id
    FROM ranked AS l
    JOIN ranked AS b
      ON  b.database_name = l.database_name
      AND b.query_id = l.query_id
      AND b.replica_role IS NOT DISTINCT FROM l.replica_role
      AND b.cheapness = 1
    WHERE l.recency = 1
    AND   l.query_plan_hash <> b.query_plan_hash
),
candidates AS
(
    SELECT *
    FROM compared
    WHERE regression_factor >= 2
    -- 10 CPU-seconds across the window: a NOISE floor, not an importance ranking — it exists to exclude
    -- near-zero-cost queries whose ratios are all sampling jitter; magnitude ranking stays with
    -- regression_factor and the scorer.
    AND   latest_total_cpu_us >= 10000000
    -- #6548: SQL Server own housekeeping statements (change tracking cleanup and the like) are not application code and
    -- cannot be fixed by anyone monitoring. Left out BEFORE the LIMIT below, so they cannot use up its rows or lead the fact.
    AND   NOT EXISTS (SELECT 1 FROM query_store_text AS x
                      WHERE x.server_id = $1
                        AND x.database_name = compared.database_name
                        AND x.query_id = compared.query_id
                        AND (lower(x.query_sql_text) ~ '" + PerformanceMonitor.Common.InternalStatements.LowerTextPattern + @"'
                          -- #6665: or the monitor's own statement (its collector-side drop never fires on Darling)
                          OR " + PerformanceMonitor.Darling.Storage.QueryStoreSelfStatement.SelfOfStoredText + @"))
    -- #5630: 100, not 20. The caller drops the queries whose two plans were compiled for different inputs
    -- (PlanInputComparison) and keeps the worst 20 of what is left, so the cut moved after that check.
    ORDER BY regression_factor DESC
    LIMIT 100
)
SELECT
    c.query_id,
    c.latest_cpu,
    c.latest_dur,
    c.latest_is_forced,
    c.force_failure_count,
    c.best_cpu,
    c.best_dur,
    c.regression_factor,
    -- #3902: appended, so the ordinals above are untouched. With query_id it names each offender for the
    -- regressed-queries drill-down (AnalysisContext.PlanRegressionOffenders).
    c.database_name,
    -- #3953: appended for the same reason.
    c.best_last_exec,
    -- #5630: appended for the same reason. The two plans to compare, then the statement text (the table is keyed
    -- on (server, database, query_id), so it cannot fan a row out).
    c.latest_plan_id,
    c.best_plan_id,
    x.query_sql_text,
    -- #5631: appended for the same reason. Rows per call of the newer and the cheaper plan; NULL when unknown.
    c.latest_rows,
    c.best_rows
FROM candidates AS c
LEFT JOIN query_store_text AS x
  ON  x.server_id = $1
  AND x.database_name = c.database_name
  AND x.query_id = c.query_id
ORDER BY c.regression_factor DESC";

    /// <summary>
    /// Detects plan regressions: a query whose currently-active plan has per-execution
    /// cost >= 2x the best plan that query is known to perform well with. Emits one
    /// aggregate PLAN_REGRESSION fact. Sourced from Query Store (v_query_store_stats);
    /// no fact when Query Store is not enabled on the monitored databases.
    /// Windows on last_execution_time (the 14-day comparison window) because a plan regression is about
    /// when the query last RAN, not when we happened to collect it. It ALSO carries a redundant
    /// collection_time bound so TimescaleDB can exclude chunks — see the note in the SQL (#2387).
    /// </summary>
    private async Task CollectPlanRegressionFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        /* #3902: cleared first, so a read that fails below leaves "not known" for the drill-down rather
           than a list some earlier pass stamped on a reused context. */
        context.PlanRegressionOffenders = null;
        context.PlanRegressionFailures = null;
        context.PlanRegressionReadsIntervalTable = null;
        context.PlanRegressionWindowStart = null;

        /* #5824: how long this read took, stamped in the finally whatever the outcome, so the scheduled pass's analysis_pass
           row in collection_log can say what the store's most expensive analysis read cost on this pass. */
        context.PlanRegressionReadMs = null;
        var readStarted = System.Diagnostics.Stopwatch.GetTimestamp();

        /* #5448: the built-days read and the daily read share this transaction (see PlanRegressionDailySql). */
        NpgsqlTransaction? snapshot = null;
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            /* #5812 (bound 2, M2): every PLAN_REGRESSION read runs read-only with PlanRegressionWorkMemSql, set here only,
               on whichever transaction holds it (the daily read's snapshot or the exact-bound read's own). */
            async Task BoundTheReadAsync()
            {
                await using (var readOnly = new NpgsqlCommand("SET TRANSACTION READ ONLY", connection, snapshot) { CommandTimeout = FactCommandTimeoutSeconds })
                {
                    await readOnly.ExecuteNonQueryAsync(context.CancellationToken);
                }

                await using (var workMem = new NpgsqlCommand(PlanRegressionWorkMemSql, connection, snapshot) { CommandTimeout = FactCommandTimeoutSeconds })
                {
                    await workMem.ExecuteNonQueryAsync(context.CancellationToken);
                }
            }

            /* #2387: the chunk-exclusion bound, one CLOCK-SKEW MARGIN below the comparison window. Bound as
               its own parameter rather than written as "$2 - INTERVAL '1 day'" so the planner compares
               against a bare parameter, which is the form runtime chunk exclusion handles most reliably. */
            var collectionBound = AsNaive(
                context.TimeRangeStart.AddDays(-(PlanRegressionWindowDays + PlanRegressionSkewMarginDays)));

            /* #3953: the latest-snapshot interval table where its coverage holds everything this read would read,
               raw otherwise. Decided once here and recorded, so the drill-down reads the same source. */
            var readsTable = await QueryStoreIntervalLatest.ReadsTableAsync(
                connection, context.ServerId, collectionBound, FactCommandTimeoutSeconds, _logger, context.CancellationToken);
            context.PlanRegressionReadsIntervalTable = readsTable;

            /* #5448: the closed days that are fully built, only when the table is the source. Any failure is an empty
               set: the read below then runs PlanRegressionTableSql on its exact bounds, as it did before the daily
               totals existed. That failure is logged in the helper and is NOT a collection caveat: the exact read is
               whole, so nothing is missing, and if it fails too its own catch records it, once. A cancelled pass is
               not swallowed. */
            var rawWindowStart = AsNaive(context.TimeRangeStart.AddDays(-PlanRegressionWindowDays));
            var windowFloor = PlanRegressionWindowFloor(rawWindowStart);
            var builtDays = new List<PlanRegressionDaily.BuiltDay>();
            if (readsTable)
            {
                /* One REPEATABLE READ READ ONLY transaction for the built-days read and the daily read: both see the same
                   built set and the same totals, and the live floor below stays a bound parameter. */
                snapshot = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, context.CancellationToken);
                await BoundTheReadAsync();

                builtDays = await QueryStoreIntervalLatest.ReadBuiltDaysAsync(
                    connection, snapshot, context.ServerId, windowFloor, FactCommandTimeoutSeconds, _logger,
                    ex => AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken), context.CancellationToken);
                if (builtDays.Count == 0)
                {
                    /* No built day, or the read failed (which leaves this transaction aborted): the exact-bound read runs
                       outside it, in the short transaction below. */
                    await snapshot.DisposeAsync();
                    snapshot = null;
                }
                else if (TestOnlyAfterBuiltDaysRead.Value is { } afterBuiltDays)
                {
                    await afterBuiltDays(context.CancellationToken);
                }
            }

            var readsDays = builtDays.Count > 0;
            if (snapshot is null)
            {
                /* #5812 (M2): the exact-bound reads (the table read while no day is built or after a failed built-days read,
                   and the raw read of a server without coverage) take the same work memory as the daily read, in a short
                   transaction of their own. They group the whole 14-day window by plan, and at the default work_mem that
                   grouping went to disk. SET LOCAL ends with this transaction, which is disposed right after the read, so
                   the plan reads below and the next user of the pooled connection keep the server's own value. */
                snapshot = await connection.BeginTransactionAsync(context.CancellationToken);
                await BoundTheReadAsync();
            }

            using var cmd = new NpgsqlCommand(
                readsDays ? PlanRegressionDailySql : readsTable ? PlanRegressionTableSql : PlanRegressionSql, connection, snapshot)
            { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            if (readsDays)
            {
                /* $2 M, $3 the built days, $4 the live floor, $5 M - 1 day (the collection bound, bare for #2387). */
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, windowFloor);
                cmd.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Date,
                    Value = builtDays.Select(b => b.Day).ToArray()
                });
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, PlanRegressionLiveFloorWithWatermarks(windowFloor, builtDays));
                cmd.Parameters.AddWithValue(NpgsqlTypes.NpgsqlDbType.Timestamp, windowFloor.AddDays(-1));
                cmd.Parameters.Add(new NpgsqlParameter
                {
                    NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Timestamp,
                    Value = builtDays.Select(b => b.ThroughOrDayEnd).ToArray()
                });
            }
            else
            {
                cmd.Parameters.AddWithValue(rawWindowStart);
                cmd.Parameters.AddWithValue(collectionBound);
                if (readsTable)
                {
                    /* $4, the table's partitioning column: the same value, bare, for the same reason. */
                    cmd.Parameters.AddWithValue(collectionBound);
                }
            }

            /* #5558: Query Store on a secondary copy is the primary's content, and rows from the days this node was
               primary stay in the window, so the read skips those databases. The marker sits in the suffix all three
               variants share, so they cannot drift; bound after every other parameter. */
            PgSecondaryReplicaScope.Apply(cmd, context, "PLAN_REGRESSION", "database_name", "WHERE");

            /* #5630: read the candidates first (the worst 100 by factor), then judge them, because judging reads plans
               and a second command cannot run while this reader is open. */
            /* #5812: a current MCP or viewer call reuses the candidates of a read of the same shape younger than one analysis
               cadence (PlanRegressionCandidateCache.RefreshAfter). The scheduled pass always reads and refills the entry, so it
               stays as fresh as before. Everything below the read (the input check, the cut, the fact) still runs every time. */
            var readStartedUtc = DateTime.UtcNow;
            /* The daily read's leading five parameters stand for the window and the built days; $6, the watermarks, goes in
               last so a moved watermark is reported as one. An exact-bound read's shape carries its exact window start
               (rawWindowStart), so a call with a different window length never takes another pass's read (L3). */
            var shape = context.ReusePlanRegressionCandidates || context.RefreshPlanRegressionCandidates
                ? PlanRegressionCandidateCache.ShapeOf(
                    cmd, readsDays ? 5 : readsTable ? 4 : 3, windowFloor, builtDays.Select(b => b.Day).ToList(),
                    exactWindowStart: readsDays ? null : rawWindowStart, watermarkParameter: readsDays ? 5 : null)
                : null;
            var decision = PlanRegressionCandidateCache.ReuseDecision.NoEntry;
            var reused = shape is null || context.RefreshPlanRegressionCandidates
                ? null
                : PlanRegressionCache.TryGet(StoreIdentity, context.ServerId, shape, context.TimeRangeEnd, readStartedUtc, out decision);
            if (shape is not null && context.RefreshPlanRegressionCandidates)
            {
                _logger?.LogDebug("PLAN_REGRESSION candidates for server {ServerId}: the scheduled pass reads live and refills the cache", context.ServerId);
            }
            else if (shape is not null)
            {
                _logger?.LogDebug("PLAN_REGRESSION candidates for server {ServerId}: {Decision} ({Outcome})",
                    context.ServerId, decision, reused is null ? "read live" : "reused");
            }

            var candidates = reused is null ? new List<PlanRegressionCandidate>() : new List<PlanRegressionCandidate>(reused);
            if (reused is null)
            {
                using (var reader = await cmd.ExecuteReaderAsync(context.CancellationToken))
                {
                    while (await reader.ReadAsync(context.CancellationToken))
                    {
                        candidates.Add(new PlanRegressionCandidate(
                            DatabaseName: reader.GetString(8),
                            QueryId: ToInt64(reader.GetValue(0)),
                            LatestCpu: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                            LatestIsForced: !reader.IsDBNull(3) && Convert.ToBoolean(reader.GetValue(3)),
                            ForceFailures: reader.IsDBNull(4) ? 0L : ToInt64(reader.GetValue(4)),
                            BestCpu: reader.IsDBNull(5) ? 0.0 : Convert.ToDouble(reader.GetValue(5)),
                            Factor: reader.IsDBNull(7) ? 0.0 : Convert.ToDouble(reader.GetValue(7)),
                            BestLastExec: reader.IsDBNull(9) ? null : Convert.ToDateTime(reader.GetValue(9)),
                            LatestPlanId: reader.IsDBNull(10) ? null : ToInt64(reader.GetValue(10)),
                            BestPlanId: reader.IsDBNull(11) ? null : ToInt64(reader.GetValue(11)),
                            StatementText: reader.IsDBNull(12) ? null : reader.GetString(12),
                            LatestRows: reader.IsDBNull(13) ? null : Convert.ToDouble(reader.GetValue(13)),
                            BestRows: reader.IsDBNull(14) ? null : Convert.ToDouble(reader.GetValue(14))));
                    }
                }

                if (shape is not null && PlanRegressionCandidateCache.IsCurrent(context.TimeRangeEnd, readStartedUtc))
                {
                    PlanRegressionCache.Store(StoreIdentity, context.ServerId, shape, readStartedUtc, candidates);
                }
            }

            /* The snapshot has done its job: the plan reads below are ordinary statements, so one that fails cannot abort it. */
            if (snapshot is not null)
            {
                await snapshot.DisposeAsync();
                snapshot = null;
            }

            /* #6267: the queries whose timeouts or errors rose. The cost read above counts Regular runs only, so a plan that
               regresses into timeouts or errors is found here, from the wide table's non-Regular rows (the partial index's
               predicate). A failed read costs the failure half only: the cost half below still reports, and the failure is
               recorded as a collection caveat. */
            var failures = new List<PlanRegressionFailureRule.Failure>();
            try
            {
                failures = await ReadPlanRegressionFailuresAsync(connection, context, rawWindowStart, collectionBound);
            }
            catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
            {
                ReportCollectionFailure(ex, context);
            }

            /* #5630: drop the queries whose two plans were not compiled for the same inputs, keep the worst 20 of the
               rest. Everything that reads the list downstream (the offenders, the regressed-queries drill-down, the
               force-plan targets built from it) sees only the kept queries. The drill-down names a query by
               (database, query_id) and not by replica, so a query one replica's row excludes is out for every replica
               (an excluded query must not reach a force): a query is judged whole, every replica row of it, the first
               time its key comes up, so the exclusion is known before any row of it is kept and the cut to 20 counts
               only rows that stay. */
            var kept = new List<PlanRegressionCandidate>();
            var excludedKeys = new HashSet<PlanRegressionOffender>();
            var judgedKeys = new HashSet<PlanRegressionOffender>();
            var judgements = new Dictionary<int, PlanInputJudgement>();
            var planReads = new PlanReadFailures();
            /* Two replica rows of one query can be two candidates over the same two plans: one read and one comparison, as Lite's. */
            var planOutcomes = new Dictionary<(string Database, long Latest, long Best), PlanInputOutcome>();
            for (var i = 0; i < candidates.Count; i++)
            {
                if (kept.Count >= PlanRegressionMaxOffenders)
                    break;

                var candidate = candidates[i];
                var queryKey = new PlanRegressionOffender(candidate.DatabaseName, candidate.QueryId);
                if (judgedKeys.Add(queryKey))
                {
                    for (var j = i; j < candidates.Count; j++)
                    {
                        if (candidates[j].DatabaseName != queryKey.DatabaseName || candidates[j].QueryId != queryKey.QueryId)
                            continue;

                        var verdict = await JudgePlanRegressionInputsAsync(connection, context, candidates[j], planReads, planOutcomes);
                        if (verdict.Exclude)
                        {
                            excludedKeys.Add(queryKey);
                            break;
                        }

                        judgements[j] = verdict;
                    }
                }

                if (excludedKeys.Contains(queryKey))
                    continue;

                kept.Add(candidate with { Unverified = judgements[i].Unverified, InputBasis = judgements[i].Basis });
            }

            var offenderCount = kept.Count;
            var unverified = kept.Count(k => k.Unverified);
            var worstFactor = 0.0;
            var worstQueryId = 0L;
            var worstLatestCpu = 0.0;
            var worstBestCpu = 0.0;
            var worstDimension = 1;
            var worstLatestForced = 0;
            var worstForceFailures = 0L;
            var worstInputBasis = 0;
            DateTime? worstBestLastExec = null;
            var offenders = new List<PlanRegressionOffender>();

            for (var i = 0; i < kept.Count; i++)
            {
                var row = kept[i];
                // Rows arrive ordered by regression_factor DESC — the first row is the worst offender.
                if (i == 0)
                {
                    worstQueryId = row.QueryId;
                    worstLatestForced = row.LatestIsForced ? 1 : 0;
                    worstForceFailures = row.ForceFailures;
                    worstFactor = row.Factor;
                    worstLatestCpu = row.LatestCpu;
                    worstBestCpu = row.BestCpu;
                    worstBestLastExec = row.BestLastExec;
                    worstInputBasis = row.InputBasis;
                    // Which CASE branch fired, not which raw ratio is larger (review catch on #2138):
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

            /* #5448: M is recorded only now, after the read has succeeded. A read that threw leaves "not known" (null), as the
               AnalysisContext doc says, so the drill-down does not start at an edge no fact read from. */
            context.PlanRegressionWindowStart = readsDays ? windowFloor : null;

            /* #5630: plan reads that failed are one event for the pass, not one per candidate. The fact already carries
               them (the kept candidates are counted unverified), so the family is recorded as not read only when there is
               no fact to carry them. Only the exception's type is logged: a plan holds customer values. */
            if (planReads.FirstFailure is Exception planReadFailure)
            {
                if (offenderCount == 0)
                {
                    context.RecordCollectionFailure(
                        CollectionFailure.FamilyOf(nameof(CollectPlanRegressionFactsAsync)), nameof(CollectPlanRegressionFactsAsync),
                        ClassifyOutcome(planReadFailure), planReadFailure);
                }

                _logger?.LogWarning(
                    "[PgFactCollector] The PLAN_REGRESSION input check could not read the plans of {FailedReads} candidate(s) on server {ServerId} ({ExceptionType}); they are kept unverified",
                    planReads.Count, context.ServerId, planReadFailure.GetType().Name);
            }

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
                    /* #3953: 1 when this pass read the latest-snapshot interval table, 0 for the raw slice. */
                    ["plan_regression_source"] = readsTable ? 1 : 0,
                    /* #5630: queries left out as a change of input (the statement recompiles on every call, the newer
                       plan's cost followed a 10x bigger output, or the two plans were compiled for different values over
                       unchanged statistics, #5631), and kept queries that could not be checked (a plan or the
                       statement text is missing, or was withheld by the statement filter). */
                    ["cross_input_excluded_count"] = excludedKeys.Count,
                    ["inputs_unverified_count"] = unverified,
                    /* #5631: how the worst kept offender's two plans were compared: 1 compiled for the same parameter
                       values, 2 compiled for different values after a statistics update, 0 not checked. */
                    ["worst_input_basis"] = worstInputBasis,
                }
            };

            /* The cost figures exist only when a plan's cost regressed: a fact raised by failures alone carries none, so the
               advice reads their absence as "no cost regression" and not as a zero factor (#6267). */
            if (offenderCount > 0)
            {
                fact.Metadata["worst_regression_factor"] = worstFactor;
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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
        finally
        {
            /* After the reader and the connection's other locals are gone; a rollback, nothing here wrote. */
            if (snapshot is not null)
            {
                await snapshot.DisposeAsync();
            }

            context.PlanRegressionReadMs = (long)System.Diagnostics.Stopwatch.GetElapsedTime(readStarted).TotalMilliseconds;
        }
    }

    /// <summary>How many offenders the PLAN_REGRESSION fact reports: the worst of what the input check leaves (#5630).</summary>
    internal const int PlanRegressionMaxOffenders = 20;

    /// <summary>One row of the PLAN_REGRESSION read, with the keys to the two plans and the statement text (#5630).</summary>
    internal sealed record PlanRegressionCandidate(
        string DatabaseName,
        long QueryId,
        double LatestCpu,
        bool LatestIsForced,
        long ForceFailures,
        double BestCpu,
        double Factor,
        DateTime? BestLastExec,
        long? LatestPlanId,
        long? BestPlanId,
        string? StatementText,
        double? LatestRows,
        double? BestRows,
        bool Unverified = false,
        int InputBasis = 0);

    /// <summary>The plan reads of one PLAN_REGRESSION pass that failed: how many, and the first exception (#5630).</summary>
    private sealed class PlanReadFailures
    {
        public int Count { get; private set; }

        public Exception? FirstFailure { get; private set; }

        public void Add(Exception ex)
        {
            Count++;
            FirstFailure ??= ex;
        }
    }

    /// <summary>What the input check decided about one candidate: leave it out, or keep it (checked or not).</summary>
    private readonly record struct PlanInputJudgement(PlanInputOutcome Outcome, bool Unverified)
    {
        public bool Exclude => PlanInputComparison.IsDrop(Outcome);

        public int Basis => PlanInputComparison.InputBasis(Outcome);
    }

    /// <summary>
    /// The stored plan XML for two plan_ids of one database (#5630): the plan map reaches the plan dimension, which holds
    /// text or gzip bytes. A plan the map or the dimension does not hold is absent from the result.
    /// $1 server_id, $2 database_name, $3 plan_ids.
    /// </summary>
    internal const string PlanRegressionPlansSql = @"
SELECT m.plan_id, d.query_plan_xml, d.query_plan_gz
FROM collect.query_store_plan_map AS m
LEFT JOIN query_plan_dim AS d
  ON d.digest = m.digest
WHERE m.server_id = $1
AND   m.database_name = $2
AND   m.plan_id = ANY($3)";

    /// <summary>
    /// Decides whether one PLAN_REGRESSION candidate is a real plan regression or only a change of input (#5630, #5631).
    /// The shared rule (<see cref="PlanInputComparison"/>) judges in order: a statement with OPTION (RECOMPILE) is out; a
    /// newer plan that returns 10x the rows per call or more, with the regression gone per row, is out (its cost followed its
    /// output); neither of those reads a plan. Otherwise the two stored plans are read, and a pair compiled for different
    /// parameter values over unchanged statistics is out, while the same values, or different values after a statistics
    /// update, are kept. Anything that cannot be checked (a plan or the statement text is missing, unreadable or withheld, or
    /// the read failed) keeps the candidate, as before, and counts as unverified. A pair of plans already judged in this pass
    /// (<paramref name="planOutcomes"/>) is not read again. Only the outcome leaves
    /// <see cref="PlanInputComparison"/>: no value is logged or stored.
    /// </summary>
    private async Task<PlanInputJudgement> JudgePlanRegressionInputsAsync(
        NpgsqlConnection connection, AnalysisContext context, PlanRegressionCandidate candidate, PlanReadFailures planReads,
        Dictionary<(string Database, long Latest, long Best), PlanInputOutcome> planOutcomes)
    {
        var text = candidate.StatementText;
        var textUnknown = string.IsNullOrWhiteSpace(text) || WithheldStatementMarker.IsMarker(text);

        /* The statement and the rows per call decide the first two drops, so no plan is read for them. */
        var early = PlanInputComparison.JudgeBeforePlans(text, candidate.Factor, candidate.LatestRows, candidate.BestRows);
        if (PlanInputComparison.IsDrop(early))
            return new PlanInputJudgement(early, Unverified: false);

        if (candidate.LatestPlanId is not long latestId || candidate.BestPlanId is not long bestId)
            return new PlanInputJudgement(PlanInputOutcome.KeepUnverified, Unverified: true);

        var pairKey = (candidate.DatabaseName, latestId, bestId);
        if (planOutcomes.TryGetValue(pairKey, out var cachedOutcome))
            return JudgementFor(cachedOutcome);

        string? latestXml = null;
        string? bestXml = null;
        try
        {
            using var cmd = new NpgsqlCommand(PlanRegressionPlansSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(candidate.DatabaseName);
            cmd.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint,
                Value = new[] { latestId, bestId }
            });
            using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
            while (await reader.ReadAsync(context.CancellationToken))
            {
                var planId = ToInt64(reader.GetValue(0));
                var xml = PayloadDimensions.ResolveContent(
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    reader.IsDBNull(2) ? null : (byte[])reader.GetValue(2));
                if (planId == latestId)
                    latestXml = xml;
                if (planId == bestId)
                    bestXml = xml;
            }
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* The check could not run, so the candidate stays, as it did before the check existed. The caller records the
               failure once for the pass (#2826), so a broken connection is one event, not one per candidate. */
            if (planReads.Count == 0)
                ReportCollectionFailure(ex, context, nameof(CollectPlanRegressionFactsAsync), record: false);
            planReads.Add(ex);
            planOutcomes[pairKey] = PlanInputOutcome.KeepUnverified;
            return new PlanInputJudgement(PlanInputOutcome.KeepUnverified, Unverified: true);
        }

        var outcome = PlanInputComparison.JudgePlans(latestXml, bestXml);
        planOutcomes[pairKey] = outcome;
        return JudgementFor(outcome);

        PlanInputJudgement JudgementFor(PlanInputOutcome planOutcome) =>
            PlanInputComparison.IsDrop(planOutcome)
                ? new PlanInputJudgement(planOutcome, Unverified: false)
                /* A kept pair is unverified when the plans could not be compared, or when the statement text was missing or
                   withheld (the OPTION (RECOMPILE) check could not run). */
                : new PlanInputJudgement(planOutcome, Unverified: planOutcome == PlanInputOutcome.KeepUnverified || textUnknown);
    }

    public const string ProcedureStatsSql = @"
SELECT
    COUNT(DISTINCT object_name) AS distinct_procs,
    SUM(delta_execution_count) AS total_executions,
    SUM(delta_worker_time) AS total_cpu_time_us,
    SUM(delta_elapsed_time) AS total_elapsed_time_us,
    SUM(delta_logical_reads) AS total_logical_reads
FROM procedure_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
AND   delta_execution_count > 0";

    /// <summary>
    /// Collects procedure stats: top procedure by delta CPU time in the period.
    /// </summary>
    private async Task CollectProcedureStatsFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            using var cmd = new NpgsqlCommand(ProcedureStatsSql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
            cmd.Parameters.AddWithValue(context.ServerId);
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
            cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* Degrades to "no facts" so one unavailable input cannot cost this server its other
               facts — but WHY it degraded is reported, not assumed (#2826): a cancelled query is
               not "no data". An abandonment is NOT swallowed here (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    public const string PlanAdvisorySql = @"
SELECT query_plan_xml, query_plan_gz
FROM v_query_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3
/* #2069: plan content written since V54 lives as gzip bytes (query_plan_gz) with the text
   column NULL, so the presence guard and the projection both carry BOTH forms and the read
   loop resolves text-else-gz (PayloadDimensions.ResolveContent). A guard on query_plan_xml
   alone matches nothing on a store using the default plan_xml_compression = 'gzip', which
   costs this collector every MISSING_INDEX and PLAN_WARNING it exists to emit. */
AND   (query_plan_xml IS NOT NULL OR query_plan_gz IS NOT NULL)
ORDER BY delta_worker_time DESC
LIMIT 10";

    /// <summary>
    /// WS4: plan-XML advisories. Parses the already-collected query plans of the top queries by
    /// cost with the shared ShowPlanParser/PlanAnalyzer and emits two advise-only facts —
    /// MISSING_INDEX (Value = distinct suggested indexes) and PLAN_WARNING (Value = actionable
    /// warnings). The specifics ride in the finding drill-down (DrillDownCollector); Fact.Metadata
    /// is numeric only. Mirrors the Lite/Dashboard fact collectors against query_stats.query_plan_xml.
    /// </summary>
    private async Task CollectPlanAdvisoryFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        try
        {
            var planXmls = new List<string>();

            /* PG port: Lite scopes the connection in a block to release its DuckDB read lock
               before the CPU-only parse; the scoping is kept so the connection closes before
               the parse, even though PG holds no lock. */
            await using (var connection = await _postgres.OpenConnectionAsync(context.CancellationToken))
            {
                using var command = new NpgsqlCommand(PlanAdvisorySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
                command.Parameters.AddWithValue(context.ServerId);
                command.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                command.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await command.ExecuteReaderAsync(context.CancellationToken);
                while (await reader.ReadAsync(context.CancellationToken))
                {
                    var planXml = PayloadDimensions.ResolveContent(
                        reader.IsDBNull(0) ? null : reader.GetString(0),
                        reader.IsDBNull(1) ? null : reader.GetFieldValue<byte[]>(1));
                    if (!string.IsNullOrEmpty(planXml))
                        planXmls.Add(planXml);
                }
            }

            // Connection released above; parse off the store (CPU-only, no DB).
            if (planXmls.Count == 0)
                return;

            var summary = PlanAdvisoryAggregator.SummarizeCancellable(planXmls, _analyzerConfig, context.CancellationToken);

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
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            // query_stats / plan parse may be unavailable — skip, the advisory is best-effort.
            // An abandonment is NOT swallowed here (#2443).
            ReportCollectionFailure(ex, context);
        }
    }

}
