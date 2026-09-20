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

namespace PerformanceMonitor.Darling.Analysis;

public sealed partial class PgTargetFactCollector
{
    /// <summary>
    /// Whether <c>pg_stat_kcache</c> is INSTALLED on this target, from the newest <c>pg_extension_availability</c>
    /// capture at or before the window's end — the daily collector (#2545), so the lower bound <c>$2</c> is bound
    /// WIDER than the analysis window (<see cref="KernelAvailabilityLookbackDays"/> before its end): a four-hour
    /// window holds no availability row most of the day, and "no row in the window" must not read as "absent".
    /// <c>$1</c> server_id, <c>$2</c> lookback start, <c>$3</c> window end.
    ///
    /// <para><b>Per database, so the verdict is "installed anywhere".</b> V89's column names carry the warning:
    /// <c>installed_version</c> (and therefore <c>state</c>) is a claim about the CONNECTED database — the collector
    /// runs per database since V91 — while <c>pg_stat_kcache()</c> reports every database's statements from any one
    /// of them. So the newest capture's rows are counted by state, and the extension is installed for this family's
    /// purpose when ANY database row says <c>installed</c> or <c>outdated</c>; <c>available</c> (one <c>CREATE
    /// EXTENSION</c> away) and <c>absent</c> (the server does not offer it — Aurora, which is every PostgreSQL target
    /// the fleet monitors today) are both "not installed", told apart by the reason flag the fact carries. A NULL
    /// <c>collection_time</c> is "the availability collector has not run here": unknown, and unknown is not
    /// absent — the caller then lets the kernel rows speak for themselves (rows exist only if the function exists)
    /// and says nothing when there are none.</para>
    /// </summary>
    public const string PgTargetKernelAvailabilitySql = @"
WITH latest AS (
    SELECT MAX(collection_time) AS collection_time
    FROM pg_extension_availability
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   extension_name = 'pg_stat_kcache'
)
SELECT
    l.collection_time,
    CAST(count(a.state) FILTER (WHERE a.state IN ('installed', 'outdated')) AS integer) AS installed_databases,
    CAST(count(a.state) FILTER (WHERE a.state = 'available') AS integer)                AS available_databases,
    CAST(count(a.state) AS integer)                                                    AS rows_seen
FROM latest AS l
LEFT JOIN pg_extension_availability AS a
  ON  a.server_id = $1
  AND a.collection_time = l.collection_time
  AND a.extension_name = 'pg_stat_kcache'
GROUP BY l.collection_time";

    /// <summary>
    /// The window's kernel CPU from <c>pg_kernel_stats</c> (<c>pg_stat_kcache</c>, per (database, <c>query_id</c>),
    /// cumulative), differenced per identity and RATED PER COLLECTION — the unit the <c>pg_cpu_burn_cores</c>
    /// baseline arm buckets (<c>PgTargetBaselineProvider.Kernel.cs</c>) and the anomaly detector's window read by
    /// alias (<c>PgTargetAnomalyDetector.CpuBurnWindowSql</c>): one differencing, three readers, none re-derived.
    /// <c>$1</c> server_id, <c>$2</c>/<c>$3</c> window (naive UTC).
    ///
    /// <para><b>The differencing is <see cref="PgTargetCheckpointSql"/>'s, per IDENTITY.</b> <c>LAG</c> over each
    /// (database, <c>query_id</c>) series, the reset detected as <c>ROW_NUMBER() OVER identity &gt; 1 AND stats_since
    /// IS DISTINCT FROM LAG(stats_since)</c> — <c>DarlingPgDatabaseReader.PgDatabaseSql</c>'s note on why a
    /// <c>LAG(stamp) IS NOT NULL</c> guard misses the FIRST reset (NULL → timestamp) applies unchanged — OR the
    /// counter going backwards (belt to the stamp's brace, the table's own reader's rule). A reset row contributes
    /// its CURRENT value whole, not <c>GREATEST(raw, 0)</c>'s zero: <c>stats_since</c> is the instant the counters
    /// restarted from zero, it lies inside the interval when it moved, so the current value IS the CPU burned since —
    /// <c>DarlingPgKernelStatsReader.PgKernelStatsSql</c> records why the clamp would read a restart as a quiet
    /// server. The count of resets rides on the fact so the reader knows a floor when one applies.</para>
    ///
    /// <para><b>A delta counts only when its predecessor is the IMMEDIATELY preceding collection.</b> The collector
    /// captures the top 500 statements by CPU (<c>PgKernelStatsCollector</c>), so an identity can fall out of a
    /// capture and return with history the window never saw; <c>LAG</c> over the identity alone would hand that gap's
    /// whole CPU to the collection it reappeared in and spike one sample. So every row carries its collection's
    /// ordinal in the window (<c>DENSE_RANK() OVER (ORDER BY collection_time)</c>, <c>k</c>) and <c>deltas</c> keeps a
    /// row only when its identity's previous row was the collection immediately before (<c>prev_k = k - 1</c>) — the
    /// sampled-wait read's "a first in-window sighting contributes nothing" rule, per interval. The ordinal, not a
    /// join back to a distinct-times CTE: executed on a planted 31-day series the join shape ran past the 60-second
    /// baseline timeout on fresh, unanalysed rows (a nested loop over two window-function CTEs the planner had no
    /// statistics for), while the rank is one extra sort over rows the identity window already sorts. A statement that left and returned contributes nothing at
    /// its return; the sum is a floor, and the window's statements are the tracked population, not all work (the
    /// advice says so: background processes, untracked and nested statements are outside <c>pg_stat_kcache</c>).</para>
    ///
    /// <para><b>Cores busy.</b> Per collection, Σ over identities of <c>Δexec_user + Δexec_system + Δplan_cpu</c> ms
    /// over the collection's own gap in ms — CPU-seconds burned per wall second, which is CORES BUSY: a rate,
    /// engine-neutral, and the only CPU figure a target without <c>pg_cpu_utilization</c> has. No core count is
    /// collected (<c>pg_settings</c> carries none; <c>max_parallel_workers</c> is not cores), so this read yields no
    /// percentage and the family draws no absolute bar — the grade is <c>ANOMALY_PG_CPU_BURN</c>'s, against this
    /// server's own hour-of-week routine. Plan CPU is included in the total and stated as its own share: a
    /// parameter-sensitive or un-prepared workload burns real cores planning, and leaving it out would hide exactly
    /// the CPU the plan family's advice is about.</para>
    ///
    /// <para>The header columns are scalar subqueries over the CTEs (the WAL-volume read's shape) so the read returns
    /// exactly one row whether or not the window has rows: <c>collection_count = 0</c> is an EMPTY window, told apart
    /// from a window with rows and no rateable interval (<c>rated_samples = 0</c> — a single collection, or every
    /// identity new). The top statement by CPU rides along for the advice and the bad-actor hop: the CPU bad actor,
    /// which is a different ordering from lane 7's exec-time one (a statement that WAITS tops that list; one that
    /// BURNS tops this).</para>
    /// </summary>
    public const string PgTargetKernelCpuSql = @"
WITH ranked AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        coalesce(exec_user_time_ms, 0)   AS user_ms_total,
        coalesce(exec_system_time_ms, 0) AS system_ms_total,
        coalesce(plan_cpu_time_ms, 0)    AS plan_ms_total,
        stats_since,
        DENSE_RANK() OVER (ORDER BY collection_time) AS k
    FROM pg_kernel_stats
    WHERE server_id = $1
    AND   collection_time >= $2
    AND   collection_time <= $3
    AND   query_id IS NOT NULL
),
series AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        user_ms_total,
        system_ms_total,
        plan_ms_total,
        stats_since,
        k,
        LAG(k)               OVER identity AS prev_k,
        LAG(collection_time) OVER identity AS prev_time,
        LAG(user_ms_total)   OVER identity AS prev_user,
        LAG(system_ms_total) OVER identity AS prev_system,
        LAG(plan_ms_total)   OVER identity AS prev_plan,
        LAG(stats_since)     OVER identity AS prev_since
    FROM ranked
    WINDOW identity AS (PARTITION BY database_name, query_id ORDER BY collection_time)
),
deltas AS (
    SELECT
        collection_time,
        database_name,
        query_id,
        extract(epoch FROM (date_trunc('second', collection_time) - date_trunc('second', prev_time))) AS interval_sec,
        (stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user) AS reset_here,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN user_ms_total   ELSE user_ms_total   - prev_user   END AS user_ms,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN system_ms_total ELSE system_ms_total - prev_system END AS system_ms,
        CASE WHEN stats_since IS DISTINCT FROM prev_since OR user_ms_total < prev_user
             THEN plan_ms_total   ELSE GREATEST(plan_ms_total - prev_plan, 0) END AS plan_ms
    FROM series
    WHERE prev_k = k - 1
),
rated AS (
    SELECT *
    FROM deltas
    WHERE interval_sec > 0
),
per_collection AS (
    SELECT
        collection_time,
        interval_sec,
        SUM(user_ms)   AS user_ms,
        SUM(system_ms) AS system_ms,
        SUM(plan_ms)   AS plan_ms,
        (SUM(user_ms) + SUM(system_ms) + SUM(plan_ms)) / (interval_sec * 1000.0) AS cores_busy,
        CAST(count(*) FILTER (WHERE reset_here) AS integer) AS resets
    FROM rated
    GROUP BY collection_time, interval_sec
),
by_query AS (
    SELECT database_name, query_id, SUM(user_ms + system_ms + plan_ms) AS cpu_ms
    FROM rated
    GROUP BY database_name, query_id
    ORDER BY SUM(user_ms + system_ms + plan_ms) DESC, query_id
    LIMIT 1
)
SELECT
    (SELECT MAX(cores_busy) FROM per_collection)                                    AS peak_cores_busy,
    (SELECT AVG(cores_busy) FROM per_collection)                                    AS mean_cores_busy,
    (SELECT CAST(count(*) AS integer) FROM per_collection)                          AS rated_samples,
    (SELECT coalesce(SUM(user_ms), 0) FROM per_collection)                          AS user_ms,
    (SELECT coalesce(SUM(system_ms), 0) FROM per_collection)                        AS system_ms,
    (SELECT coalesce(SUM(plan_ms), 0) FROM per_collection)                          AS plan_ms,
    (SELECT coalesce(SUM(interval_sec), 0) FROM per_collection)                     AS rated_sec,
    (SELECT CAST(coalesce(SUM(resets), 0) AS integer) FROM per_collection)          AS reset_count,
    (SELECT CAST(coalesce(MAX(k), 0) AS integer) FROM ranked)                       AS collection_count,
    (SELECT query_id FROM by_query)                                                 AS top_query_id,
    (SELECT cpu_ms FROM by_query)                                                   AS top_query_cpu_ms,
    (SELECT database_name FROM by_query)                                            AS top_query_database";

    /// <summary>How far before the window's end the availability read looks for the newest
    /// <c>pg_extension_availability</c> capture: the collector is daily (<c>CollectorScheduleDefaults</c>, 1440 min),
    /// so three days tolerates two missed runs before the verdict becomes "unknown" — which is silence, never
    /// "absent".</summary>
    internal const int KernelAvailabilityLookbackDays = 3;

    /// <summary>Metadata KEY (the numeric-metadata idiom) on the <c>unavailable</c> shape: the newest availability
    /// capture says the server does not offer <c>pg_stat_kcache</c> at all (Aurora — every measured cluster).</summary>
    public const string KernelReasonKcacheAbsentKey = "reason_pg_stat_kcache_absent";

    /// <summary>Metadata KEY beside <see cref="KernelReasonKcacheAbsentKey"/> when the extension is offered but not
    /// created in any database the collector connected to — one <c>CREATE EXTENSION</c> away, the actionable state.</summary>
    public const string KernelReasonKcacheAvailableKey = "reason_pg_stat_kcache_available";

    /// <summary>
    /// <c>PG_CPU_BURN_CORES</c> from the window's <c>pg_kernel_stats</c> — the reset-aware differences of
    /// <c>exec_user_time_ms + exec_system_time_ms + plan_cpu_time_ms</c> (<c>stats_since</c> the reset witness) summed
    /// across statements per collection, over the collection's own gap: cores busy — and <c>PG_CPU_DECOMPOSITION</c>,
    /// that CPU time against the wait facts already in the list (burning versus waiting). /* filled by lane 28 of
    /// #3691 — the marker stays, as v1's did. Two reads in this file, both <c>public const string …Sql</c> so
    /// <see cref="AllSql"/> reflects them; <c>CommandTimeout = FactCommandTimeoutSeconds</c> on each,
    /// <c>context.CancellationToken</c> on every store call, <c>$N</c> positional, no bare <c>now()</c>, the
    /// <c>when (!AnalysisShutdown.IsExpectedAbandon(…))</c> fence around <see cref="ReportCollectionFailure"/>; the
    /// rate's denominator is the kernel source's OWN rated time with the witness's
    /// <see cref="AnalysisContext.ObservedDurationMs"/> stated beside it (the wait family's rule), never the nominal
    /// window. */
    ///
    /// <para><b>Availability is READ, never inferred — the honesty arm.</b> <see cref="PgTargetKernelAvailabilitySql"/>
    /// first. Not installed (the newest capture says <c>absent</c> or only <c>available</c>) ⇒ exactly ONE fact,
    /// <c>PG_CPU_BURN_CORES</c> in its <c>unavailable</c> shape with the reason flag — value 0, no cores, no
    /// decomposition, and the detector (which reads the same table and finds no rows) emits no anomaly — so an
    /// operator on the measured fleet (Aurora does not ship <c>pg_stat_kcache</c>; fifty clusters, no population)
    /// sees "not measurable here, and why", never a zero read as an idle server. Installed ⇒ the CPU read. Unknown
    /// (the availability collector has never run here) ⇒ the CPU read decides: rows exist only if the function does;
    /// no rows is a collector that has not run, which the coverage witness describes, so nothing is emitted. A
    /// present extension with rows but no rateable interval (one collection) emits nothing either — a coverage
    /// statement, not zero CPU. The earlier stub said absence was a silent no-fact; the fleet calibration made the
    /// case for saying so on the face, since silence on every monitored target would read as "the family never ran".</para>
    ///
    /// <para><b>The decomposition reads the wait facts already emitted, not the wait tables again.</b> This family
    /// runs LAST in <c>CollectFactsAsync</c> (the root's emission order — pinned) precisely so it can: the wait
    /// partial has already chosen the source (Aurora's measured deltas, or stock's sampled estimate — READ-then-
    /// fallback on the data) and stamped every wait fact with <c>wait_ms</c>, <c>share_of_wait_time</c>,
    /// <c>wait_source_observed_ms</c> and <c>is_sampled</c>; re-running its SQL here would be a second copy of the
    /// hardest read in v1 (the three-state interval, the reset rule) that could drift from the first. Both sides are
    /// normalised to RATES before they are shared — cores busy over the kernel source's rated time, backends waiting
    /// over the wait source's observed time — because the two sources have their own cadences and a gap in one is
    /// not a gap in the other. The decomposition is emitted only when both sides are known: a pass with no wait fact
    /// (no wait source wrote the window, or the window had no waiting) cannot say what share waiting was, and a
    /// share invented from one side would be the lie the family exists to stop.</para>
    /// </summary>
    private async partial Task CollectKernelFactsAsync(AnalysisContext context, List<Fact> facts)
    {
        if (context.ObservedDurationMs <= 0) return;

        try
        {
            await using var connection = await _postgres.OpenConnectionAsync(context.CancellationToken);

            var availability = await ReadKernelAvailabilityAsync(context, connection);
            if (availability is { Installed: false, Known: true })
            {
                facts.Add(UnavailableCpuBurn(context, availability.Available));
                return;
            }

            KernelCpuRow row;
            using (var cmd = new NpgsqlCommand(PgTargetKernelCpuSql, connection) { CommandTimeout = FactCommandTimeoutSeconds })
            {
                cmd.Parameters.AddWithValue(context.ServerId);
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeStart));
                cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

                using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
                if (!await reader.ReadAsync(context.CancellationToken)) return;

                row = new KernelCpuRow(
                    PeakCores: reader.IsDBNull(0) ? 0.0 : Convert.ToDouble(reader.GetValue(0)),
                    MeanCores: reader.IsDBNull(1) ? 0.0 : Convert.ToDouble(reader.GetValue(1)),
                    RatedSamples: reader.IsDBNull(2) ? 0 : Convert.ToInt32(reader.GetValue(2)),
                    UserMs: Convert.ToDouble(reader.GetValue(3)),
                    SystemMs: Convert.ToDouble(reader.GetValue(4)),
                    PlanMs: Convert.ToDouble(reader.GetValue(5)),
                    RatedSec: Convert.ToDouble(reader.GetValue(6)),
                    ResetCount: Convert.ToInt32(reader.GetValue(7)),
                    CollectionCount: Convert.ToInt32(reader.GetValue(8)),
                    TopQueryId: reader.IsDBNull(9) ? null : ToInt64(reader.GetValue(9)),
                    TopQueryCpuMs: reader.IsDBNull(10) ? 0.0 : Convert.ToDouble(reader.GetValue(10)),
                    TopQueryDatabase: reader.IsDBNull(11) ? null : reader.GetString(11));
            }

            EmitKernelFacts(context, facts, row);
        }
        catch (Exception ex) when (!AnalysisShutdown.IsExpectedAbandon(ex, context.CancellationToken))
        {
            /* pg_kernel_stats arrived in V97 and pg_extension_availability in V89 (database_name in V95). A
               pre-migration store raises 42P01 / 42703 here, which the reporter classifies quiet. An abandonment is
               NOT swallowed (#2443). */
            ReportCollectionFailure(ex, context);
        }
    }

    /// <summary>The availability read — see <see cref="PgTargetKernelAvailabilitySql"/>. <c>Known</c> is false when
    /// the collector has never captured this server (no capture in the lookback), and the caller treats unknown as
    /// "let the rows speak", never as absent.</summary>
    private async Task<KernelAvailability> ReadKernelAvailabilityAsync(AnalysisContext context, NpgsqlConnection connection)
    {
        using var cmd = new NpgsqlCommand(PgTargetKernelAvailabilitySql, connection) { CommandTimeout = FactCommandTimeoutSeconds };
        cmd.Parameters.AddWithValue(context.ServerId);
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd.AddDays(-KernelAvailabilityLookbackDays)));
        cmd.Parameters.AddWithValue(AsNaive(context.TimeRangeEnd));

        using var reader = await cmd.ExecuteReaderAsync(context.CancellationToken);
        if (!await reader.ReadAsync(context.CancellationToken) || reader.IsDBNull(0))
            return new KernelAvailability(Known: false, Installed: false, Available: false);

        var installed = Convert.ToInt32(reader.GetValue(1));
        var available = Convert.ToInt32(reader.GetValue(2));
        return new KernelAvailability(Known: true, Installed: installed > 0, Available: installed == 0 && available > 0);
    }

    /// <summary>One header row of <see cref="PgTargetKernelCpuSql"/>. Internal so <c>PgTargetKernelTests</c> can drive
    /// the arithmetic through <see cref="EmitKernelFacts"/> on arranged rows without a store.</summary>
    internal readonly record struct KernelCpuRow(
        double PeakCores, double MeanCores, int RatedSamples,
        double UserMs, double SystemMs, double PlanMs, double RatedSec,
        int ResetCount, int CollectionCount,
        long? TopQueryId, double TopQueryCpuMs, string? TopQueryDatabase);

    private readonly record struct KernelAvailability(bool Known, bool Installed, bool Available);

    /// <summary>
    /// Stamps <c>PG_CPU_BURN_CORES</c> from a rated window and, when the pass carries wait facts,
    /// <c>PG_CPU_DECOMPOSITION</c> beside it. Nothing when the window rated no interval (see the method remarks).
    /// Internal so the tests execute the cores-busy arithmetic, the shares and the "both sides known" rule on
    /// arranged rows — the seam <see cref="EmitWaitFacts"/> offers the wait pins.
    /// </summary>
    internal static void EmitKernelFacts(AnalysisContext context, List<Fact> facts, KernelCpuRow row)
    {
        if (row.RatedSamples == 0 || row.RatedSec <= 0) return;

        var cpuMs = row.UserMs + row.SystemMs + row.PlanMs;
        var kernelObservedMs = row.RatedSec * 1000.0;
        var coresBusy = cpuMs / kernelObservedMs;
        var execMs = row.UserMs + row.SystemMs;

        var burn = new Fact
        {
            Source = PgTargetSources.KernelSource,
            Key = PgTargetFactKeys.CpuBurnCores,
            Value = coresBusy,
            ServerId = context.ServerId,
            Metadata =
            {
                [PgTargetScorer.KernelCoresBusyKey] = coresBusy,
                [PgTargetScorer.KernelCoresBusyPeakKey] = row.PeakCores,
                [PgTargetScorer.KernelCoresBusyMeanKey] = row.MeanCores,
                ["cpu_ms"] = cpuMs,
                ["user_ms"] = row.UserMs,
                ["system_ms"] = row.SystemMs,
                ["plan_cpu_ms"] = row.PlanMs,
                [PgTargetScorer.KernelUserShareKey] = execMs > 0 ? row.UserMs / execMs : 0,
                [PgTargetScorer.KernelPlanCpuShareKey] = cpuMs > 0 ? row.PlanMs / cpuMs : 0,
                ["reset_count"] = row.ResetCount,
                ["rated_samples"] = row.RatedSamples,
                ["collection_count"] = row.CollectionCount,
                [PgTargetScorer.KernelObservedMsKey] = kernelObservedMs,
                ["observed_ms"] = context.ObservedDurationMs,
                /* The same CPU over the pass's coverage witness (pg_database_stats) — stated beside the source's own
                   denominator so a reader can see when the two cadences disagree (the wait family's witness_fraction). */
                ["witness_cores_busy"] = cpuMs / context.ObservedDurationMs,
                ["coverage_fraction"] = context.Coverage?.Fraction ?? 0,
                /* 1, not 0: no bar was chosen for this fact — it is graded only through ANOMALY_PG_CPU_BURN against
                   the server's own baseline (the PG_WAL_VOLUME_SHIFT rule) — so there is no unmeasured number for the
                   flag to disclose here; the anomaly's fact discloses its own (threshold_lineage = 0, both bars unmeasured). */
                ["threshold_lineage"] = 1,
            },
        };
        if (row.TopQueryId is { } topId)
        {
            burn.Metadata[PgTargetScorer.KernelTopQueryIdKey] = topId;
            burn.Metadata[PgTargetScorer.KernelTopQueryShareKey] = cpuMs > 0 ? row.TopQueryCpuMs / cpuMs : 0;
            burn.Metadata["top_query_cpu_ms"] = row.TopQueryCpuMs;
            /* The statement's database travels through the one string seam a server-scoped fact has that the
               reconciler does not fold on (DatabaseName is the fold key — and this fact is the server's, not the
               database's); the id itself is a double in Metadata, exact to 2^53, which every queryid is not, so the
               advice renders it with "0" formatting and the bad-actor hop matches through PgTargetFactKeys.BadActorKey
               on the same double. */
            if (row.TopQueryDatabase is { Length: > 0 } db)
                burn.ObjectName = db;
        }
        facts.Add(burn);

        /* ── The decomposition: this CPU against the wait profile the wait partial already emitted (method remarks). */
        double totalWaitMs = 0, waitObservedMs = 0, ioWaitMs = 0;
        var waitKnown = false;
        var waitSampled = false;
        foreach (var fact in facts)
        {
            if (fact.Source != PgTargetSources.WaitsSource) continue;
            var observed = fact.Metadata.GetValueOrDefault(PgTargetScorer.WaitSourceObservedMsKey);
            if (observed <= 0) continue;
            var share = fact.Metadata.GetValueOrDefault(PgTargetScorer.WaitShareOfWaitTimeKey);
            var waitMs = fact.Metadata.GetValueOrDefault(PgTargetScorer.WaitMsKey);
            if (!waitKnown && share > 0 && waitMs > 0)
            {
                /* Every wait fact carries its share of ALL waiting (CPU excluded), so one fact recovers the total the
                   wait partial summed over every event, named in the vocabulary or not. */
                totalWaitMs = waitMs / share;
                waitObservedMs = observed;
                waitSampled = fact.Metadata.GetValueOrDefault(PgTargetScorer.WaitIsSampledKey) >= 1.0;
                waitKnown = true;
            }
            if (fact.Key == PgTargetFactKeys.WaitKey("IO", null))
                ioWaitMs = waitMs;
        }
        if (!waitKnown) return;

        var backendsWaiting = totalWaitMs / waitObservedMs;
        var ioBackendsWaiting = ioWaitMs / waitObservedMs;
        var backendTime = coresBusy + backendsWaiting;
        if (backendTime <= 0) return;

        facts.Add(new Fact
        {
            Source = PgTargetSources.KernelSource,
            Key = PgTargetFactKeys.CpuDecomposition,
            Value = coresBusy / backendTime,
            ServerId = context.ServerId,
            Metadata =
            {
                [PgTargetScorer.KernelBurnShareKey] = coresBusy / backendTime,
                [PgTargetScorer.KernelWaitShareKey] = backendsWaiting / backendTime,
                [PgTargetScorer.KernelIoWaitShareKey] = ioBackendsWaiting / backendTime,
                [PgTargetScorer.KernelCoresBusyKey] = coresBusy,
                [PgTargetScorer.KernelBackendsWaitingKey] = backendsWaiting,
                ["cpu_ms"] = cpuMs,
                ["wait_ms"] = totalWaitMs,
                ["io_wait_ms"] = ioWaitMs,
                [PgTargetScorer.KernelObservedMsKey] = kernelObservedMs,
                [PgTargetScorer.WaitSourceObservedMsKey] = waitObservedMs,
                [PgTargetScorer.KernelWaitIsSampledKey] = waitSampled ? 1 : 0,
                ["observed_ms"] = context.ObservedDurationMs,
            },
        });
    }

    /// <summary>The <c>PG_CPU_BURN_CORES</c> fact in its <c>unavailable</c> shape: value 0, <c>unavailable = 1</c> and
    /// exactly one reason flag — <see cref="KernelReasonKcacheAbsentKey"/> when the server does not offer the
    /// extension, <see cref="KernelReasonKcacheAvailableKey"/> when it is offered but created in no database the
    /// collector connected to (a metadata KEY per reason — the numeric-metadata idiom). The scorer grades nothing off
    /// it; the advice reads the flag and says which. No decomposition is emitted beside it: one side of the split is
    /// unmeasurable.</summary>
    private static Fact UnavailableCpuBurn(AnalysisContext context, bool available) =>
        new()
        {
            Source = PgTargetSources.KernelSource,
            Key = PgTargetFactKeys.CpuBurnCores,
            Value = 0,
            ServerId = context.ServerId,
            Metadata =
            {
                ["unavailable"] = 1,
                [available ? KernelReasonKcacheAvailableKey : KernelReasonKcacheAbsentKey] = 1,
                ["observed_ms"] = context.ObservedDurationMs,
                ["threshold_lineage"] = 1,
            },
        };
}
