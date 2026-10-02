/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;
using System.Text;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the kernel-time family (filled by lane 28 of #3691). Value-stated from the facts — cores busy over the
/// window (peak, mean, and against the routine when the anomaly carries one), the user/system split, the plan-CPU
/// share, the statement that burned the most kernel time by <c>query_id</c>, and the burning-versus-waiting
/// decomposition that says whether the box is out of CPU or the backends are parked — with the counter-objective
/// named on every recommendation. Every block says the reading is <c>pg_stat_kcache</c>'s (per top-level statement,
/// so background processes, nested and untracked work are outside it) and that no core count is known here, so a
/// cores-busy figure is never a host CPU percent. No <c>CREATE INDEX</c> text anywhere (D8); durability settings are
/// not a lever (D6). The <c>unavailable</c> shape gets its own card: on the measured fleet the extension does not
/// exist, and "not measurable here, and why" must never read as "no CPU".
/// </summary>
public static partial class PgTargetAdvice
{
    /// <summary>The levers and their costs, stated once for the proxy, the anomaly and the compute-bound
    /// decomposition alike. The statement first — CPU is burned by statements, and <c>pg_stat_kcache</c> names them.</summary>
    private const string CpuBurnLevers =
        "Start with the statement: get_pg_kernel_stats ranks this window's statements by the CPU they burned (user and system " +
        "time from the kernel, not elapsed time), and get_pg_top_queries has the same query_id's text, calls and mean time — a " +
        "query that BURNS tops the first list and one that WAITS tops the second, so the two orderings differ on purpose. " +
        "Then the engine's CPU levers, each with its cost: max_parallel_workers_per_gather decides how many cores one query may " +
        "take — raising it makes that query faster and leaves fewer cores for everything else, lowering it does the reverse; " +
        "jit spends compile CPU on every eligible plan and pays it back only on long analytic statements — on a short-statement " +
        "OLTP workload it is CPU spent for nothing, on a reporting one turning it off costs the speedup; and a high plan-CPU " +
        "share means planning itself is the burn — prepared statements and plan_cache_mode are the levers there, at the cost " +
        "of the generic plan not fitting every parameter. No host CPU percent is known here: pg_stat_kcache counts CPU " +
        "seconds, not cores installed, so read the host's own metrics for the ceiling before adding cores.";

    /* filled by lane 28 */
    private static partial AdviceBlock? ComposeKernel(string key, IReadOnlyDictionary<string, Fact> factsByKey) =>
        key switch
        {
            PgTargetFactKeys.CpuBurnCores => ComposeCpuBurnCores(factsByKey),
            PgTargetFactKeys.CpuDecomposition => ComposeCpuDecomposition(factsByKey),
            _ => null,
        };

    /// <summary>
    /// <c>PG_CPU_BURN_CORES</c>: the context fact's own block — the window's cores busy (peak, mean), the user/system and
    /// plan-CPU shares and the top statement, read off the fact and never assumed; or, in the <c>unavailable</c> shape,
    /// which reason. Base 0, so this roots no card; reached by key through <c>get_analysis_facts</c> and
    /// <see cref="Static"/>.
    /// </summary>
    private static AdviceBlock ComposeCpuBurnCores(IReadOnlyDictionary<string, Fact> facts)
    {
        var burn = KnobFact(facts, PgTargetFactKeys.CpuBurnCores);

        if (burn is not null && (KnobMeta(burn, "unavailable") ?? 0) > 0)
        {
            var offered = (KnobMeta(burn, "reason_pg_stat_kcache_available") ?? 0) > 0;
            var reason = offered
                ? "the server offers pg_stat_kcache but it is created in no database the collector connected to — one CREATE EXTENSION pg_stat_kcache away, in the database whose statements should be measured"
                : "this server does not offer pg_stat_kcache at all (Aurora PostgreSQL does not ship it; a managed or self-hosted PostgreSQL without the package installed reads the same way)";
            return new AdviceBlock(
                Headline: "Kernel CPU per statement is not measurable on this server",
                Investigation:
                    $"pg_stat_kcache is not installed here, so no per-statement CPU time exists to sum: {reason}. No cores-busy figure is " +
                    "stated and nothing is graded — an absent instrument is not an idle server. The reading comes from pg_extension_availability, " +
                    "the daily capture of what this server has, could have and cannot have; it is read, not inferred from an empty table.",
                Remediation: offered
                    ? "CREATE EXTENSION pg_stat_kcache (it rides on pg_stat_statements, which must be in shared_preload_libraries) in the " +
                      "database whose statements should be measured; the collector picks the rows up on its next run and this family fills in. " +
                      "Until then: elapsed time per statement from get_pg_top_queries, which cannot separate burning from waiting."
                    : "Where the engine reports it, read CPU from the platform's own metrics (on Aurora, get_pg_cpu_utilization is the " +
                      "percent of the configured capacity ceiling); per-statement elapsed time is get_pg_top_queries, which cannot separate " +
                      "burning from waiting.");
        }

        if (burn is null || KnobMeta(burn, PgTargetScorer.KernelCoresBusyKey) is not { } cores)
        {
            return new AdviceBlock(
                Headline: "CPU burned by statements this window, in cores busy",
                Investigation:
                    "The window's user-plus-system-plus-plan CPU seconds per wall second across every statement pg_stat_kcache tracks " +
                    "(pg_kernel_stats, differenced per statement with stats_since honoured) — cores busy. Context, not a finding: no core " +
                    "count is collected, so there is no percentage and no absolute bar — it is graded only by ANOMALY_PG_CPU_BURN against " +
                    "this server's own hour-of-week routine. Per top-level statement only: background processes, autovacuum and nested " +
                    "statements are outside the sum, so it is a floor on the host's CPU, never a host CPU percent.",
                Remediation: CpuBurnLevers);
        }

        var peak = KnobMeta(burn, PgTargetScorer.KernelCoresBusyPeakKey) ?? cores;
        var mean = KnobMeta(burn, PgTargetScorer.KernelCoresBusyMeanKey) ?? cores;
        var userShare = KnobMeta(burn, PgTargetScorer.KernelUserShareKey) ?? 0;
        var planShare = KnobMeta(burn, PgTargetScorer.KernelPlanCpuShareKey) ?? 0;
        var resets = KnobMeta(burn, "reset_count") ?? 0;
        var sb = new StringBuilder(640);
        sb.Append("Over the observed window this server's statements burned ").Append(Cores(cores))
          .Append(" busy on average — peaking at ").Append(Cores(peak)).Append(" in one collection interval, a per-collection mean of ")
          .Append(Cores(mean)).Append(" — ").Append(KnobPct(userShare)).Append(" of the execution CPU in user time and the rest in the kernel");
        if (planShare > 0)
            sb.Append("; planning was ").Append(KnobPct(planShare)).Append(" of all CPU counted");
        sb.Append(". ");
        AppendTopStatement(sb, burn);
        if (resets > 0)
            sb.Append("Statistics were reset ").Append(KnobNum(resets)).Append(" time(s) inside the window, so the sums are a floor. ");
        sb.Append("Context, not a finding: no core count is collected for this server, so ").Append(Cores(peak))
          .Append(" busy is not a percentage of anything — it is graded only by ANOMALY_PG_CPU_BURN against this server's own hour-of-week " +
                  "routine. The figure is pg_stat_kcache's, per top-level statement: background processes, autovacuum and nested statements " +
                  "are outside it, so it is a floor on the host's CPU and never a host CPU percent.");

        return new AdviceBlock(
            Headline: $"CPU burn: {Cores(cores)} busy across the window, {Cores(peak)} at peak",
            Investigation: sb.ToString(),
            Remediation: CpuBurnLevers);
    }

    /// <summary>The top-statement sentence, shared by the proxy's block and the anomaly's.</summary>
    private static void AppendTopStatement(StringBuilder sb, Fact burn)
    {
        if (KnobMeta(burn, PgTargetScorer.KernelTopQueryIdKey) is not { } topId) return;
        var share = KnobMeta(burn, PgTargetScorer.KernelTopQueryShareKey) ?? 0;
        sb.Append("Statement ").Append(QueryId(topId));
        if (!string.IsNullOrEmpty(burn.ObjectName))
            sb.Append(" in ").Append(burn.ObjectName);
        sb.Append(" burned ").Append(KnobPct(share)).Append(" of it — the CPU bad actor, ranked by kernel time, which is not the same " +
                  "ordering as the elapsed-time top statements. ");
    }

    /// <summary>
    /// <c>PG_CPU_DECOMPOSITION</c>: burning versus waiting as shares of observed backend time, which side dominates, at
    /// which evidence grade the wait side rests (measured deltas or a sampling estimate), and where to look next.
    /// The informational base roots no card; the co-fire that lifts it to 0.6 is read off the fact's amplifier results
    /// upstream, and the prose says which side's instrument to read regardless.
    /// </summary>
    private static AdviceBlock ComposeCpuDecomposition(IReadOnlyDictionary<string, Fact> facts)
    {
        var split = KnobFact(facts, PgTargetFactKeys.CpuDecomposition);

        if (split is null || KnobMeta(split, PgTargetScorer.KernelBurnShareKey) is not { } burnShare)
        {
            return new AdviceBlock(
                Headline: "Burning versus waiting: how this window's backend time was spent",
                Investigation:
                    "CPU seconds the kernel charged to statements (pg_stat_kcache, per top-level statement) against the wait time the wait " +
                    "profile measured or estimated over the same window, each as a rate over its own source's observed time and then as " +
                    "shares of the two together — burn_share, wait_share and the IO wait's slice. Context, not a finding: a split is " +
                    "always worth reading and never alone a problem. It becomes a finding when one side holds at least four fifths and " +
                    "that side's own instrument fired — the CPU-burn anomaly for compute-bound, a named wait crossing its bar for wait-bound.",
                Remediation:
                    "Compute-bound: the statements (get_pg_kernel_stats, get_pg_top_queries) and the CPU levers on the PG_CPU_BURN_CORES card. " +
                    "Wait-bound: the wait cards and get_pg_wait_stats — the CPU is not the constraint, whatever the backends are queued on is.");
        }

        var waitShare = KnobMeta(split, PgTargetScorer.KernelWaitShareKey) ?? (1 - burnShare);
        var ioShare = KnobMeta(split, PgTargetScorer.KernelIoWaitShareKey) ?? 0;
        var cores = KnobMeta(split, PgTargetScorer.KernelCoresBusyKey) ?? 0;
        var waiting = KnobMeta(split, PgTargetScorer.KernelBackendsWaitingKey) ?? 0;
        var sampled = (KnobMeta(split, PgTargetScorer.KernelWaitIsSampledKey) ?? 0) >= 1.0;
        var computeBound = burnShare >= PgTargetScorer.CpuDecompositionDominantShare;
        var waitBound = waitShare >= PgTargetScorer.CpuDecompositionDominantShare;

        var sb = new StringBuilder(640);
        sb.Append("Of this window's backend time, ").Append(KnobPct(burnShare)).Append(" was burning CPU (").Append(Cores(cores))
          .Append(" busy, from pg_stat_kcache) and ").Append(KnobPct(waitShare)).Append(" was waiting (")
          .Append(waiting.ToString("0.##", CultureInfo.InvariantCulture)).Append(" backend-equivalents parked");
        if (ioShare > 0)
            sb.Append(", ").Append(KnobPct(ioShare)).Append(" of the whole on IO waits");
        sb.Append("). ");
        sb.Append(sampled
            ? "The wait side is estimated from sampling (pg_wait_sampling: backend-samples times the sampling period), so it cannot see a " +
              "wait shorter than the period and counts per backend-sample; the CPU side is the kernel's measured time. "
            : "The wait side is the engine's measured wait time (pg_wait_stats deltas); the CPU side is the kernel's measured time. ");
        sb.Append(computeBound
            ? "Compute-bound: the backends were running, not queued — adding wait tuning here would move nothing; the statements burning " +
              "the cores are the lever, and whether the host has more cores to give is not known from this reading. "
            : waitBound
                ? "Wait-bound: the backends were parked, not running — the CPU is not the constraint, and the wait cards name what is. "
                : "Mixed: neither side holds four fifths, so both the statements and the waits are worth a look before either is blamed. ");
        sb.Append("Context, not a finding on its own: the split becomes a finding only when the dominant side's own instrument fired in the same window.");

        var headline = computeBound
            ? $"Compute-bound: {KnobPct(burnShare)} of backend time was burning CPU ({Cores(cores)} busy)"
            : waitBound
                ? $"Wait-bound: {KnobPct(waitShare)} of backend time was waiting ({waiting.ToString("0.##", CultureInfo.InvariantCulture)} backends parked)"
                : $"Burning {KnobPct(burnShare)} versus waiting {KnobPct(waitShare)} of backend time this window";

        return new AdviceBlock(
            Headline: headline,
            Investigation: sb.ToString(),
            Remediation: computeBound
                ? CpuBurnLevers
                : "Read the wait side first: get_pg_wait_stats for the profile and the PG_WAIT_* cards for the named waits that fired — each " +
                  "names PostgreSQL's own lever for that wait with its cost. CPU levers (parallelism, jit) would change nothing here: the " +
                  "backends were not running. If the IO slice is the bulk of the waiting, the buffer-cache and checkpoint cards carry the " +
                  "sizing arithmetic.");
    }

    /// <summary>The static block for <c>ANOMALY_PG_CPU_BURN</c> — the source sentence the composed block keeps. Built on
    /// first use, not as a <c>static readonly</c> initializer: it concatenates <c>s_anomalyHedge</c> from another partial
    /// file, and C# leaves cross-file static initialization order unspecified (the write family's note records the
    /// trap). One instance, so <c>ReferenceEquals</c> still tells "fell back" from "composed".</summary>
    private static AdviceBlock CpuBurnStatic => s_cpuBurnStatic ??= new(
        Headline: "CPU burned by statements ran well above this server's normal for this time of week",
        Investigation:
            "The window's peak cores busy — user-plus-system-plus-plan CPU seconds per wall second across every statement pg_stat_kcache " +
            "tracks, from pg_kernel_stats differenced per statement with stats_since honoured — was judged against this server's " +
            "hour-of-week baseline of the same rate over the last 30 days, and the window's MEAN had to clear the same gate, so this is " +
            "cores busy for much of the window, not one hot minute. No core count is known here: this is a deviation from the server's " +
            "own routine, not a percentage of its capacity." + s_anomalyHedge,
        Remediation: CpuBurnLevers);

    private static AdviceBlock? s_cpuBurnStatic;

    /* filled by lane 28 — the ANOMALY_PG_CPU_BURN arm of ComposeAnomaly delegates here (PgTargetAdvice.Anomaly.cs), so
       the anomaly's prose lives with its family and the shared prefix routing never reaches a SQL Server composer. */
    private static partial AdviceBlock? ComposeCpuBurnAnomaly(IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var fallback = CpuBurnStatic;
        if (!factsByKey.TryGetValue(PgTargetFactKeys.AnomalyCpuBurn, out var fact))
            return fallback;

        var block = ComposeDeviation(fact, fallback, "CPU burn", "peak_cores_busy", v => Cores(v) + " busy");
        if (ReferenceEquals(block, fallback)) return block;

        /* The centre the detector divided by: the robust median when the bucket had one, else the mean — the same
           choice ComposeDeviation's prose makes, so the two numbers in one paragraph are one number. */
        var sb = new StringBuilder(320);
        var ratio = KnobMeta(fact, "baseline_ratio");
        var median = KnobMeta(fact, "baseline_median");
        var centre = median is > 0 ? median : KnobMeta(fact, "baseline_mean");
        if (ratio is > 0 && centre is > 0)
            sb.Append(" That is ").Append(KnobNum(ratio.Value)).Append("× the ").Append(Cores(centre.Value))
              .Append(" this server routinely keeps busy at this hour of the week");
        var mean = KnobMeta(fact, "mean_cores_busy");
        if (mean is > 0)
            sb.Append(ratio is > 0 ? ", and the window's mean was " : " The window's mean was ").Append(Cores(mean.Value)).Append(" busy.");
        else if (ratio is > 0)
            sb.Append('.');

        var burn = KnobFact(factsByKey, PgTargetFactKeys.CpuBurnCores);
        if (burn is not null && (KnobMeta(burn, "unavailable") ?? 0) == 0)
        {
            var userShare = KnobMeta(burn, PgTargetScorer.KernelUserShareKey);
            if (userShare is not null)
                sb.Append(' ').Append(KnobPct(userShare.Value)).Append(" of the execution CPU was user time. ");
            else
                sb.Append(' ');
            AppendTopStatement(sb, burn);
        }
        sb.Append(" How many cores this server has is not known from this reading — ").Append(Cores(KnobMeta(fact, "peak_cores_busy") ?? 0))
          .Append(" busy may be a fraction of a large host or all of a small one; the host's own metrics answer that.");

        return block with { Investigation = block.Investigation + sb.ToString().TrimEnd() };
    }

    private static string Cores(double cores) =>
        cores.ToString("0.##", CultureInfo.InvariantCulture) + (cores == 1 ? " core" : " cores");

    private static string QueryId(double id) => id.ToString("0", CultureInfo.InvariantCulture);
}
