/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Globalization;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Advice for the memory-composition family (design §4b; filled by lane 32 of #3691). Value-stated from the facts —
/// each knob's value as read (<c>shared_buffers</c>, <c>max_connections</c>, <c>work_mem</c>,
/// <c>max_parallel_workers_per_gather</c>, <c>maintenance_work_mem</c> / <c>autovacuum_work_mem</c>,
/// <c>autovacuum_max_workers</c>, <c>wal_buffers</c>), each term's product, the sum, and the host's
/// <c>memory_total_bytes</c> it is measured against (the window's minimum, with the Serverless range when the instance
/// scaled); the reclaimable share when the pressure fact fired — with the counter-objective named on every
/// recommendation (a smaller <c>work_mem</c> spills sorts to disk; fewer connections queue the application; a smaller
/// <c>shared_buffers</c> loses cache hits to the OS). The advice says that <c>work_mem</c> is per sort/hash node, not per
/// connection, so the product is a CEILING the workload may never approach — which is why the card is advisory at 0.4
/// until a workload co-fire lifts it (D5) — and that raising <c>work_mem</c> globally is the classic overcommit path.
/// Never touches <c>fsync</c> / <c>synchronous_commit</c> / <c>full_page_writes</c> (posture is the posture family's
/// alone). No DDL (D8). The static shape claims no figure.
/// </summary>
public static partial class PgTargetAdvice
{
    private static readonly AdviceBlock s_memoryOvercommitStatic = new(
        Headline: "The configured memory worst case exceeds this host's memory",
        Investigation:
            "PostgreSQL's per-backend allocation model, summed from the latest pg_server_config snapshot: shared_buffers " +
            "once, plus max_connections × work_mem × (1 + max_parallel_workers_per_gather) — every backend may run one sort " +
            "or hash at work_mem and each parallel worker another — plus autovacuum_max_workers × maintenance_work_mem " +
            "(autovacuum_work_mem when set), plus wal_buffers; measured against the host's memory_total_bytes from " +
            "pg_cpu_utilization (AWS Performance Insights, Aurora only — a stock target has no host-memory source and " +
            "this check reads unavailable there). work_mem is a per-operation budget, not a per-connection allocation, so " +
            "the sum is a CEILING the workload may never approach: at or past 1.0× the configuration CAN exceed physical " +
            "memory if every backend spills once. That is arithmetic (engine-defined), which is why the card is advisory " +
            "alone and reaches the incident line only when a workload co-fire says it is being felt — the host's " +
            "reclaimable memory measurably short (PG_HOST_MEMORY_PRESSURE) or sorts already spilling (PG_TEMP_SPILL). " +
            "get_analysis_facts source=pg_memory carries every term in bytes.",
        Remediation:
            "Decide which term to shrink, and name what it costs. Raising work_mem globally is the classic overcommit path: " +
            "it multiplies by max_connections and by parallel workers, so set it per role or per session for the sorts " +
            "that need it and keep the global value modest — the counter-objective is that a smaller global work_mem spills " +
            "more sorts and hashes to temp files (pg_temp). Fewer max_connections behind a pooler shrinks the same term " +
            "directly — the counter-objective is that arrivals above the pool queue in the application. A smaller " +
            "shared_buffers shrinks the fixed term — the counter-objective is cache hits handed to the OS page cache. On a " +
            "Serverless instance size the sum to the MINIMUM capacity it scales down to, because that is the box the " +
            "configuration must fit when the instance is smallest.");

    private static readonly AdviceBlock s_hostMemoryPressureStatic = new(
        Headline: "The host's reclaimable memory is measurably short (Aurora)",
        Investigation:
            "The OS's reclaimable share — (memory_free_bytes + memory_cached_bytes) / memory_total_bytes from " +
            "pg_cpu_utilization's row (AWS Performance Insights os.memory.*, collected for Aurora targets only), read " +
            "minute by minute over the window and graded at its worst SUSTAINED point: the lowest share the host held for " +
            "three consecutive five-minute samples, so a single dip while a maintenance job ran never pages. The 10 % / 3 % " +
            "bars are chosen, not measured — the fleet calibration ran before the memory columns existed (threshold_lineage " +
            "= 0). On Aurora the storage tier makes the OS page cache matter less than on stock PostgreSQL, but this fact is " +
            "about the INSTANCE's memory, which is what shared_buffers and every work_mem allocation come out of.",
        Remediation:
            "Read CONFIG_PG_MEMORY_OVERCOMMIT beside this: if the configured worst case exceeds the host, the shortage was " +
            "predicted and the terms to shrink are named there. If it does not, the memory is going to something the " +
            "configuration did not budget — a long-running statement holding a large hash (get_pg_top_queries), or " +
            "maintenance running with many autovacuum workers at once. A larger instance class or a higher minimum " +
            "Serverless capacity buys memory at cost and leaves the allocation that needs it in place.");

    /* filled by lane 32 */
    private static partial AdviceBlock? ComposeMemory(string key, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        switch (key)
        {
            case PgTargetFactKeys.ConfigMemoryOvercommit:
                return factsByKey.TryGetValue(key, out var sum) ? ComposeOvercommit(sum, factsByKey) : s_memoryOvercommitStatic;
            case PgTargetFactKeys.HostMemoryPressure:
                return factsByKey.TryGetValue(key, out var pressure) ? ComposeHostMemoryPressure(pressure, factsByKey) : s_hostMemoryPressureStatic;
            default:
                return null;
        }
    }

    private static AdviceBlock ComposeOvercommit(Fact sum, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var ratio = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryOvercommitRatioKey, sum.Value);
        var worst = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryWorstCaseBytesKey);
        var totalMin = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryTotalMinBytesKey);
        var totalMax = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryTotalMaxBytesKey, totalMin);
        var serverless = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryIsServerlessKey) > 0;
        var exceeds = sum.BaseSeverity > 0;
        var critical = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryOvercommitCriticalBandKey) > 0;

        var pressureFired = factsByKey.TryGetValue(PgTargetFactKeys.HostMemoryPressure, out var pressure) && pressure.BaseSeverity > 0;
        var spillFired = factsByKey.TryGetValue(PgTargetFactKeys.TempSpill, out var spill) && spill.BaseSeverity > 0;

        var hostClause = serverless
            ? $"the window's smallest memory_total_bytes, {KnobBytes(totalMin)} (the instance scaled between {KnobBytes(totalMin)} and {KnobBytes(totalMax)}; a Serverless instance is smallest, and an overcommitted configuration most dangerous, at the low end of its range, so the sum is measured against the minimum)"
            : $"the host's memory_total_bytes, {KnobBytes(totalMin)}";

        var headlineHost = serverless
            ? $"the smallest memory this instance ran with ({KnobBytes(totalMin)})"
            : $"the host's {KnobBytes(totalMin)}";
        var headline = exceeds
            ? $"The configured memory worst case is {KnobBytes(worst)} — {Ratio(ratio)} {headlineHost}" + (critical ? " — more than double" : string.Empty)
            : $"The configured memory worst case is {KnobBytes(worst)} — {Ratio(ratio)} {headlineHost}, within the box";

        var coFire = exceeds
            ? (pressureFired, spillFired) switch
            {
                (true, true) => " Both workload co-fires are present — the host's reclaimable memory is measurably short AND sorts are spilling past work_mem — so the arithmetic is being felt and the card is past the incident line (D5).",
                (true, false) => " PG_HOST_MEMORY_PRESSURE co-fires — the host's reclaimable memory is measurably short — so the arithmetic is being felt and the card is past the incident line (D5).",
                (false, true) => " PG_TEMP_SPILL co-fires — sorts and hashes are already spilling past work_mem, so the per-backend term is real — and the card is past the incident line (D5).",
                _ => " No workload co-fire in the window: the host's reclaimable memory was not measurably short and sorts were not spilling, so this stands as an advisory (0.4) — the sum is a ceiling the workload has not approached (D5).",
            }
            : string.Empty;

        return s_memoryOvercommitStatic with
        {
            Headline = headline,
            Investigation =
                $"{TermsSentence(sum)} The sum, {KnobBytes(worst)}, is {Ratio(ratio)} of {hostClause}." +
                (exceeds
                    ? " At or past 1.0× the configuration CAN exceed physical memory if every backend spills once — PostgreSQL's own per-backend allocation model, arithmetic rather than a judgment (engine-defined)."
                    : " Under 1.0× the model fits the box and this fact is context.")
                + (critical ? " The 2× band is a chosen line, not a measured one (threshold_lineage = 0)." : string.Empty)
                + " work_mem is a per-sort / per-hash budget, not a per-connection allocation, so the product is a CEILING the workload may never approach."
                + coFire
                + EffectiveCacheSentence(sum, totalMin)
                + SamplesSentence(sum),
            Remediation = RemediationFor(sum, serverless, pressureFired, spillFired),
        };
    }

    private static string TermsSentence(Fact sum)
    {
        var sb = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemorySharedBuffersBytesKey);
        var wm = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryWorkMemBytesKey);
        var mc = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryMaxConnectionsKey);
        var parallel = sum.Metadata.TryGetValue(PgTargetScorer.MemoryParallelWorkersPerGatherKey, out var p) ? p : (double?)null;
        var backend = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryBackendTermBytesKey);
        var autovacuum = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryAutovacuumTermBytesKey);
        var workers = sum.Metadata.TryGetValue(PgTargetScorer.MemoryAutovacuumMaxWorkersKey, out var w) ? w : (double?)null;
        var perWorker = sum.Metadata.TryGetValue(PgTargetScorer.MemoryAutovacuumWorkMemBytesKey, out var avwm)
            ? (Name: "autovacuum_work_mem", Bytes: avwm)
            : sum.Metadata.TryGetValue(PgTargetScorer.MemoryMaintWorkMemBytesKey, out var mwm) ? (Name: "maintenance_work_mem", Bytes: mwm) : ((string Name, double Bytes)?)null;
        var wal = sum.Metadata.TryGetValue(PgTargetScorer.MemoryWalBuffersBytesKey, out var wb) ? wb : (double?)null;

        var parallelClause = parallel is { } par
            ? $" × (1 + max_parallel_workers_per_gather {Num(par)})"
            : " (max_parallel_workers_per_gather was not in the snapshot; multiplied by 1)";
        var autovacuumClause = workers is { } wk && perWorker is { } pw
            ? $"; autovacuum_max_workers {Num(wk)} × {pw.Name} {KnobBytes(pw.Bytes)} = {KnobBytes(autovacuum)}"
            : "; the autovacuum term could not be formed from the snapshot and contributes 0";
        var walClause = wal is { } wbv ? $"; wal_buffers {KnobBytes(wbv)}" : "; wal_buffers was not in the snapshot and contributes 0";

        return $"From the latest pg_server_config snapshot: shared_buffers {KnobBytes(sb)}; max_connections {Num(mc)} × work_mem {KnobBytes(wm)}{parallelClause} = {KnobBytes(backend)}{autovacuumClause}{walClause}.";
    }

    private static string EffectiveCacheSentence(Fact sum, double totalMin)
    {
        if (!sum.Metadata.TryGetValue(PgTargetScorer.MemoryEffectiveCacheSizeBytesKey, out var ecs)) return string.Empty;
        return sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryEffectiveCacheExceedsTotalKey) > 0
            ? $" effective_cache_size is {KnobBytes(ecs)} on a host reporting {KnobBytes(totalMin)} — a planner assumption larger than the machine is a lie to the planner (engine-defined; stated here, not graded)."
            : $" effective_cache_size is {KnobBytes(ecs)}, within the host's {KnobBytes(totalMin)} — plausible.";
    }

    private static string SamplesSentence(Fact sum)
    {
        var samples = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemorySamplesKey);
        var withMemory = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemorySamplesWithMemoryKey);
        var age = sum.Metadata.TryGetValue(PgTargetScorer.MemorySnapshotAgeSecondsKey, out var a) ? a : (double?)null;
        var ageClause = age is { } s ? $"; the config snapshot is {Num(System.Math.Round(s / 60))} min older than the window's end" : string.Empty;
        return $" ({Num(withMemory)} of {Num(samples)} pg_cpu_utilization samples in the window carried memory columns{ageClause}.)";
    }

    private static string RemediationFor(Fact sum, bool serverless, bool pressureFired, bool spillFired)
    {
        var wm = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryWorkMemBytesKey);
        var mc = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemoryMaxConnectionsKey);
        var sb = sum.Metadata.GetValueOrDefault(PgTargetScorer.MemorySharedBuffersBytesKey);
        var spillCaveat = spillFired
            ? " Sorts are already spilling in this window, so lowering the global work_mem trades a known overcommit for more temp-file writes; the per-role or per-session shape is the one that resolves both."
            : string.Empty;
        var pressureCaveat = pressureFired
            ? " The host is measurably short now, so the order is: shrink the term that is actually allocating (work_mem per session, or connections behind a pooler) before anything that costs cache hits."
            : string.Empty;
        var serverlessClause = serverless
            ? " On this Serverless instance size the sum to the MINIMUM capacity it scales down to — the box the configuration must fit when the instance is smallest — or raise the minimum capacity, at cost for every hour it is provisioned."
            : string.Empty;

        return $"Raising work_mem globally is the classic overcommit path: at {KnobBytes(wm)} it multiplies by max_connections {Num(mc)} and by parallel workers. " +
               "Set it per role or per session for the sorts that need it and keep the global value modest — the counter-objective is that a smaller global work_mem spills more sorts and hashes to temp files (pg_temp). " +
               $"Fewer max_connections behind a pooler shrinks the same term directly — the counter-objective is that arrivals above the pool queue in the application. " +
               $"A smaller shared_buffers ({KnobBytes(sb)} now) shrinks the fixed term — the counter-objective is cache hits handed to the OS page cache." +
               spillCaveat + pressureCaveat + serverlessClause;
    }

    private static AdviceBlock ComposeHostMemoryPressure(Fact pressure, IReadOnlyDictionary<string, Fact> factsByKey)
    {
        var samples = pressure.Metadata.GetValueOrDefault(PgTargetScorer.MemorySamplesKey);
        var withMemory = pressure.Metadata.GetValueOrDefault(PgTargetScorer.MemorySamplesWithMemoryKey);

        if (pressure.Metadata.GetValueOrDefault("unavailable") > 0)
        {
            var noSource = pressure.Metadata.GetValueOrDefault(PgTargetScorer.HostMemoryReasonNoSourceKey) > 0;
            var sumClause = pressure.Metadata.TryGetValue(PgTargetScorer.MemoryWorstCaseBytesKey, out var worst)
                ? $" The configured worst case from pg_server_config sums to {KnobBytes(worst)} (every term is on this fact's metadata), and there is no host memory to compare it against, so no composition verdict is given."
                : string.Empty;
            return s_hostMemoryPressureStatic with
            {
                Headline = noSource
                    ? "Host memory is not collected for this target — no pressure reading, no composition check"
                    : "Host memory columns are sparse in this window — no pressure reading, no composition check",
                Investigation = noSource
                    ? "pg_cpu_utilization has no row for this server in the window. The host's memory reaches the store only through AWS " +
                      "Performance Insights (Aurora), and core PostgreSQL exposes no OS memory counter in any version — so on a stock target " +
                      "this family is structurally unavailable, stated as such rather than read as \"no pressure\" or measured against a " +
                      "guessed total (the V136 rule)." + sumClause
                    : $"pg_cpu_utilization has {Num(samples)} rows for this server in the window but only {Num(withMemory)} carry memory_total_bytes — " +
                      "fewer than half. Rows written before V136 have the memory columns NULL, and a Performance Insights endpoint that does not " +
                      "publish os.memory.* leaves them NULL truthfully; either way the window is not a memory measurement, and the family is " +
                      "unavailable for it rather than graded on a minority of samples." + sumClause,
                Remediation = noSource
                    ? "Nothing to tune from this fact. The composition arithmetic still applies by hand: get_pg_server_config for shared_buffers, " +
                      "work_mem, max_connections, max_parallel_workers_per_gather, maintenance_work_mem, autovacuum_max_workers and wal_buffers, " +
                      "summed against the host's memory from wherever this instance's OS is observed."
                    : "Wait for a window whose rows post-date the memory columns, or widen hours_back past the pre-V136 rows only if the recent ones carry memory. " +
                      "If the recent rows are NULL too, Performance Insights is not publishing os.memory.* for this instance class.",
            };
        }

        var min = pressure.Metadata.GetValueOrDefault(PgTargetScorer.HostMemoryMinReclaimableShareKey, pressure.Value);
        var mean = pressure.Metadata.GetValueOrDefault(PgTargetScorer.HostMemoryMeanReclaimableShareKey);
        var sustained = pressure.Metadata.TryGetValue(PgTargetScorer.HostMemorySustainedMinReclaimableShareKey, out var s) ? s : (double?)null;
        var sustain = pressure.Metadata.GetValueOrDefault(PgTargetScorer.HostMemorySustainSamplesKey, PgTargetScorer.HostMemoryPressureSustainSamples);
        var active = pressure.Metadata.TryGetValue(PgTargetScorer.HostMemoryPeakActiveShareKey, out var a) ? a : (double?)null;
        var totalMin = pressure.Metadata.GetValueOrDefault(PgTargetScorer.MemoryTotalMinBytesKey);
        var totalMax = pressure.Metadata.GetValueOrDefault(PgTargetScorer.MemoryTotalMaxBytesKey, totalMin);
        var fired = pressure.BaseSeverity > 0;
        var overcommitFired = factsByKey.TryGetValue(PgTargetFactKeys.ConfigMemoryOvercommit, out var sum) && sum.BaseSeverity > 0;

        var sustainedClause = sustained is { } sus
            ? $" The worst share held for {Num(sustain)} consecutive samples was {Share(sus)} — the graded figure."
            : $" No run of {Num(sustain)} consecutive memory-carrying samples exists in the window, so nothing is graded: a single sample is not pressure.";
        var activeClause = active is { } act ? $" memory_active_bytes peaked at {Share(act)} of total." : string.Empty;
        var rangeClause = totalMax > totalMin ? $" (the instance scaled between {KnobBytes(totalMin)} and {KnobBytes(totalMax)})" : $" ({KnobBytes(totalMin)} total)";
        var overcommitClause = overcommitFired
            ? " CONFIG_PG_MEMORY_OVERCOMMIT fired in the same window: the configured worst case exceeds this host, and the arithmetic predicted the shortage — the terms to shrink are named on that card."
            : " CONFIG_PG_MEMORY_OVERCOMMIT did not fire: the configured worst case fits this host, so the memory is going to something the configuration did not budget.";

        return s_hostMemoryPressureStatic with
        {
            Headline = fired
                ? $"The host's reclaimable memory fell to {Share(min)} of total and stayed under {Share(PgTargetScorer.HostMemoryReclaimableWarningShare)} for {Num(sustain)} consecutive samples"
                : $"The host's reclaimable memory reached a low of {Share(min)} of total — not sustained under the {Share(PgTargetScorer.HostMemoryReclaimableWarningShare)} line",
            Investigation =
                $"(memory_free_bytes + memory_cached_bytes) / memory_total_bytes from pg_cpu_utilization{rangeClause}: minimum {Share(min)}, mean {Share(mean)} across {Num(withMemory)} of {Num(samples)} samples." +
                sustainedClause + activeClause +
                (fired
                    ? $" The {Share(PgTargetScorer.HostMemoryReclaimableWarningShare)} / {Share(PgTargetScorer.HostMemoryReclaimableCriticalShare)} bars and the {Num(sustain)}-sample sustain are chosen, not measured — the fleet calibration ran before the memory columns existed (threshold_lineage = 0)."
                    : " Under the warning line this fact is context: stated so the composition card can read it, and it roots nothing.")
                + " On Aurora the storage tier makes the OS page cache matter less than on stock, but this is the INSTANCE's memory — what shared_buffers and every work_mem allocation come out of."
                + overcommitClause,
        };
    }

    private static string Share(double fraction)
    {
        return (fraction * 100).ToString("0.#", CultureInfo.InvariantCulture) + "%";
    }

    private static string Ratio(double ratio)
    {
        return ratio.ToString("0.0#", CultureInfo.InvariantCulture) + "×";
    }

    private static string Num(double value)
    {
        return value.ToString("0.#", CultureInfo.InvariantCulture);
    }
}
