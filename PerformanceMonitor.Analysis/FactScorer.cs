using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Analysis;

/// <summary>
/// Assigns severity to facts using threshold formulas (Layer 1)
/// and contextual amplifiers (Layer 2).
///
/// Layer 1: Base severity 0.0-1.0 from thresholds alone.
/// Layer 2: Amplifiers multiply base up to 2.0 max using corroborating facts.
/// Layer 3: Tuning-class keys (parallelism, high-DOP queries, the routine ANOMALY_* case) are capped at
/// the WARNING ceiling unless an impact peer co-fired or, for an anomaly, its own deviation is extreme.
///
/// Formula: severity = min(base * (1.0 + sum(amplifiers)), 2.0)
/// </summary>
public class FactScorer
{
    /// <summary>
    /// The source registry: every <see cref="Fact.Source"/> value a collector on either SKU emits, in the
    /// spelling the facts carry (#3541 A13).
    ///
    /// <para><b>Why a registry exists at all.</b> Sources were only ever string literals — in the collectors
    /// that stamp them, in the switch below that scores them, and in the four the <c>get_analysis_facts</c>
    /// description happened to mention ("waits, blocking, config, memory") out of the fifteen that exist. A
    /// caller filtering on any of the other eleven got <c>[]</c>, which reads as "no facts of that kind" for
    /// a value that could never have matched. The MCP tools now publish THIS list as the accepted set and
    /// refuse anything outside it; the analysis-side tests pin it against every <c>Source = "..."</c> literal
    /// in the three collector assemblies AND against the switch arms below, so a new source that lands in a
    /// collector without landing here fails a test rather than becoming the sixteenth silent value.</para>
    ///
    /// <para>Sorted, and kept sorted, because the list is published verbatim in a refusal message and a
    /// description on both SKUs. <c>coverage</c> and <c>sessions</c> are emitted but not scored (they carry
    /// context, base severity 0); they are still filterable, so they are still members. <c>perfmon</c> is
    /// named in the amplifier context set below but no collector emits it, so it is NOT a member — a filter
    /// on it would always be empty, which is the outcome this registry exists to refuse.</para>
    ///
    /// <para>The fourteen <c>pg_</c> members (eleven from #3542 v1, three from #3691 v2) are the PostgreSQL-TARGET vocabulary (D2), declared in
    /// <see cref="PgTargetSources"/> and registered here the day they were declared — before any content lane
    /// emits one — so the D2 rule (a PostgreSQL fact can never wear a SQL Server source) is enforced by the same
    /// sweep that guards the rest of the list rather than by convention.</para>
    /// </summary>
    public static readonly IReadOnlyList<string> KnownSources = new[]
    {
        "anomaly", "bad_actor", "blocking", "config", "coverage", "cpu", "database_config", "disk", "io",
        "jobs", "memory",
        PgTargetSources.BloatSource, PgTargetSources.BufferSource, PgTargetSources.ConfigSource,
        PgTargetSources.CpuSource, PgTargetSources.DatabaseSource, PgTargetSources.IoSource,
        PgTargetSources.PostureSource, PgTargetSources.QueriesSource, PgTargetSources.ReplicationSource,
        PgTargetSources.SessionsSource, PgTargetSources.TempSource, PgTargetSources.VacuumSource,
        PgTargetSources.WaitsSource, PgTargetSources.WriteSource,
        "queries", "sessions", "tempdb", "waits",
    };

    /// <summary>
    /// Scores all facts: Layer 1 (base severity), then Layer 2 (amplifiers).
    /// </summary>
    public void ScoreAll(List<Fact> facts)
    {
        // Layer 1: base severity from thresholds
        foreach (var fact in facts)
        {
            fact.BaseSeverity = fact.Source switch
            {
                "waits" => ScoreWaitFact(fact),
                "blocking" => ScoreBlockingFact(fact),
                "cpu" => ScoreCpuFact(fact),
                "io" => ScoreIoFact(fact),
                "tempdb" => ScoreTempDbFact(fact),
                "memory" => ScoreMemoryFact(fact),
                "queries" => ScoreQueryFact(fact),
                "config" => ScoreConfigFact(fact),
                "database_config" => ScoreDatabaseConfigFact(fact),
                "jobs" => ScoreJobFact(fact),
                "disk" => ScoreDiskFact(fact),
                "bad_actor" => ScoreBadActorFact(fact),
                "anomaly" => ScoreAnomalyFact(fact),
                /* #3542 D2: the PostgreSQL-target vocabulary is pg_-prefixed precisely so it lands in ONE
                   arm and never in any of the SQL Server ones above; PgTargetScorer routes it per source. */
                _ when PgTargetSources.IsPgSource(fact.Source) => PgTargetScorer.ScoreBase(fact),
                _ => 0.0
            };
        }

        // Build lookup for amplifier evaluation (include context facts that amplifiers reference)
        var contextSources = new HashSet<string>
            { "config", "cpu", "io", "tempdb", "memory", "queries", "perfmon",
              "database_config", "jobs", "sessions", "disk", "bad_actor", "anomaly" };
        /* The pg_ sources join the context set for the same reason "cpu" and "sessions" are in it: a
           PG_CPU_PERCENT at 30% or a CONFIG_PG_MAX_CONNECTIONS context fact has base 0, and an amplifier
           that reads it (the load-family confirmer, the saturation ceiling) must still be able to see it. */
        contextSources.UnionWith(PgTargetSources.All);
        var factsByKey = facts
            .Where(f => f.BaseSeverity > 0 || contextSources.Contains(f.Source))
            .ToFactLookup();

        // Layer 2: amplifiers boost base severity using corroborating facts
        foreach (var fact in facts)
        {
            if (fact.BaseSeverity <= 0)
            {
                fact.Severity = 0;
                continue;
            }

            var amplifiers = GetAmplifiers(fact);
            var totalBoost = 0.0;

            foreach (var amp in amplifiers)
            {
                var matched = amp.Predicate(factsByKey);
                fact.AmplifierResults.Add(new AmplifierResult
                {
                    Description = amp.Description,
                    Matched = matched,
                    Boost = matched ? amp.Boost : 0.0
                });

                if (matched) totalBoost += amp.Boost;
            }

            fact.Severity = Math.Min(fact.BaseSeverity * (1.0 + totalBoost), 2.0);
        }

        // Layer 3: tuning-class severity cap. Parallelism (CXPACKET) saturates its base to 1.0 at 25%
        // of period and then amplifiers multiply it past 1.5 into the CRITICAL band on stacking alone —
        // but parallelism is a TUNING opportunity, not an outage. Cap the FINAL severity of the
        // tuning-class keys at the WARNING ceiling so they can't reach CRITICAL by amplifier count.
        // ESCAPE HATCH: release the cap entirely when an impact-bearing peer co-fired — THREADPOOL
        // (thread exhaustion), SOS_SCHEDULER_YIELD (CPU starvation), or RESOURCE_SEMAPHORE (grant
        // starvation) — because then the parallelism genuinely IS driving an outage and CRITICAL is
        // earned. "Co-fired" must mean SIGNIFICANT, not merely present. Only THREADPOOL's base is
        // self-gating: ScoreWaitFact requires >= 15 min per observed hour AND >= 1s avg before THREADPOOL
        // scores at all, so BaseSeverity > 0 there already means real exhaustion — keep the presence
        // check. SOS_SCHEDULER_YIELD (0.75, null) and RESOURCE_SEMAPHORE (0.01, 0.10) have NO minimum
        // guard, so their BaseSeverity > 0 fires on ANY trace of the wait; and SOS physically co-occurs with
        // high CXPACKET (parallel workers yield -> SOS) and is emitted for any delta_wait_time_ms > 0,
        // so a trivial SOS (e.g. 500ms over an hour) would release the cap on exactly the busy servers
        // the cap targets — re-admitting the CXPACKET=CRITICAL noise the cap exists to kill. Gate
        // SOS/RS on SIGNIFICANCE (fraction of period) via the same HasSignificantWait helper the
        // amplifiers use, not on mere presence: SOS at 0.25 (matches the CXPACKET SOS amplifier bar);
        // RS at 0.10 (RESOURCE_SEMAPHORE has no HasSignificantWait amplifier bar, so pick a bar here —
        // 0.10 of period is meaningful grant starvation, and since #3538 A5 it is also the wait's own
        // CRITICAL bar in GetWaitThresholds, so "significant enough to release the cap" and "saturated"
        // are one number). Caps numeric Severity only — SeverityBand is
        // derived from it downstream, so a capped fact stays in WARNING without a separate band edit
        // and Lite parity is preserved.
        var impactPeerCoFired =
            (factsByKey.TryGetValue("THREADPOOL", out var tpPeer) && tpPeer.BaseSeverity > 0)
            || HasSignificantWait(factsByKey, "SOS_SCHEDULER_YIELD", 0.25)
            || HasSignificantWait(factsByKey, "RESOURCE_SEMAPHORE", 0.10);

        // #3526: the SECOND escape, per-fact and for ANOMALY_* only — extremity. The impact-peer
        // escape above is the right shape for CXPACKET (parallelism is only an outage when a thread/CPU/
        // grant peer says so), but applied to every ANOMALY_* it made the baseline engine notification-
        // inert at shipped settings: every anomaly ramp saturates its BASE at 1.0, the cap holds the FINAL
        // at 1.49, and the notify floor is 1.5 — so a 20σ session spike beside a 15σ batch-request spike
        // at 3am produced nothing that could page unless THREADPOOL/SOS/RESOURCE_SEMAPHORE happened to be
        // in the picture too. The product's best per-server-calibrated statistics were structurally its
        // quietest. An anomaly whose deviation is EXTREME against the cutoff it fired at (IsExtremeAnomaly:
        // 3x its own fire threshold — 10.5σ on the 3.5σ robust path, 15σ on the 5.0σ heavy-tail path, 6σ
        // classical; 3x the absolute bar on the never-blind fallback path) is released from the cap on its
        // own evidence. Releasing the cap does NOT page by itself: base still maxes at 1.0, so the CRITICAL
        // band is reached only through the anomaly co-fire amplifiers (AnomalyAmplifiers) — corroboration
        // stays the house rule for >= 1.5, the escape merely stops the cap from discarding it. The
        // impact-peer escape is unchanged and still releases everything; CXPACKET / CXCONSUMER /
        // QUERY_HIGH_DOP have no extremity arm and stay capped however many anomalies co-fire beside them.
        if (!impactPeerCoFired)
        {
            foreach (var fact in facts)
            {
                if (IsTuningClassKey(fact.Key) && !IsExtremeAnomaly(fact))
                    fact.Severity = Math.Min(fact.Severity, TuningClassSeverityCeiling);
            }
        }
    }

    /// <summary>
    /// Scores a wait fact using the fraction-of-period formula.
    /// Two waits (THREADPOOL, PAGELATCH_UP) carry a minimum-wait gate expressed PER OBSERVED HOUR so the
    /// same server reads the same at every <c>hours_back</c> (#3538 A7).
    /// </summary>
    private static double ScoreWaitFact(Fact fact)
    {
        var fraction = fact.Value;
        if (fraction <= 0) return 0.0;

        // THREADPOOL: require both meaningful total wait time AND meaningful average.
        // Tiny amounts are normal thread pool grow/shrink housekeeping, not exhaustion. The total is
        // judged per observed hour (ThreadpoolMinWaitMsPerObservedHour) — see the constant for why
        // the pre-#3538 absolute 1 h bar meant "a quarter of the window" at 4 h, "the whole window" at
        // 1 h and "0.6% of the window" at 168 h.
        if (fact.Key == "THREADPOOL")
        {
            var waitTimeMs = fact.Metadata.GetValueOrDefault("wait_time_ms");
            var avgMs = fact.Metadata.GetValueOrDefault("avg_ms_per_wait");
            if (waitTimeMs / ObservedHours(fact) < ThreadpoolMinWaitMsPerObservedHour
                || avgMs < ThreadpoolMinAvgMsPerWait) return 0.0;
        }

        // PAGELATCH_UP (tempdb allocation contention) is scored on wait_time_ms PER OBSERVED HOUR, not
        // fraction-of-period, because its source — the Dashboard's report.tempdb_contention_analysis
        // contention_level CASE — trips on a PAGELATCH_UP total over the last hour (install/47:2411 reads
        // collect.wait_stats WHERE collection_time >= DATEADD(HOUR, -1, ...); :2515: pagelatch_up_ms >
        // 10000 -> "MEDIUM - PAGELATCH_UP contention"). PAGELATCH_UP is the canonical PFS/GAM/SGAM
        // allocation-page latch (the fix is add tempdb data files / TF 1118), and the source reads the
        // SAME server-wide wait_stats this fact is built from, so scoring the hourly wait total is a
        // faithful port — the pre-#3538 form applied the source's one-hour bar to the WHOLE analysis
        // window and so was 4x more sensitive than its source at the default 4 h and 168x at a week
        // (PagelatchUpMinWaitMsPerObservedHour has the measurement). Flat 0.5 (MEDIUM) at the source's
        // single PAGELATCH_UP tier — there is no higher band for it there; the view's CRITICAL
        // "allocation contention" comes from a tempdb-scoped dm_os_waiting_tasks flag
        // (allocation_contention_warning, install/47:2503) that is NOT carried in this fact.
        if (fact.Key == "PAGELATCH_UP")
            return fact.Metadata.GetValueOrDefault("wait_time_ms") / ObservedHours(fact) > PagelatchUpMinWaitMsPerObservedHour
                ? 0.5 : 0.0;

        var thresholds = GetWaitThresholds(fact.Key);
        if (thresholds == null) return 0.0;

        return ApplyThresholdFormula(fraction, thresholds.Value.concerning, thresholds.Value.critical);
    }

    /// <summary>
    /// The hours the collector actually observed inside the window a wait fact was summed over — the
    /// divisor for the per-hour gates above (#3538 A7). Read from the fact's own metadata rather than
    /// from <see cref="AnalysisContext"/>, which the scorer never sees: every wait fact carries
    /// <c>period_duration_ms</c> (the nominal window) and, from a collector that stamps coverage (#3538
    /// A2), <c>coverage_fraction</c>; their product is the observed time (see
    /// <see cref="FactCollectorHelpers.AddCoverageFraction"/> for why the divisor travels under that
    /// name). A collector that stamps no coverage — the frozen Dashboard twin — divides by its nominal
    /// window, which is exactly what its own fractions do.
    ///
    /// <para>A fact with no <c>period_duration_ms</c> at all is read as a ONE-HOUR window. Every collector
    /// stamps the period, so only a hand-built fact lacks it; reading it as one hour makes the per-hour
    /// bars read as plain totals (10 s of PAGELATCH_UP, 15 min of THREADPOOL), which is the one reading a
    /// fact with no window can honestly have.</para>
    ///
    /// <para>A <c>coverage_fraction</c> of exactly 0 (what <c>WindowCoverage.Unobserved</c> stamps) falls back
    /// to the NOMINAL window rather than to a near-zero divisor. That branch defends the arithmetic against
    /// 0/0 and should be moot: both collectors emit no wait fact at all when <c>ObservedDurationMs &lt;= 0</c>,
    /// so a fact carrying zero coverage and a non-zero wait total is a fact no shipped collector produces. If
    /// one ever did, dividing by ~0 would turn any trace into an enormous hourly rate and fire on the
    /// artifact; the nominal reading is the pre-#3538 form — the least-sensitive honest one — and is pinned
    /// so the choice cannot silently flip.</para>
    /// </summary>
    private static double ObservedHours(Fact fact)
    {
        var periodMs = fact.Metadata.GetValueOrDefault("period_duration_ms");
        if (periodMs <= 0) return 1.0;
        var coverage = fact.Metadata.GetValueOrDefault("coverage_fraction", 1.0);
        var observedMs = coverage > 0 ? periodMs * coverage : periodMs;
        return observedMs / 3_600_000.0;
    }

    /// <summary>
    /// Scores blocking/deadlock facts using events-per-hour thresholds.
    /// </summary>
    private static double ScoreBlockingFact(Fact fact)
    {
        var value = fact.Value; // events per hour
        if (value <= 0) return 0.0;

        return fact.Key switch
        {
            // Blocking: concerning >10/hr, critical >50/hr. Inherited, not measured here. The alerting
            // layer's blocking band (ServerHealthThresholds.BlockingWarnPerHour / BlockingCriticalPerHour
            // in PerformanceMonitor.Common/ServerHealthBands.cs, #3539 A3) measured 14 days of
            // blocked_process_reports on the same 43-server fleet and placed its WARNING at 5/hr and
            // CRITICAL at 20/hr; this pair sits 2-2.5x above it and was deliberately NOT moved in the
            // #3538 A5 pass — the scorer's blocking story reaches CRITICAL through its amplifiers
            // (sleeping head blocker, lock waits, deadlocks) rather than on the count alone, so the two ladders
            // are not the same instrument and aligning them is a decision on its own evidence.
            "BLOCKING_EVENTS" => ApplyThresholdFormula(value, 10, 50),
            // Deadlocks: 5/hr concerning, 20/hr critical — the SAME pair the alerting layer derived from
            // 14 days of collect.deadlocks on the 43-server dogfood fleet (2,722 deadlocks over 14,448
            // server-hours; ServerHealthThresholds.DeadlockWarnPerHourDefault /
            // DeadlockCriticalPerHourDefault in PerformanceMonitor.Common/ServerHealthBands.cs, #3368):
            // 5/hr is the 99.94th percentile of server-hours (99.1% hold at most two), and 20/hr sits
            // inside the measured empty interval [16, 89] between the routine mode (which tops out at 15)
            // and the storms (90, 102). The pre-#3538 pair was (5, null) — base saturated at 1.0 the moment
            // the WARNING bar was reached, so 5/hr and 90/hr scored identically and the CRITICAL band was
            // reachable only through amplifiers. Now 5/hr roots a story at 0.5 and the storm mode
            // saturates at 1.0. The literals are repeated rather than bound because this assembly does
            // not reference PerformanceMonitor.Common; FactScorerTests pins the two pairs equal.
            "DEADLOCKS" => ApplyThresholdFormula(value, 5, 20),
            // Blocking chain: scored by structural magnitude. Value = worst-chain depth >= 1
            // for any emitted chain, so the value<=0 guard above never trips this arm.
            "BLOCKING_CHAIN" => ScoreBlockingChain(fact),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores a BLOCKING_CHAIN fact by structural magnitude — the worse of chain depth
    /// and transitive victim count. Max, not average, so one severe dimension scores high
    /// without being diluted by the other.
    /// </summary>
    private static double ScoreBlockingChain(Fact fact)
    {
        var depth = fact.Metadata.GetValueOrDefault("worst_chain_depth");
        var victims = fact.Metadata.GetValueOrDefault("worst_chain_victim_count");
        return Math.Max(
            ApplyThresholdFormula(depth, 3, 8),
            ApplyThresholdFormula(victims, 5, 25));
    }

    /// <summary>
    /// Scores CPU utilization. Value is average SQL CPU %.
    /// </summary>
    private static double ScoreCpuFact(Fact fact)
    {
        return fact.Key switch
        {
            // CPU %: concerning at 75%, critical at 95%
            "CPU_SQL_PERCENT" => ApplyThresholdFormula(fact.Value, 75, 95),
            // CPU spike: value is max CPU %. Concerning at 80%, critical at 95%.
            // Only emitted when max is significantly above average (bursty).
            "CPU_SPIKE" => ApplyThresholdFormula(fact.Value, 80, 95),
            // Runnable-task queue depth — a STANDALONE scheduler-pressure signal that roots the collected
            // cpu_scheduler_stats snapshot directly. Distinct from (and additive to) the #1494 THREADPOOL
            // runnable-queue amplifier, which still fires independently off the same RUNNABLE_TASKS fact.
            "RUNNABLE_TASKS" => ScoreRunnableTasks(fact),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores the runnable-task-queue pressure signal (RUNNABLE_TASKS context fact; Value =
    /// total_runnable_tasks_count from the latest cpu_scheduler_stats snapshot). Tiers mirror the
    /// Dashboard's report.cpu_scheduler_pressure pressure_level CASE
    /// (install/47_create_reporting_views.sql lines 1839-1844): > 50 CRITICAL, > 20 HIGH, > 10 MEDIUM,
    /// else the collector's own runnable_tasks_warning flag (SUM(runnable_tasks_count) >= cpu_count) as a
    /// small-box HIGH fallback the absolute > 10 bar misses. Base maxes at 1.0 (WARNING band) exactly as
    /// every other base fact does — the CRITICAL band (>= 1.5) is reached only with corroboration, which
    /// is precisely the runnable-queue -> THREADPOOL amplifier path (#1494). A bare runnable queue with no
    /// thread/CPU corroboration is a strong WARNING, not an outage.
    /// </summary>
    private static double ScoreRunnableTasks(Fact fact)
    {
        var total = fact.Value; // total_runnable_tasks_count (latest snapshot)
        if (total > 50) return 1.0;   // CRITICAL - High runnable task queue (install/47:1839)
        if (total > 20) return 0.75;  // HIGH - Moderate runnable task queue (install/47:1840)
        if (total > 10) return 0.5;   // MEDIUM - Some runnable tasks queued (install/47:1841)
        // Small-box per-scheduler pressure below the absolute bar (install/47:1844: runnable_tasks_warning).
        if (fact.Metadata.GetValueOrDefault("runnable_tasks_warning") >= 1.0) return 0.75;
        return 0.0;
    }

    /// <summary>
    /// Scores I/O latency facts. Value is average latency in ms.
    /// </summary>
    private static double ScoreIoFact(Fact fact)
    {
        return fact.Key switch
        {
            // Read latency: concerning at 20ms, critical at 50ms
            "IO_READ_LATENCY_MS" => ApplyThresholdFormula(fact.Value, 20, 50),
            // Write latency: concerning at 10ms, critical at 30ms
            "IO_WRITE_LATENCY_MS" => ApplyThresholdFormula(fact.Value, 10, 30),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores TempDB usage. Value is usage fraction — reserved ÷ tempdb's growth CEILING where it has one,
    /// and ÷ the current allocation where it does not (#2515).
    /// </summary>
    private static double ScoreTempDbFact(Fact fact)
    {
        return fact.Key switch
        {
            // TempDB usage scores the WORSE of two INDEPENDENT pressures: space-fraction fill (concerning
            // 75%, critical 90%) and absolute version-store size (ScoreTempDbVersionStore) — a multi-GB
            // version store is a problem even when total tempdb space is nowhere near full, and the
            // fraction arm is blind to it.
            "TEMPDB_USAGE" => Math.Max(
                ApplyThresholdFormula(fact.Value, 0.75, 0.90),
                ScoreTempDbVersionStore(fact)),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores tempdb VERSION-STORE pressure by ABSOLUTE reserved size (max_version_store_mb, carried in
    /// the TEMPDB_USAGE fact metadata by every collector), independent of the space-fraction the main arm
    /// scores. The version store grows with long-running RCSI/snapshot transactions (and heavy triggers)
    /// that pin old row versions, so it can reach gigabytes while total tempdb space is barely used —
    /// space-fraction alone misses it. Tiers mirror the Dashboard's report.tempdb_pressure pressure_level
    /// CASE (install/47_create_reporting_views.sql lines 1431-1433): > 5000 MB CRITICAL, > 2000 MB HIGH,
    /// > 1000 MB MEDIUM. Base maxes at 1.0 (WARNING) like every base fact — the > 5000 "CRITICAL" tier
    /// caps at 1.0 here; the CRITICAL band is earned only via corroboration. tempdb_contention_analysis
    /// corroborates the > 1 GB bar (version_store_high_warning fires at 1 GB — install/47:2504,
    /// install/34:146). Absent metadata (older facts) scores 0, preserving prior behavior.
    ///
    /// <para>#2515 moved the FRACTION arm's denominator to tempdb's growth ceiling and deliberately left these
    /// bars alone. They are absolute MB and the ceiling is a denominator, so nothing about their reachability
    /// moved: they are unreachable on an Azure SQL Database tempdb still at its initial ~62 MB for the reason
    /// #2516 gave — a version store cannot exceed 1 GB inside a 62 MB tempdb — and they become reachable there
    /// exactly when tempdb autogrows past a gigabyte, which is the same rule as everywhere else. The arm stays
    /// self-consistent and needs no change; the fraction arm is what covers RCSI (on by default on Azure SQL
    /// Database) until tempdb has grown that far.</para>
    /// </summary>
    private static double ScoreTempDbVersionStore(Fact fact)
    {
        var versionStoreMb = fact.Metadata.GetValueOrDefault("max_version_store_mb");
        if (versionStoreMb > 5000) return 1.0;   // CRITICAL - Version store > 5GB (install/47:1431)
        if (versionStoreMb > 2000) return 0.75;  // HIGH - Version store > 2GB     (install/47:1432)
        if (versionStoreMb > 1000) return 0.5;   // MEDIUM - Version store > 1GB   (install/47:1433)
        return 0.0;
    }

    /// <summary>
    /// Scores memory facts: grant waiters (MEMORY_GRANT_PENDING), security-cache growth (MEMORY_CLERKS),
    /// plan-cache single-use bloat (PLAN_CACHE_BLOAT), and ring-buffer physical-memory-pressure
    /// notifications (MEMORY_PRESSURE_EVENTS).
    /// </summary>
    private static double ScoreMemoryFact(Fact fact)
    {
        return fact.Key switch
        {
            // Grant waiters: concerning at 1, critical at 5
            "MEMORY_GRANT_PENDING" => ApplyThresholdFormula(fact.Value, 1, 5),
            // Security cache (TokenAndPermUserStore) growth — WARNING at >= 1 GB. See ScoreSecurityCache.
            "MEMORY_CLERKS" => ScoreSecurityCache(fact),
            // Plan-cache single-use bloat — % single-use plans, size-guarded. See ScorePlanCacheBloat.
            "PLAN_CACHE_BLOAT" => ScorePlanCacheBloat(fact),
            // Ring-buffer physical-memory-pressure notifications — max indicator band. See
            // ScoreMemoryPressureEvents.
            "MEMORY_PRESSURE_EVENTS" => ScoreMemoryPressureEvents(fact),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores plan-cache single-use bloat off the PLAN_CACHE_BLOAT fact (Value = single_use_percent =
    /// single_use_plans * 100 / total_plans over the LATEST plan_cache_stats snapshot). Tiers mirror the
    /// Dashboard's report.plan_cache_bloat bloat_level CASE
    /// (install/47_create_reporting_views.sql lines 1485-1487): &gt; 50 CRITICAL, &gt; 30 HIGH, &gt; 20
    /// MEDIUM, else NORMAL. Base maxes at 1.0 (WARNING) like every base fact — the &gt; 50 "CRITICAL"
    /// tier caps at 1.0 here; the CRITICAL band is earned only via corroboration.
    ///
    /// <para>NOISE-CONTROL GUARD (not in the Dashboard's raw report view, appropriate for a SCORED
    /// recommendation): only score when the single-use footprint is materially large
    /// (single_use_size_mb &gt;= 100). A tiny or idle cache can show a high single-use % on a handful of
    /// MB — that is not memory bloat worth a card, so it stays context-only (score 0) below the size
    /// floor. The percentage still rides in Value for the AI surface either way.</para>
    /// </summary>
    private static double ScorePlanCacheBloat(Fact fact)
    {
        // Real memory bloat only — a high % on a trivially small single-use footprint is noise.
        if (fact.Metadata.GetValueOrDefault("single_use_size_mb") < 100.0) return 0.0;

        var singleUsePercent = fact.Value; // single_use_plans * 100 / total_plans (latest snapshot)
        if (singleUsePercent > 50) return 1.0;   // CRITICAL - single-use plans > 50% (install/47:1485)
        if (singleUsePercent > 30) return 0.75;  // HIGH     - single-use plans > 30% (install/47:1486)
        if (singleUsePercent > 20) return 0.5;   // MEDIUM   - single-use plans > 20% (install/47:1487)
        return 0.0;
    }

    /// <summary>
    /// Scores ring-buffer physical-memory-pressure notifications off the MEMORY_PRESSURE_EVENTS fact
    /// (Value = the max of memory_indicators_process / memory_indicators_system over the analysis
    /// window; the collector emits it only when a genuine MEDIUM+ indicator is present). Bands mirror the
    /// Dashboard's report.memory_pressure_events severity CASE
    /// (install/47_create_reporting_views.sql lines 229-236), which keys severity purely off the
    /// indicators: process/system &gt;= 3 → HIGH, &gt;= 2 → MEDIUM, else LOW. A real
    /// RESOURCE_MEMPHYSICAL_LOW is a genuine memory-pressure event, so HIGH earns the WARNING band (0.9);
    /// MEDIUM is a softer 0.5. These are incident-ish facts (a real event, not a standing config), so
    /// 0.5+ roots via the InferenceEngine's incident threshold — no ConfigAdvisoryRootKey. The LOW floor
    /// scores 0 as a defensive backstop (the collector already gates it out).
    /// </summary>
    private static double ScoreMemoryPressureEvents(Fact fact)
    {
        var maxIndicator = fact.Value; // max(process, system) memory-pressure indicator in the window
        if (maxIndicator >= 3) return 0.9;  // HIGH   - indicator >= 3 (install/47:230-231)
        if (maxIndicator >= 2) return 0.5;  // MEDIUM - indicator >= 2 (install/47:232-233)
        return 0.0;                         // LOW    - not a scored concern (install/47:234)
    }

    /// <summary>
    /// Scores TokenAndPermUserStore (security cache) growth off the otherwise context-only MEMORY_CLERKS
    /// fact. That fact carries each top-clerk's size in MB keyed by its clerk_type (MemoryClerksCollector
    /// stores clerk_type = sys.dm_os_memory_clerks.type), so the security cache is the USERSTORE_TOKENPERM
    /// entry. The Dashboard fires a single WARNING at >= 1 GB with no size escalation
    /// (install/50_configuration_issues_analyzer.sql line 562 severity=WARNING, line 583 threshold
    /// pages_kb / 1024 / 1024 >= 1.0), so this is a flat WARNING-band base (0.9). Absent when the clerk is
    /// not among the top-10 collected, which for a >= 1 GB clerk is effectively never. Non-security clerk
    /// sets (buffer pool, etc.) score 0, preserving MEMORY_CLERKS as context-only for those.
    /// </summary>
    private static double ScoreSecurityCache(Fact fact)
    {
        var securityCacheMb = fact.Metadata.GetValueOrDefault("USERSTORE_TOKENPERM");
        return securityCacheMb >= 1024.0 ? 0.9 : 0.0; // >= 1 GB -> flat WARNING (install/50:562,583)
    }

    /// <summary>
    /// Scores query-level aggregate facts.
    /// </summary>
    private static double ScoreQueryFact(Fact fact)
    {
        return fact.Key switch
        {
            // Spills: concerning at 100, critical at 1000 in the period
            "QUERY_SPILLS" => ApplyThresholdFormula(fact.Value, 100, 1000),
            // High DOP queries: concerning at 5, critical at 20 in the period
            "QUERY_HIGH_DOP" => ApplyThresholdFormula(fact.Value, 5, 20),
            // Parameter sensitivity: worst max/min worker-time ratio. Magnitude-driven —
            // concerning at 10x, critical at 100x — so a lone catastrophic plan still scores high.
            "PARAMETER_SENSITIVITY" => ApplyThresholdFormula(fact.Value, 10, 100),
            // Plan regression: worst per-exec cost factor vs the best plan. Concerning 2x, critical 10x.
            "PLAN_REGRESSION" => ApplyThresholdFormula(fact.Value, 2, 10),
            // WS4: plan-XML advisories (advise-only), parsed from the top collected query plans.
            // Each scores its 0.4 advisory base only when >=1 was found (Value = count) and roots a
            // standalone card via InferenceEngine.ConfigAdvisoryRootKeys. The specific suggested
            // indexes / warning detail ride in the finding drill-down (Fact metadata is numeric only).
            "MISSING_INDEX" => fact.Value > 0 ? 0.4 : 0.0,
            "PLAN_WARNING" => fact.Value > 0 ? 0.4 : 0.0,
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores config-source advisory facts. FILE_AUTOGROWTH_PERCENT (WS3) is a base-0.3
    /// advisory; the four server-level config keys (WS3) and the three server-health keys (WS5:
    /// CONFIG_IFI_DISABLED / CONFIG_LPIM_DISABLED / SERVER_MEMORY_DUMPS) score 0.4 ONLY when the
    /// value is bad, and 0 otherwise — so audit_config still sees every CONFIG_* fact (it reads the
    /// raw value), but only a BAD one roots a recommendation card (via
    /// InferenceEngine.ConfigAdvisoryRootKeys). The WS5 keys are advise-only: there is no Apply,
    /// only advice prose and copy-paste guidance.
    /// Edition is NOT needed to decide "bad" — only later for the recommended MAXDOP value.
    /// Every other "config"-source fact (SERVER_* / DATABASE_TOTAL_SIZE_MB / SERVER_HARDWARE /
    /// CONFIG_MAX_WORKER_THREADS / CONFIG_MIN_MEMORY_MB) is a leaf/amplifier with no base severity
    /// of its own and scores 0 here, exactly as before (it contributes only via amplifiers / the
    /// audit tool / the narrow-memory derivation).
    /// </summary>
    private static double ScoreConfigFact(Fact fact)
    {
        switch (fact.Key)
        {
            case "FILE_AUTOGROWTH_PERCENT":
                // Base 0.3 when at least one large percent-growth file was found; 0 otherwise.
                return fact.Metadata.GetValueOrDefault("file_count") > 0 ? 0.3 : 0.0;

            // MAXDOP at 0 = unlimited parallelism — bad. Any other value is operator-chosen.
            case "CONFIG_MAXDOP":
                return fact.Value == 0 ? 0.4 : 0.0;

            // CTFP <= 5 (the default 5 and below) is too low for almost any workload.
            case "CONFIG_CTFP":
                return fact.Value <= 5 ? 0.4 : 0.0;

            // max server memory left at the 2 PB default = SQL can take all RAM, starving the OS.
            case "CONFIG_MAX_MEMORY_MB":
                return fact.Value == 2147483647 ? 0.4 : 0.0;

            // min server memory pinned near max — emitted by the collector ONLY when bad, so any
            // presence of this fact is a flag.
            case "CONFIG_MIN_MAX_MEMORY_NARROW":
                return 0.4;

            // Priority boost enabled (value_in_use == 1) — a Dashboard WARNING (install/50_configuration_issues_analyzer.sql
            // line 368: "Priority boost is enabled ... not recommended"): it hands SQL Server threads an
            // above-normal Windows scheduling priority, starving OS-critical threads. It is rare and
            // clearly-wrong (not a routine tuning choice like MAXDOP/CTFP), so it scores the WARNING band
            // (0.9) — surfacing prominently when present — rather than the low 0.4 config-advisory base.
            case "CONFIG_PRIORITY_BOOST":
                return fact.Value == 1 ? 0.9 : 0.0;

            // Lightweight pooling / fiber mode enabled (value_in_use == 1) — a Dashboard WARNING
            // (install/50:401: "Lightweight pooling (fiber mode) is enabled ... issues with OLEDB and other
            // components"). Same rationale: rare, clearly-wrong, WARNING band (0.9).
            case "CONFIG_LIGHTWEIGHT_POOLING":
                return fact.Value == 1 ? 0.9 : 0.0;

            // WS5 server-health advisories (advise-only — no Apply). Each carries the bad/good
            // signal in Value so it scores its 0.4 advisory base only when bad and 0 otherwise;
            // the noise-control gating (Express / small-RAM for LPIM, dumps>0, IFI-known) lives in
            // the collectors so a fact that would score 0 is simply never emitted.

            // IFI off (Value == 0) is universally good advice — always advisory when known.
            case "CONFIG_IFI_DISABLED":
                return fact.Value == 0 ? 0.4 : 0.0;

            // LPIM off (Value == 0). The collector only emits this when it plausibly matters
            // (not Express, meaningful RAM), so reaching the scorer with Value 0 is already a flag.
            case "CONFIG_LPIM_DISABLED":
                return fact.Value == 0 ? 0.4 : 0.0;

            // A memory dump always warrants a look — advisory when the count is > 0.
            case "SERVER_MEMORY_DUMPS":
                return fact.Value > 0 ? 0.4 : 0.0;

            default:
                return 0.0;
        }
    }

    /// <summary>
    /// Scores database configuration facts.
    /// Auto-shrink and auto-close are always bad.
    /// RCSI-off gets a low base that only becomes visible through amplifiers
    /// when reader/writer lock contention (LCK_M_S, LCK_M_IS) is present.
    /// </summary>
    private static double ScoreDatabaseConfigFact(Fact fact)
    {
        if (fact.Key != "DB_CONFIG") return 0.0;

        var autoShrink = fact.Metadata.GetValueOrDefault("auto_shrink_on_count");
        var autoClose = fact.Metadata.GetValueOrDefault("auto_close_on_count");
        var pageVerifyBad = fact.Metadata.GetValueOrDefault("page_verify_not_checksum_count");
        var rcsiOff = fact.Metadata.GetValueOrDefault("rcsi_off_count");

        var score = 0.0;

        // Auto-shrink, auto-close, bad page verify are always concerning
        if (autoShrink > 0 || autoClose > 0 || pageVerifyBad > 0)
            score = Math.Max(score, Math.Min((autoShrink + autoClose + pageVerifyBad) * 0.3, 1.0));

        // RCSI-off: low base (0.3) — below display threshold alone.
        // Amplifiers for LCK_M_S/LCK_M_IS push it above 0.5 when reader/writer
        // contention confirms RCSI would help.
        if (rcsiOff > 0)
            score = Math.Max(score, 0.3);

        // Query Store disabled on a user database — INFO advisory (install/50_configuration_issues_analyzer.sql
        // line 83 severity=INFO). Detected purely from the aggregate counts every collector already emits:
        // query_store_on_count (user DBs with QS on; system DBs are excluded from both counts) < database_count
        // (user DB total) means at least one user database has Query Store off. Low 0.3 base — DB_CONFIG is a
        // ConfigAdvisoryRootKey so it roots as a standing INFO advisory at any positive severity, and 0.3 keeps
        // it in the INFO band (< 0.75) matching the Dashboard. Requires BOTH counts present so a fact carrying
        // partial metadata never trips it.
        if (fact.Metadata.TryGetValue("database_count", out var dbCount) && dbCount > 0
            && fact.Metadata.TryGetValue("query_store_on_count", out var queryStoreOn)
            && queryStoreOn < dbCount)
            score = Math.Max(score, 0.3);

        return score;
    }

    /// <summary>
    /// Scores running job facts. Long-running jobs are a signal.
    /// </summary>
    private static double ScoreJobFact(Fact fact)
    {
        return fact.Key switch
        {
            // Long-running jobs: concerning at 1, critical at 3
            "RUNNING_JOBS" => ApplyThresholdFormula(fact.Value, 1, 3),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores disk space facts. Low free space is critical.
    /// </summary>
    private static double ScoreDiskFact(Fact fact)
    {
        if (fact.Key != "DISK_SPACE") return 0.0;

        var freePct = fact.Value;
        // Invert: lower free space is worse. Critical < 5%, concerning < 10%
        if (freePct < 0.05) return 1.0;
        if (freePct < 0.10) return 0.5 + 0.5 * (0.10 - freePct) / 0.05;
        if (freePct < 0.20) return 0.5 * (0.20 - freePct) / 0.10;
        return 0.0;
    }

    /// <summary>
    /// Scores bad actor queries using execution count tier x per-execution impact.
    /// A query running 100K times at 1ms CPU is different from 100K times at 5s CPU.
    /// The tier gets it in the door, per-execution impact determines how bad it is.
    /// </summary>
    private static double ScoreBadActorFact(Fact fact)
    {
        var execCount = fact.Metadata.GetValueOrDefault("execution_count");
        var avgCpuMs = fact.Metadata.GetValueOrDefault("avg_cpu_ms");
        var avgReads = fact.Metadata.GetValueOrDefault("avg_reads");

        // Execution count tier base — higher tiers for more frequent queries
        var tierBase = execCount switch
        {
            < 1_000 => 0.5,
            < 10_000 => 0.7,
            < 100_000 => 0.85,
            _ => 1.0
        };

        // Per-execution impact: use the worse of CPU or reads
        // CPU: concerning at 50ms, critical at 2000ms
        var cpuImpact = ApplyThresholdFormula(avgCpuMs, 50, 2000);
        // Reads: concerning at 5K, critical at 250K
        var readsImpact = ApplyThresholdFormula(avgReads, 5_000, 250_000);

        var impact = Math.Max(cpuImpact, readsImpact);

        // Final: tier * impact. Both must be meaningful.
        // A high-frequency query with trivial per-execution cost won't score.
        // A heavy query that only runs once won't score high either.
        return tierBase * impact;
    }

    // Wait-profile severity ramp (see the ANOMALY_WAIT_PROFILE arm). Floor matches the detectors'
    // DefaultRatioThreshold; these are HONEST per-second-scale starting values. UNCALIBRATED as of the
    // 2026-09 dogfood measurement (#3538 A5): that pass measured each wait TYPE's fraction of a 4-hour
    // window (the statistic GetWaitThresholds grades) over 1,075 server-windows, not the all-types
    // ms/sec PEAK-over-baseline-mean ratio this ramp grades, so none of its figures transfers here.
    // Calibrating this ramp needs the wait-profile detector's own ratio distribution over the fleet —
    // per server-window, peak ms/sec ÷ the hour-of-week baseline mean — which is one more column on the
    // same read, not a different instrument; until it is read, 4x/12x stand as reasoned values.
    private const double WaitProfileRatioFloor = 4.0;
    private const double WaitProfileRatioSpan = 8.0;

    // Bounded-metric low-quality fallback ramp (see the z-score anomaly arm). When the quality gate fires
    // on a thin baseline (baseline_low_quality=1) the stored deviation_sigma is the real (small) z that the
    // 2σ gate would zero out — so grade off the absolute exceedance (peak ÷ the absolute-fallback bar, which
    // is >= 1.0 on a fire) instead: floor 0.5 AT the bar (clears InferenceEngine's 0.5 entry-point), ramping
    // to 1.0 at 2× the bar. UNCALIBRATED as of the 2026-09 dogfood measurement, which read wait fractions
    // and never the fallback exceedance: this path grades a store whose baseline is too thin to trust,
    // which is a fresh install's first days, and the measured fleet is not that. The 2x-the-bar span is
    // a reasoned shape (the same 2x every deviation ramp saturates at), not a measured one, and the
    // measurement that would calibrate it is a young store's exceedance distribution, not more fleet.
    private const double LowQualityFallbackSpan = 1.0;

    /* #3538 A7: the per-observed-hour gates two wait facts carry before their fraction is graded. Both
       were absolute totals before — THREADPOOL >= 3,600,000 ms, PAGELATCH_UP > 10,000 ms — applied to
       whatever window the caller asked for, so hours_back changed the verdict on an unchanged server:
       a THREADPOOL total that is a quarter of a 4 h window (the shipped default, where these were tuned)
       is the WHOLE of a 1 h window and 0.6% of a 168 h one, while a PAGELATCH_UP bar meant for one hour
       of wait_stats was met by 42x the exposure at a week. Dividing by ObservedHours(fact) makes the
       same per-hour rate score the same at every window; dividing by OBSERVED rather than nominal hours
       (#3538 A2) keeps collector downtime from deflating the rate the way it deflated the fractions. */

    /// <summary>
    /// THREADPOOL: the wait total per observed hour below which the fact is thread-pool housekeeping,
    /// not exhaustion. 15 min/hr is the pre-#3538 absolute 1 h bar expressed at the 4 h default window it
    /// was tuned on — identical behaviour there, window-invariant everywhere else. In fraction terms it
    /// is 0.25 of observed time (the numerator sums concurrent waiters, so this is "a quarter of the
    /// schedulers' worth of tasks parked for want of a worker"), which is why THREADPOOL's
    /// GetWaitThresholds pair (0.01, null) is dominated by this gate: any fact that clears it has already
    /// saturated its fraction ramp. Fleet lineage (2026-09, 43 servers, 4 days, 405 non-zero server-hours):
    /// THREADPOOL per hour p50 6 ms, p99 488 ms, max 5,801 ms — the worst hour measured is 0.6% of this
    /// bar, and no hour reached the old 3,600,000 ms either. The bar is set by what exhaustion IS, not by
    /// where the healthy fleet sits; the measurement says only that nothing routine approaches it.
    /// </summary>
    private const double ThreadpoolMinWaitMsPerObservedHour = 900_000;

    /// <summary>
    /// THREADPOOL: the minimum average wait per task. A one-second average means tasks are genuinely
    /// queued for a worker, not briefly parked during pool growth. Unchanged from the pre-#3538 gate and
    /// not a rate, so it needs no window scaling. Fleet lineage: max avg_ms_per_wait on any THREADPOOL
    /// row in the 4-day pass was 247.5 ms.
    /// </summary>
    private const double ThreadpoolMinAvgMsPerWait = 1_000;

    /// <summary>
    /// PAGELATCH_UP: the wait total per observed hour above which the fact scores its flat 0.5. 10 s/hr is
    /// the ported source's own bar at the ported source's own window — report.tempdb_contention_analysis
    /// sums PAGELATCH_UP over collect.wait_stats for the LAST HOUR (install/47:2411) and trips at
    /// &gt; 10000 ms (:2515) — so this is the port made faithful, not a re-derivation. Fleet lineage (2026-09,
    /// 1,075 server-4h-windows): PAGELATCH_UP per 4 h p50 35.5 ms, p90 1,091 ms, p99 5,830 ms, max
    /// 14,274 ms; the old absolute &gt; 10,000 ms fired on 2 of those windows (0.19%), both of them under 3.6 s
    /// per hour — they fired only because the bar meant for one hour was being applied to four. Under the
    /// hourly form no measured window reaches it. The alternative — 2.5 s/hr, which would have kept the
    /// 4 h default firing on exactly those two windows — was rejected because that sensitivity was an
    /// accident of the default window length, never a calibrated choice. What this cannot see: a 10 s
    /// burst inside one hour of a longer window averages out (the per-sample-vs-window class, #3538 A8).
    /// </summary>
    private const double PagelatchUpMinWaitMsPerObservedHour = 10_000;

    // Layer-3 tuning-class severity ceiling (see ScoreAll). Parallelism/anomaly signals describe a tuning
    // opportunity, not an outage — their FINAL severity is capped here (bands are >= 1.5 CRITICAL) unless an
    // impact peer co-fired or (#3526, ANOMALY_* only) the anomaly's own deviation is extreme. 1.49 keeps a
    // capped fact in the WARNING band without touching SeverityBand.
    private const double TuningClassSeverityCeiling = 1.49;

    /* #3526: the extremity escape's multiple (see IsExtremeAnomaly). An anomaly leaves the tuning-class cap
       when its deviation is this many times the cutoff it FIRED at. Every deviation ramp saturates its base
       at 2x its anchor (ScoreAnomalyFact: 0.5 at the anchor, 1.0 at 2x), so 3x sits a full anchor PAST the
       point where the ramp stopped distinguishing — "extreme" means "so far out the scorer ran out of
       scale", not "the top of the ramp". The arithmetic per path, with the detectors' shipped cutoffs
       (AnomalyThresholds): robust modified-z 3.5 → the escape opens at 10.5σ; heavy-tail modified-z 5.0
       (waits, query duration) → 15σ; classical z 2.0 (rollup-bound metrics, pre-#1743 facts) → 6σ — the
       #1743 fleet measurement read a busy tenant's REAL 2-3x evening surge at 1.4-2.0 classical sigmas,
       so 6 classical sigmas against a stddev that history has already inflated is a genuinely rare
       reading, not a busy evening. All three sit under the 25σ display cap (SigmaDisplayCap), so a fact
       can actually carry them. The never-blind fallback path (baseline_low_quality) has no meaningful
       sigma, so it escapes only at 3x its ABSOLUTE bar (fallback_exceedance >= 3): I/O latency 150 ms,
       batch requests 15,000/s, sessions 1,500, query duration 15 s total elapsed — and CPU (bar 90%) and
       memory total/target (bar 101%) can never reach 3x their bars, so a young store's CPU or memory
       anomaly cannot escape on an untrustworthy baseline at all, which is correct: "we do not know your
       normal yet" is not evidence of an outage. An operator who scales a metric's deviation threshold
       scales its fire_threshold with it (ModifiedZThresholdFor), so the escape bar tracks the knob — up
       to the display cap: AnomalyGate clamps the stored deviation_sigma at SigmaDisplayCap (25σ) BEFORE
       the scorer ever sees it, so a bar above 25σ would be unreachable and the escape would go silently
       dead for exactly the deployments that tuned a metric hard (a knob past ~8.3x the shipped anchor
       puts 3x over 25) — the same "structurally quietest" defect this constant exists to fix, just for a
       differently-tuned store. IsExtremeAnomaly therefore takes min(3x anchor, SigmaDisplayCap): a sigma
       pinned at the cap means "at least 25σ", which is extreme under any anchor an operator can set. The
       wait profile's modified_z is not display-capped (BaselineMath.ModifiedZScore) and needs no such
       bound. */
    private const double ExtremeAnomalyMultiple = 3.0;

    /// <summary>
    /// Tuning-class keys whose FINAL severity is capped at the WARNING ceiling (Layer 3) unless an
    /// impact peer co-fired: parallelism (CXPACKET/CXCONSUMER), excessive-DOP queries, and every
    /// anomaly fact. CXPACKET and (#3526) the corroborated ANOMALY_* families can exceed the ceiling
    /// on amplifiers; an anomaly is released from the cap only when <see cref="IsExtremeAnomaly"/> holds.
    /// QUERY_HIGH_DOP still maxes at 1.0 — its membership is forward-safety as that ramp evolves.
    /// </summary>
    private static bool IsTuningClassKey(string key) =>
        key is "CXPACKET" or "CXCONSUMER" or "QUERY_HIGH_DOP"
        || key.StartsWith("ANOMALY_", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The deviation-scored anomaly families (the z-score / modified-z detectors: peak vs a per-server
    /// hour-of-week baseline, graded off <c>deviation_sigma</c> against <c>fire_threshold</c>, or off
    /// <c>fallback_exceedance</c> on the low-quality path). Shared by <see cref="ScoreAnomalyFact"/> and
    /// <see cref="IsExtremeAnomaly"/> so the two cannot route a key differently.
    /// </summary>
    private static bool IsDeviationScoredAnomalyKey(string key) =>
        /* #3542: the PostgreSQL z-score families are registered in PgTargetScorer, not here — the shared
           extremity escape and ramp then read the same AnomalyGate metadata for both engines (#3584). */
        PgTargetScorer.IsDeviationScoredAnomalyKey(key)
        || key.StartsWith("ANOMALY_CPU_SPIKE", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_READ_LATENCY", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_WRITE_LATENCY", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_BATCH_REQUESTS", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_SESSION_SPIKE", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_QUERY_DURATION", StringComparison.OrdinalIgnoreCase)
        || key.StartsWith("ANOMALY_MEMORY_PRESSURE", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// #1743: the cutoff a deviation-scored anomaly actually FIRED at (carried by the detector as
    /// <c>fire_threshold</c>): the knob-scaled classical threshold on the classical path, the knob-scaled
    /// modified-z cutoff on the robust path, and the pre-#1743 default of 2.0 when the fact carries none.
    /// The severity ramp anchors here and the extremity escape is a multiple of it.
    /// </summary>
    private static double FireAnchor(Fact fact)
    {
        var anchor = fact.Metadata.GetValueOrDefault("fire_threshold", 2.0);
        return anchor <= 0 ? 2.0 : anchor;
    }

    /// <summary>
    /// #3526: whether an ANOMALY_* fact's deviation is extreme enough to leave the Layer-3 tuning-class
    /// cap on its own evidence (see <see cref="ExtremeAnomalyMultiple"/> for the arithmetic). Routes
    /// each family off the SAME metadata its severity ramp grades from, so a fact can never be extreme
    /// on one statistic while scored on another:
    /// <list type="bullet">
    ///   <item>Deviation-scored families, trustworthy baseline: <c>deviation_sigma &gt;= 3 x fire_threshold</c>.</item>
    ///   <item>Deviation-scored families, low-quality baseline: <c>fallback_exceedance &gt;= 3</c> — the
    ///     stored sigma is the real (small, meaningless) z the detector refused to trust, so the escape
    ///     must not read it either.</item>
    ///   <item>ANOMALY_WAIT_PROFILE on the robust trigger: <c>modified_z &gt;= 3 x 5.0</c>; on the
    ///     pre-#1743 / robust-less ratio trigger: <c>ratio &gt;= 3 x 4.0</c>. Never for <c>is_new</c> — a
    ///     first-occurrence profile's sentinel ratio (NoBaselineRatio, 100) is a scoring device, not a
    ///     measurement, and "no baseline" cannot be "extreme against baseline".</item>
    ///   <item>ANOMALY_PG_WAIT_PROFILE (#3691): the same three readings through
    ///     <see cref="PgTargetScorer.IsExtremeWaitProfileAnomaly"/>, with the ratio arm anchored on the
    ///     PostgreSQL profile's own firing multiple; the PostgreSQL deadlock-rate ratio family stays capped.</item>
    /// </list>
    /// The ratio/count/delta families — blocking and deadlock spikes, day-over-day object growth and
    /// contention, the legacy per-type ANOMALY_WAIT_ facts — stay capped: none is graded in sigmas, so
    /// "3x the fire threshold" has no calibrated meaning for them, and the blocking/deadlock classes
    /// already reach CRITICAL through their never-capped impact keys (BLOCKING_EVENTS, BLOCKING_CHAIN,
    /// DEADLOCKS) when the events are real.
    /// </summary>
    private static bool IsExtremeAnomaly(Fact fact)
    {
        if (!fact.Key.StartsWith("ANOMALY_", StringComparison.OrdinalIgnoreCase)) return false;

        if (IsDeviationScoredAnomalyKey(fact.Key))
        {
            if (fact.Metadata.GetValueOrDefault("baseline_low_quality") >= 1.0)
                return fact.Metadata.GetValueOrDefault("fallback_exceedance") >= ExtremeAnomalyMultiple;

            // Bounded at the display cap — see ExtremeAnomalyMultiple: the stored sigma can never exceed it.
            var escapeBar = Math.Min(ExtremeAnomalyMultiple * FireAnchor(fact), Baselines.AnomalyThresholds.SigmaDisplayCap);
            return fact.Metadata.GetValueOrDefault("deviation_sigma") >= escapeBar;
        }

        /* #3691 (#3689 §5 residue): the PostgreSQL wait profile escapes the same way its SQL Server twin
           below does — the one PG arm, delegating so the predicate sits beside the ramp it must agree with
           (PgTargetScorer.ScoreRatioAnomaly): same is_new refusal, same 3x heavy-tail modified-z bar on a
           robust bucket, and 3x ITS OWN ratio anchor (PgRatioAnomalyThreshold) otherwise, where the arm below
           reads 3x WaitProfileRatioFloor. The multiple is passed so "extreme" is one number for both engines. */
        if (PgTargetScorer.IsPgRatioAnomalyKey(fact.Key))
            return PgTargetScorer.IsExtremeWaitProfileAnomaly(fact, ExtremeAnomalyMultiple);

        if (fact.Key.StartsWith("ANOMALY_WAIT_PROFILE", StringComparison.OrdinalIgnoreCase))
        {
            if (fact.Metadata.GetValueOrDefault("is_new") > 0) return false;
            var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
            if (modifiedZ > 0)
                return modifiedZ >= ExtremeAnomalyMultiple * Baselines.AnomalyThresholds.HeavyTailModifiedZThreshold;
            return fact.Metadata.GetValueOrDefault("ratio") >= ExtremeAnomalyMultiple * WaitProfileRatioFloor;
        }

        return false;
    }

    /// <summary>
    /// Scores anomaly facts based on deviation from baseline.
    /// At 2σ → 0.5, at 4σ → 1.0. Higher deviations are more severe.
    /// For count-based anomalies (blocking/deadlock spikes), uses ratio instead.
    /// </summary>
    private static double ScoreAnomalyFact(Fact fact)
    {
        /* #3542: the PostgreSQL ratio families (deadlock rate) have their own ramp — the SQL Server ratio
           arms below recognise ANOMALY_BLOCKING_SPIKE / ANOMALY_DEADLOCK_SPIKE by literal prefix and would
           never see them. The PostgreSQL z-score families deliberately fall THROUGH to the shared
           deviation ramp below via IsDeviationScoredAnomalyKey. */
        if (PgTargetScorer.IsPgRatioAnomalyKey(fact.Key))
            return PgTargetScorer.ScoreRatioAnomaly(fact);

        if (IsDeviationScoredAnomalyKey(fact.Key))
        {
            // Deviation-based scoring: 2σ = 0.5, 4σ = 1.0
            var deviation = fact.Metadata.GetValueOrDefault("deviation_sigma");
            var confidence = fact.Metadata.GetValueOrDefault("confidence", 1.0);

            // Thin/untrustworthy baseline: the detector's quality gate fired on the absolute-fallback bar,
            // NOT the z-score, so deviation_sigma is the real (small) z. Applying the 2σ gate below would
            // zero it and InferenceEngine would silently drop the finding (Severity must clear 0.5 to root)
            // — defeating the "fire on the absolute bar, not silence" guarantee (e.g. memory 96% on a young
            // store). Grade off the absolute exceedance instead: the fire already cleared the bar so
            // exceedance >= 1.0 → floor 0.5, ramping to 1.0 at 2× the bar.
            if (fact.Metadata.GetValueOrDefault("baseline_low_quality") >= 1.0)
            {
                var over = Math.Max(0.0, fact.Metadata.GetValueOrDefault("fallback_exceedance") - 1.0);
                // Floor AFTER the confidence multiply, not before it. confidence is a hardcoded 1.0
                // everywhere today so the ramp already lands >= 0.5 at the bar, but if confidence ever
                // drops sub-1.0 the un-floored product would fall below InferenceEngine's 0.5 root
                // entry-point and silently drop the finding — re-breaking the "fire on the absolute bar,
                // not silence" guarantee. Math.Max(0.5, ...) keeps a fired low-quality anomaly rootable
                // regardless of confidence. Behaviorally identical at confidence == 1.0.
                return Math.Max(0.5, (0.5 + 0.5 * Math.Min(over / LowQualityFallbackSpan, 1.0)) * confidence);
            }

            /* #1743: the ramp anchors on the cutoff the fact actually FIRED at (carried by the
               detector as fire_threshold), saturating at 2x the anchor — exactly the old 2σ→4σ
               shape for classical fires and for pre-#1743 facts (default 2.0), and the same
               proportional shape for robust fires at 3.5 or 5.0. Without the anchor, a family
               firing at 5σ scores saturated-flat 1.0 forever against a ramp built for 2σ fires. */
            var anchor = FireAnchor(fact);
            if (deviation < anchor) return 0.0;
            var base_score = 0.5 + 0.5 * Math.Min((deviation - anchor) / anchor, 1.0);
            return base_score * confidence;
        }

        // Wait-profile (the one ANOMALY_WAIT_PROFILE fact) — must precede the generic ANOMALY_WAIT_
        // branch below, which it also prefix-matches. The ratio is now the HONEST per-second scale
        // (peak window all-types ms/sec ÷ baseline mean), so the ramp is far smaller than the old
        // 5×/20× that was calibrated to a ~240×-inflated per-hour-vs-per-interval input: 4× → 0.5,
        // saturating to 1.0 at 12×. Starting values matching the detectors' DefaultRatioThreshold;
        // still uncalibrated — see WaitProfileRatioFloor for what the 2026-09 fleet pass did and did not
        // measure, and which read would calibrate this ramp.
        if (fact.Key.StartsWith("ANOMALY_WAIT_PROFILE", StringComparison.OrdinalIgnoreCase))
        {
            /* #1743: detectors with robust baselines fire this fact on the MODIFIED z-score, and
               carry it as modified_z — grade off the same statistic, or the masked-surge class the
               robust trigger exists to catch (real sustained deviations whose ratio sits under 4x
               against a burst-inflated mean) would be zeroed right after being caught. Ramp mirrors
               the ratio's shape: 0.5 at the 5.0 firing cutoff, saturating to 1.0 at 15σ. A fact
               without modified_z (pre-#1743 detector, robust-less bucket, or the is_new fallback
               whose sentinel ratio must keep scoring) keeps the ratio ramp unchanged. */
            var modifiedZ = fact.Metadata.GetValueOrDefault("modified_z");
            var isNewProfile = fact.Metadata.GetValueOrDefault("is_new") > 0;
            if (modifiedZ > 0 && !isNewProfile)
            {
                if (modifiedZ < Baselines.AnomalyThresholds.HeavyTailModifiedZThreshold) return 0.0;
                return 0.5 + 0.5 * Math.Min(
                    (modifiedZ - Baselines.AnomalyThresholds.HeavyTailModifiedZThreshold) / 10.0, 1.0);
            }
            var ratio = fact.Metadata.GetValueOrDefault("ratio");
            if (ratio < WaitProfileRatioFloor) return 0.0;
            return 0.5 + 0.5 * Math.Min((ratio - WaitProfileRatioFloor) / WaitProfileRatioSpan, 1.0);
        }

        if (fact.Key.StartsWith("ANOMALY_WAIT_", StringComparison.OrdinalIgnoreCase))
        {
            // Legacy per-type wait anomaly (detectors now emit ANOMALY_WAIT_PROFILE instead; kept for
            // any pre-upgrade persisted facts). Ratio-based scoring: 5x = 0.5, 20x = 1.0.
            var ratio = fact.Metadata.GetValueOrDefault("ratio");
            if (ratio < 5) return 0.0;
            return 0.5 + 0.5 * Math.Min((ratio - 5.0) / 15.0, 1.0);
        }

        if (fact.Key.StartsWith("ANOMALY_BLOCKING_SPIKE", StringComparison.OrdinalIgnoreCase) || fact.Key.StartsWith("ANOMALY_DEADLOCK_SPIKE", StringComparison.OrdinalIgnoreCase))
        {
            // Ratio-based: 3x = 0.5, 10x = 1.0
            var ratio = fact.Metadata.GetValueOrDefault("ratio");
            if (ratio < 3) return 0.0;
            return 0.5 + 0.5 * Math.Min((ratio - 3.0) / 7.0, 1.0);
        }

        if (fact.Key.StartsWith("ANOMALY_OBJECT_GROWTH", StringComparison.OrdinalIgnoreCase))
        {
            // Ratio of growth vs the trip threshold: 1x = 0.4, 5x = 1.0 (day-over-day table growth)
            var ratio = fact.Metadata.GetValueOrDefault("growth_ratio");
            if (ratio < 1.0) return 0.0;
            return 0.4 + 0.6 * Math.Min((ratio - 1.0) / 4.0, 1.0);
        }

        if (fact.Key.StartsWith("ANOMALY_OBJECT_CONTENTION", StringComparison.OrdinalIgnoreCase))
        {
            // Ratio of new lock-wait ms vs the trip threshold: 1x = 0.4, 10x = 1.0
            var ratio = fact.Metadata.GetValueOrDefault("contention_ratio");
            if (ratio < 1.0) return 0.0;
            return 0.4 + 0.6 * Math.Min((ratio - 1.0) / 9.0, 1.0);
        }

        return 0.0;
    }

    /// <summary>
    /// Generic threshold formula used by waits, latency, and count-based metrics.
    /// Critical == null means "concerning only" — hitting concerning = 1.0.
    /// </summary>
    internal static double ApplyThresholdFormula(double value, double concerning, double? critical)
    {
        if (value <= 0) return 0.0;

        if (critical == null)
            return Math.Min(value / concerning, 1.0);

        if (value >= critical.Value)
            return 1.0;

        if (value >= concerning)
            return 0.5 + 0.5 * (value - concerning) / (critical.Value - concerning);

        return 0.5 * (value / concerning);
    }

    /// <summary>
    /// Returns amplifier definitions for a fact. Each amplifier has a description,
    /// a boost value, and a predicate that evaluates against the current fact set.
    /// Amplifiers are defined per wait type and will grow as more fact categories are added.
    /// </summary>
    private static List<AmplifierDefinition> GetAmplifiers(Fact fact)
    {
        return fact.Key switch
        {
            "SOS_SCHEDULER_YIELD" => SosSchedulerYieldAmplifiers(),
            "CXPACKET" => CxPacketAmplifiers(),
            "THREADPOOL" => ThreadpoolAmplifiers(),
            "PAGEIOLATCH_SH" or "PAGEIOLATCH_EX" => PageiolatchAmplifiers(),
            "LATCH_EX" or "LATCH_SH" => LatchAmplifiers(),
            "BLOCKING_EVENTS" => BlockingEventsAmplifiers(),
            "BLOCKING_CHAIN" => BlockingChainAmplifiers(),
            "RESOURCE_SEMAPHORE_QUERY_COMPILE" => ResourceSemaphoreQueryCompileAmplifiers(),
            "DEADLOCKS" => DeadlockAmplifiers(),
            "LCK" => LckAmplifiers(),
            "CPU_SQL_PERCENT" => CpuSqlPercentAmplifiers(),
            "CPU_SPIKE" => CpuSpikeAmplifiers(),
            "IO_READ_LATENCY_MS" => IoReadLatencyAmplifiers(),
            "IO_WRITE_LATENCY_MS" => IoWriteLatencyAmplifiers(),
            "MEMORY_GRANT_PENDING" => MemoryGrantAmplifiers(),
            "QUERY_SPILLS" => QuerySpillAmplifiers(),
            "PARAMETER_SENSITIVITY" => ParameterSensitivityAmplifiers(),
            "PLAN_REGRESSION" => PlanRegressionAmplifiers(),
            "DB_CONFIG" => DbConfigAmplifiers(),
            "DISK_SPACE" => DiskSpaceAmplifiers(),
            /* #3542: BEFORE the ANOMALY_ arm, so ANOMALY_PG_* routes to the PostgreSQL table and not to the
               SQL Server load-family confirmers (CPU_SQL_PERCENT is not a fact a PostgreSQL pass can emit). */
            _ when PgTargetScorer.IsPgKey(fact.Key) => PgTargetScorer.Amplifiers(fact.Key),
            _ when fact.Key.StartsWith("ANOMALY_", StringComparison.OrdinalIgnoreCase) => AnomalyAmplifiers(fact.Key),
            _ => []
        };
    }

    /// <summary>
    /// #3526: the anomaly co-fire arm — corroboration for the baseline engine's findings, which had no
    /// amplifiers at all and so could never leave their 1.0 base. Modelled on the impact-peer style of
    /// the Layer-3 escape and the PAGEIOLATCH / IO-latency arms: a sibling anomaly family firing in the
    /// same window (BaseSeverity &gt; 0 — the scorer zeroes anything under its cutoff, so &gt; 0 means
    /// "fired against its own baseline") is a co-fire, and a MEASURED absolute fact confirming the same
    /// pressure (SQL CPU &gt;= 80%, the I/O-latency fact at its concerning bar, grant waiters, the
    /// buffer-pool / log waits at the bars their own arms use) is a co-fire.
    ///
    /// <para>Worked numbers — the arm's magnitudes are chosen so corroboration can carry an EXTREME anomaly
    /// past the 1.5 notify floor and nothing can carry a routine one there:</para>
    /// <list type="bullet">
    ///   <item>Base at the fire threshold (0.5) with two co-fires: 0.5 x (1 + 0.3 + 0.3) = 0.8. With every
    ///     arm lit (the load family's maximum is +1.2): 1.1. Never reaches 1.5, and the Layer-3 cap holds
    ///     regardless because the anomaly is not extreme.</item>
    ///   <item>Base saturated but ROUTINE (2x the anchor — 4σ classical, 7σ robust; 1.0) with three co-fires:
    ///     1.0 x 1.9 = 1.9 → capped to 1.49. Still WARNING: saturation is not extremity, and the cap is
    ///     exactly what keeps a busy evening from paging.</item>
    ///   <item>Base EXTREME (&gt;= 3x the anchor; 1.0, the cap released) alone: 1.0. Below the cap it escaped
    ///     — a lone 20σ reading with nothing else moving does not page, by design: the CRITICAL band is
    ///     earned only with corroboration (the same rule every other base fact follows), and a solitary
    ///     extreme reading is exactly the shape a collector hiccup or a variance-collapsed baseline pinned
    ///     at the 25σ display cap produces.</item>
    ///   <item>Base EXTREME with one +0.3 co-fire: 1.3, WARNING. With two: 1.6 → pages. The issue's own
    ///     3am shape — a 20σ session spike (root, extreme) beside a 15σ batch-request anomaly (+0.3) and
    ///     SQL CPU at 85% (+0.3) — scores 1.6 and reaches the operator for the first time at shipped
    ///     settings. A +0.3 and a +0.2 land on 1.5 exactly: two independent corroborators is the bar.</item>
    /// </list>
    /// </summary>
    private static List<AmplifierDefinition> AnomalyAmplifiers(string key)
    {
        if (key.StartsWith("ANOMALY_SESSION_SPIKE", StringComparison.OrdinalIgnoreCase)
            || key.StartsWith("ANOMALY_BATCH_REQUESTS", StringComparison.OrdinalIgnoreCase)
            || key.StartsWith("ANOMALY_CPU_SPIKE", StringComparison.OrdinalIgnoreCase)
            || key.StartsWith("ANOMALY_QUERY_DURATION", StringComparison.OrdinalIgnoreCase))
            return LoadAnomalyAmplifiers(key);

        if (key.StartsWith("ANOMALY_READ_LATENCY", StringComparison.OrdinalIgnoreCase))
            return ReadLatencyAnomalyAmplifiers();

        if (key.StartsWith("ANOMALY_WRITE_LATENCY", StringComparison.OrdinalIgnoreCase))
            return WriteLatencyAnomalyAmplifiers();

        if (key.StartsWith("ANOMALY_WAIT_PROFILE", StringComparison.OrdinalIgnoreCase))
            return WaitProfileAnomalyAmplifiers();

        if (key.StartsWith("ANOMALY_MEMORY_PRESSURE", StringComparison.OrdinalIgnoreCase))
            return MemoryPressureAnomalyAmplifiers();

        // Blocking/deadlock spikes and the object-stats anomalies have no arm: they are not released from
        // the cap (IsExtremeAnomaly) and their impact lives in the never-capped BLOCKING_* / DEADLOCKS keys.
        return [];
    }

    /// <summary>
    /// A sibling anomaly family fired in the same window against its own baseline. The self-key is
    /// skipped by the callers, so a family never corroborates itself.
    /// </summary>
    private static bool AnomalyCoFired(Dictionary<string, Fact> facts, string siblingKey) =>
        facts.TryGetValue(siblingKey, out var sibling) && sibling.BaseSeverity > 0;

    /// <summary>
    /// The LOAD family — sessions, batch requests, CPU, query duration — corroborate one another (a real
    /// surge moves more than one of them) and are confirmed by measured SQL CPU at the 80% bar the SOS
    /// and compile-gateway arms already use. Each sibling is +0.3; the root's own key is omitted.
    /// </summary>
    private static List<AmplifierDefinition> LoadAnomalyAmplifiers(string selfKey)
    {
        var amplifiers = new List<AmplifierDefinition>();
        void Sibling(string siblingKey, string description)
        {
            if (selfKey.StartsWith(siblingKey, StringComparison.OrdinalIgnoreCase)) return;
            amplifiers.Add(new()
            {
                Description = description,
                Boost = 0.3,
                Predicate = facts => AnomalyCoFired(facts, siblingKey)
            });
        }

        Sibling("ANOMALY_SESSION_SPIKE", "Session-count anomaly co-fired — the surge is visible in connections too");
        Sibling("ANOMALY_BATCH_REQUESTS", "Batch-request anomaly co-fired — the surge is visible in throughput too");
        Sibling("ANOMALY_CPU_SPIKE", "CPU anomaly co-fired — the surge is consuming CPU far above this server's norm");
        Sibling("ANOMALY_QUERY_DURATION", "Query-duration anomaly co-fired — the surge is slowing queries");
        amplifiers.Add(new()
        {
            Description = "SQL Server CPU >= 80% — the surge is consuming real CPU, not just moving a counter",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("CPU_SQL_PERCENT", out var cpu) && cpu.Value >= 80
        });
        return amplifiers;
    }

    /// <summary>
    /// ANOMALY_READ_LATENCY: a per-server read-latency deviation confirmed by the absolute read-latency fact
    /// at its concerning bar (20 ms — "bad in absolute terms, not only for you"), by the wait profile
    /// shifting (queries are actually waiting on it), by write latency deviating alongside (a storage-side
    /// event, not one hot file), and by PAGEIOLATCH at the IO_READ_LATENCY_MS arm's own 10% bar.
    /// </summary>
    private static List<AmplifierDefinition> ReadLatencyAnomalyAmplifiers() =>
    [
        new()
        {
            Description = "Read latency at the absolute concerning bar — slow for any server, not only against this baseline",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("IO_READ_LATENCY_MS", out var io) && io.BaseSeverity >= 0.5
        },
        new()
        {
            Description = "Wait-profile anomaly co-fired — queries are waiting on the slow reads",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_WAIT_PROFILE")
        },
        new()
        {
            Description = "Write-latency anomaly co-fired — the storage path is slow in both directions",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_WRITE_LATENCY")
        },
        new()
        {
            Description = "PAGEIOLATCH waits elevated — buffer pool misses confirm the read pressure",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "PAGEIOLATCH_SH", 0.10)
                              || HasSignificantWait(facts, "PAGEIOLATCH_EX", 0.10)
        }
    ];

    /// <summary>
    /// ANOMALY_WRITE_LATENCY: the write-side twin — the absolute write-latency fact at its concerning bar
    /// (10 ms), the wait profile shifting, read latency deviating alongside, and WRITELOG at the
    /// IO_WRITE_LATENCY_MS arm's own 5% bar.
    /// </summary>
    private static List<AmplifierDefinition> WriteLatencyAnomalyAmplifiers() =>
    [
        new()
        {
            Description = "Write latency at the absolute concerning bar — slow for any server, not only against this baseline",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("IO_WRITE_LATENCY_MS", out var io) && io.BaseSeverity >= 0.5
        },
        new()
        {
            Description = "Wait-profile anomaly co-fired — queries are waiting on the slow writes",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_WAIT_PROFILE")
        },
        new()
        {
            Description = "Read-latency anomaly co-fired — the storage path is slow in both directions",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_READ_LATENCY")
        },
        new()
        {
            Description = "WRITELOG waits elevated — transaction log I/O confirms the write pressure",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "WRITELOG", 0.05)
        }
    ];

    /// <summary>
    /// ANOMALY_WAIT_PROFILE: the all-types wait rate shifting against its baseline, corroborated by WHAT the
    /// waiting is costing — I/O latency deviating (+0.3 each side), query duration deviating (+0.3: the
    /// waits are landing on user queries), and the load family moving (+0.2 each: a surge is driving it).
    /// </summary>
    private static List<AmplifierDefinition> WaitProfileAnomalyAmplifiers() =>
    [
        new()
        {
            Description = "Read-latency anomaly co-fired — the wait shift is storage-bound",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_READ_LATENCY")
        },
        new()
        {
            Description = "Write-latency anomaly co-fired — the wait shift is log/storage-bound",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_WRITE_LATENCY")
        },
        new()
        {
            Description = "Query-duration anomaly co-fired — the waits are landing on user queries",
            Boost = 0.3,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_QUERY_DURATION")
        },
        new()
        {
            Description = "CPU anomaly co-fired — a load surge is driving the wait shift",
            Boost = 0.2,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_CPU_SPIKE")
        },
        new()
        {
            Description = "Session-count anomaly co-fired — a connection surge is driving the wait shift",
            Boost = 0.2,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_SESSION_SPIKE")
        }
    ];

    /// <summary>
    /// ANOMALY_MEMORY_PRESSURE: total-over-target deviating against baseline, corroborated by the symptoms
    /// real memory pressure produces — ring-buffer pressure notifications (the engine saying so itself),
    /// grant waiters at the PAGEIOLATCH arm's bar, RESOURCE_SEMAPHORE in the wait stats, PAGEIOLATCH at
    /// the 10% bar (buffer pool churn), and read latency deviating (the churn reaching storage).
    /// </summary>
    private static List<AmplifierDefinition> MemoryPressureAnomalyAmplifiers() =>
    [
        new()
        {
            Description = "Memory-pressure notifications present — the engine itself is reporting pressure",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("MEMORY_PRESSURE_EVENTS", out var mp) && mp.BaseSeverity > 0
        },
        new()
        {
            Description = "Memory grant waiters present — grants competing for the same memory",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("MEMORY_GRANT_PENDING", out var mg) && mg.Value >= 1
        },
        new()
        {
            Description = "RESOURCE_SEMAPHORE waits present — grant pressure visible in wait stats",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("RESOURCE_SEMAPHORE", out var rs) && rs.BaseSeverity > 0
        },
        new()
        {
            Description = "PAGEIOLATCH waits elevated — buffer pool churning under the pressure",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "PAGEIOLATCH_SH", 0.10)
                              || HasSignificantWait(facts, "PAGEIOLATCH_EX", 0.10)
        },
        new()
        {
            Description = "Read-latency anomaly co-fired — the churn is reaching storage",
            Boost = 0.2,
            Predicate = facts => AnomalyCoFired(facts, "ANOMALY_READ_LATENCY")
        }
    ];

    /// <summary>
    /// PARAMETER_SENSITIVITY: a single plan with wildly varying per-execution cost.
    /// Corroborated by grant/spill divergence and memory-grant pressure.
    /// </summary>
    private static List<AmplifierDefinition> ParameterSensitivityAmplifiers() =>
    [
        new()
        {
            Description = "Three or more sensitive plans — systemic parameter-sniffing problem",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("PARAMETER_SENSITIVITY", out var f)
                              && f.Metadata.GetValueOrDefault("offender_count") >= 3
        },
        new()
        {
            Description = "Memory grant varies with the plan — classic sniffing fingerprint",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("PARAMETER_SENSITIVITY", out var f)
                              && f.Metadata.GetValueOrDefault("grant_divergence") > 0
        },
        new()
        {
            Description = "Worst plan spills on some parameter values but not others",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("PARAMETER_SENSITIVITY", out var f)
                              && f.Metadata.GetValueOrDefault("spill_divergence") > 0
        },
        new()
        {
            Description = "Memory grant pressure present — sensitive plans competing for grants",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("MEMORY_GRANT_PENDING", out var f) && f.BaseSeverity > 0
        }
    ];

    /// <summary>
    /// PLAN_REGRESSION: a query running a worse plan than one it performed well with.
    /// Corroborated by a failing forced plan and by CPU pressure.
    /// </summary>
    private static List<AmplifierDefinition> PlanRegressionAmplifiers() =>
    [
        new()
        {
            Description = "Three or more regressed queries — systemic plan-choice instability",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("PLAN_REGRESSION", out var f)
                              && f.Metadata.GetValueOrDefault("offender_count") >= 3
        },
        new()
        {
            Description = "Worst regression is on a forced plan that is failing to apply",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("PLAN_REGRESSION", out var f)
                              && f.Metadata.GetValueOrDefault("latest_is_forced") > 0
                              && f.Metadata.GetValueOrDefault("force_failure_count") > 0
        },
        new()
        {
            Description = "CPU spike present — regressed plan likely driving it",
            Boost = 0.25,
            Predicate = facts => facts.TryGetValue("CPU_SPIKE", out var f) && f.BaseSeverity > 0
        },
        new()
        {
            Description = "SQL Server CPU elevated — regressed plan contributing",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("CPU_SQL_PERCENT", out var f) && f.BaseSeverity > 0
        }
    ];

    /// <summary>
    /// SOS_SCHEDULER_YIELD: CPU starvation confirmed by parallelism waits.
    /// More amplifiers added when config and CPU utilization facts are available.
    /// </summary>
    private static List<AmplifierDefinition> SosSchedulerYieldAmplifiers() =>
    [
        new()
        {
            Description = "CXPACKET significant — parallelism consuming schedulers",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "THREADPOOL waits present — escalating to thread exhaustion",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        },
        new()
        {
            Description = "SQL Server CPU > 80% — confirmed CPU saturation",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("CPU_SQL_PERCENT", out var cpu) && cpu.Value >= 80
        }
    ];

    /// <summary>
    /// CXPACKET: parallelism waits confirmed by CPU pressure and bad config.
    /// CXCONSUMER is grouped into CXPACKET by the collector.
    /// </summary>
    private static List<AmplifierDefinition> CxPacketAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD high — CPU starvation from parallelism",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.25)
        },
        new()
        {
            Description = "THREADPOOL waits present — thread exhaustion cascade",
            Boost = 0.4,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        },
        new()
        {
            Description = "CTFP at default (5) — too low for most workloads",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("CONFIG_CTFP", out var ctfp) && ctfp.Value <= 5
        },
        new()
        {
            Description = "MAXDOP at 0 — unlimited parallelism",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("CONFIG_MAXDOP", out var maxdop) && maxdop.Value == 0
        },
        new()
        {
            Description = "Queries running with DOP > 8 — excessive parallelism confirmed",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("QUERY_HIGH_DOP", out var dop) && dop.BaseSeverity > 0
        }
    ];

    /// <summary>
    /// THREADPOOL: thread exhaustion — the impact-bearing escalation path for a parallelism →
    /// worker-exhaustion meltdown. The CXPACKET amplifier is deliberately heavy (+0.5): CXPACKET
    /// itself is capped at the WARNING ceiling (see the Layer-3 cap in ScoreAll), so a genuine
    /// meltdown must reach CRITICAL through THREADPOOL (an impact key, never capped), not through
    /// parallelism alone. The runnable-queue amplifier corroborates real scheduler CPU pressure from
    /// the RUNNABLE_TASKS context fact's runnable_tasks_warning flag (the collector's own
    /// SUM(runnable_tasks_count) >= cpu_count heuristic, read from cpu_scheduler_stats).
    /// </summary>
    private static List<AmplifierDefinition> ThreadpoolAmplifiers() =>
    [
        new()
        {
            Description = "CXPACKET significant — parallel queries consuming thread pool",
            Boost = 0.5,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "Runnable-task queue backed up — schedulers under real CPU pressure",
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue("RUNNABLE_TASKS", out var rt)
                              && rt.Metadata.GetValueOrDefault("runnable_tasks_warning") >= 1.0
        },
        new()
        {
            Description = "Lock contention present — blocked queries holding worker threads",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("LCK") && facts["LCK"].BaseSeverity >= 0.5
        }
    ];

    /// <summary>
    /// PAGEIOLATCH: memory pressure confirmed by other waits.
    /// Buffer pool, query, and config amplifiers added when those facts are available.
    /// </summary>
    private static List<AmplifierDefinition> PageiolatchAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD elevated — CPU pressure alongside I/O pressure",
            Boost = 0.1,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.15)
        },
        new()
        {
            Description = "Read latency > 20ms — confirmed disk I/O bottleneck",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("IO_READ_LATENCY_MS", out var io) && io.Value >= 20
        },
        new()
        {
            Description = "Memory grant waiters present — grants competing with buffer pool",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("MEMORY_GRANT_PENDING", out var mg) && mg.Value >= 1
        }
    ];

    /// <summary>
    /// LATCH_EX/LATCH_SH: in-memory page latch contention.
    /// Common causes: TempDB allocation contention, hot page updates,
    /// parallel insert into heaps or narrow indexes.
    /// </summary>
    private static List<AmplifierDefinition> LatchAmplifiers() =>
    [
        new()
        {
            Description = "TempDB usage elevated — latch contention likely on TempDB allocation pages",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("TEMPDB_USAGE", out var t) && t.BaseSeverity > 0
        },
        new()
        {
            Description = "CXPACKET significant — parallel operations amplifying latch contention",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "SOS_SCHEDULER_YIELD elevated — latch spinning contributing to CPU pressure",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.15)
        }
    ];

    /// <summary>
    /// BLOCKING_EVENTS: blocking confirmed by lock waits and deadlocks.
    /// </summary>
    private static List<AmplifierDefinition> BlockingEventsAmplifiers() =>
    [
        new()
        {
            Description = "Head blocker sleeping with open transaction — abandoned transaction pattern",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("BLOCKING_EVENTS", out var f)
                              && f.Metadata.GetValueOrDefault("sleeping_blocker_count") > 0
        },
        new()
        {
            Description = "Lock contention waits elevated — blocking visible in wait stats",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("LCK") && facts["LCK"].BaseSeverity >= 0.3
        },
        new()
        {
            Description = "Deadlocks also present — blocking escalating to deadlocks",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// BLOCKING_CHAIN: a reconstructed blocking pile-up, amplified by an abandoned apex
    /// transaction and by the cascade symptoms a deep/wide chain produces.
    /// </summary>
    private static List<AmplifierDefinition> BlockingChainAmplifiers() =>
    [
        new()
        {
            Description = "Apex head blocker is sleeping — abandoned transaction at the top of the chain",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("BLOCKING_CHAIN", out var f)
                              && f.Metadata.GetValueOrDefault("worst_apex_sleeping") > 0
        },
        new()
        {
            Description = "Deadlocks also present — chain blocking escalating to deadlocks",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0
        },
        new()
        {
            Description = "THREADPOOL waits present — chain victims pinning worker threads",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// RESOURCE_SEMAPHORE_QUERY_COMPILE: compile-gateway memory pressure. Corroborated by
    /// CPU signals (compilation is CPU-heavy), not by runtime-grant signals.
    /// </summary>
    private static List<AmplifierDefinition> ResourceSemaphoreQueryCompileAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD elevated — compilation competing for CPU",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.25)
        },
        new()
        {
            Description = "SQL Server CPU > 80% — compilation a measurable share of CPU load",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("CPU_SQL_PERCENT", out var cpu) && cpu.Value >= 80
        },
        new()
        {
            Description = "RESOURCE_SEMAPHORE also present — broad memory starvation, not isolated compile pressure",
            Boost = 0.2,
            Predicate = facts => facts.ContainsKey("RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// DEADLOCKS: deadlocks confirmed by blocking patterns.
    /// </summary>
    private static List<AmplifierDefinition> DeadlockAmplifiers() =>
    [
        new()
        {
            Description = "Blocking events also present — systemic contention pattern",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0
        },
        new()
        {
            Description = "Reader/writer lock waits present — RCSI could prevent some deadlocks",
            Boost = 0.3,
            Predicate = facts => (facts.ContainsKey("LCK_M_S") && facts["LCK_M_S"].BaseSeverity > 0)
                              || (facts.ContainsKey("LCK_M_IS") && facts["LCK_M_IS"].BaseSeverity > 0)
        },
        new()
        {
            Description = "Databases without RCSI — reader/writer isolation amplifying deadlocks",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("DB_CONFIG", out var db) && db.Metadata.GetValueOrDefault("rcsi_off_count") > 0
        }
    ];

    /// <summary>
    /// LCK (grouped general lock contention): confirmed by blocking reports and deadlocks.
    /// </summary>
    private static List<AmplifierDefinition> LckAmplifiers() =>
    [
        new()
        {
            Description = "Blocked process reports present — confirmed blocking events",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("BLOCKING_EVENTS") && facts["BLOCKING_EVENTS"].BaseSeverity > 0
        },
        new()
        {
            Description = "Deadlocks present — lock contention escalating to deadlocks",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("DEADLOCKS") && facts["DEADLOCKS"].BaseSeverity > 0
        },
        new()
        {
            Description = "THREADPOOL waits present — blocking causing thread exhaustion",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// DB_CONFIG: database misconfiguration amplified by related symptoms.
    /// RCSI-off amplifiers only fire when reader/writer lock contention is present —
    /// LCK_M_S (shared lock waits) and LCK_M_IS (intent-shared) are readers blocked
    /// by writers. RCSI eliminates these. Writer/writer conflicts (LCK_M_X, LCK_M_U)
    /// are NOT helped by RCSI and should not trigger this amplifier.
    /// </summary>
    private static List<AmplifierDefinition> DbConfigAmplifiers() =>
    [
        new()
        {
            Description = "I/O latency elevated — auto_shrink may be causing fragmentation and I/O pressure",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("IO_READ_LATENCY_MS", out var io) && io.BaseSeverity > 0
        },
        new()
        {
            Description = "LCK_M_S waits — readers blocked by writers, RCSI would eliminate shared lock waits",
            Boost = 0.5,
            Predicate = facts => facts.TryGetValue("DB_CONFIG", out var db)
                              && db.Metadata.GetValueOrDefault("rcsi_off_count") > 0
                              && facts.TryGetValue("LCK_M_S", out var lckS) && lckS.BaseSeverity > 0
        },
        new()
        {
            Description = "LCK_M_IS waits — intent-shared locks blocked by writers, RCSI would eliminate these",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("DB_CONFIG", out var db)
                              && db.Metadata.GetValueOrDefault("rcsi_off_count") > 0
                              && facts.TryGetValue("LCK_M_IS", out var lckIS) && lckIS.BaseSeverity > 0
        },
        new()
        {
            Description = "Deadlocks with reader/writer lock waits — RCSI eliminates reader/writer deadlocks",
            Boost = 0.4,
            Predicate = facts => facts.TryGetValue("DB_CONFIG", out var db)
                              && db.Metadata.GetValueOrDefault("rcsi_off_count") > 0
                              && facts.TryGetValue("DEADLOCKS", out var dl) && dl.BaseSeverity > 0
                              && (facts.TryGetValue("LCK_M_S", out var s) && s.BaseSeverity > 0
                               || facts.TryGetValue("LCK_M_IS", out var i) && i.BaseSeverity > 0)
        }
    ];

    /// <summary>
    /// DISK_SPACE: low disk space amplified by I/O activity and TempDB pressure.
    /// </summary>
    private static List<AmplifierDefinition> DiskSpaceAmplifiers() =>
    [
        new()
        {
            Description = "TempDB usage elevated — growing TempDB on a nearly full volume",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("TEMPDB_USAGE", out var t) && t.BaseSeverity > 0
        },
        new()
        {
            Description = "Query spills present — spills to disk on a nearly full volume",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("QUERY_SPILLS", out var s) && s.BaseSeverity > 0
        }
    ];

    /// <summary>
    /// CPU_SQL_PERCENT: CPU saturation confirmed by scheduler yields and parallelism.
    /// </summary>
    private static List<AmplifierDefinition> CpuSqlPercentAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD elevated — scheduler pressure confirms CPU saturation",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "SOS_SCHEDULER_YIELD", 0.25)
        },
        new()
        {
            Description = "CXPACKET significant — parallelism contributing to CPU load",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        }
    ];

    /// <summary>
    /// CPU_SPIKE: bursty CPU event (max >> average) confirmed by scheduler
    /// pressure, parallelism, or query spills during the spike.
    /// </summary>
    private static List<AmplifierDefinition> CpuSpikeAmplifiers() =>
    [
        new()
        {
            Description = "SOS_SCHEDULER_YIELD present — scheduler pressure during CPU spike",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("SOS_SCHEDULER_YIELD") && facts["SOS_SCHEDULER_YIELD"].BaseSeverity > 0
        },
        new()
        {
            Description = "CXPACKET significant — parallelism contributing to CPU spike",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
        },
        new()
        {
            Description = "THREADPOOL waits present — CPU spike causing thread exhaustion",
            Boost = 0.4,
            Predicate = facts => facts.ContainsKey("THREADPOOL") && facts["THREADPOOL"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// IO_READ_LATENCY_MS: read latency confirmed by PAGEIOLATCH waits.
    /// </summary>
    private static List<AmplifierDefinition> IoReadLatencyAmplifiers() =>
    [
        new()
        {
            Description = "PAGEIOLATCH waits elevated — buffer pool misses confirm I/O pressure",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "PAGEIOLATCH_SH", 0.10)
                              || HasSignificantWait(facts, "PAGEIOLATCH_EX", 0.10)
        }
    ];

    /// <summary>
    /// IO_WRITE_LATENCY_MS: write latency confirmed by WRITELOG waits.
    /// </summary>
    private static List<AmplifierDefinition> IoWriteLatencyAmplifiers() =>
    [
        new()
        {
            Description = "WRITELOG waits elevated — transaction log I/O bottleneck confirmed",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "WRITELOG", 0.05)
        }
    ];

    /// <summary>
    /// MEMORY_GRANT_PENDING: grant pressure confirmed by RESOURCE_SEMAPHORE waits and spills.
    /// </summary>
    private static List<AmplifierDefinition> MemoryGrantAmplifiers() =>
    [
        new()
        {
            Description = "RESOURCE_SEMAPHORE waits present — memory grant pressure in wait stats",
            Boost = 0.3,
            Predicate = facts => facts.ContainsKey("RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].BaseSeverity > 0
        },
        new()
        {
            Description = "Query spills present — queries running with insufficient memory grants",
            Boost = 0.2,
            Predicate = facts => facts.TryGetValue("QUERY_SPILLS", out var s) && s.BaseSeverity > 0
        }
    ];

    /// <summary>
    /// QUERY_SPILLS: spills confirmed by memory grant pressure.
    /// </summary>
    private static List<AmplifierDefinition> QuerySpillAmplifiers() =>
    [
        new()
        {
            Description = "Memory grant waiters present — insufficient memory for query grants",
            Boost = 0.3,
            Predicate = facts => facts.TryGetValue("MEMORY_GRANT_PENDING", out var mg) && mg.Value >= 1
        },
        new()
        {
            Description = "RESOURCE_SEMAPHORE waits — grant pressure visible in wait stats",
            Boost = 0.2,
            Predicate = facts => facts.ContainsKey("RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].BaseSeverity > 0
        }
    ];

    /// <summary>
    /// Checks if a wait type is present with at least the given fraction of period.
    /// </summary>
    private static bool HasSignificantWait(Dictionary<string, Fact> facts, string waitType, double minFraction)
    {
        return facts.TryGetValue(waitType, out var fact) && fact.Value >= minFraction;
    }

    /// <summary>
    /// Thresholds for wait types: (concerning, critical) as a FRACTION of the observed period, graded by
    /// <see cref="ApplyThresholdFormula"/> — 0.5 at concerning (the InferenceEngine root entry point),
    /// 1.0 at critical; a null critical saturates at concerning. Returns null for a wait type with no
    /// entry — the fact scores 0, stays a context fact, and can never root a story.
    ///
    /// <para><b>Every entry carries its measurement lineage (#3538 A5).</b> The population is the
    /// 2026-09 dogfood read: 43 SQL Server primaries on one production store class, 4 days of
    /// <c>wait_stats</c> bucketed into 1,075 server-4-hour windows, each wait type's
    /// <c>SUM(delta_wait_time_ms) ÷ (4 h)</c> — the same fraction-of-period this method grades, divided by
    /// the NOMINAL window; the collectors now divide by OBSERVED time (#3538 A2), so the fleet figures are
    /// the lower-bound reading of what a fully-collected window shows the scorer, and a partly-collected
    /// one reads higher. Percentiles are over the non-zero windows; "fires on N of 1,075" counts every
    /// window. Where the fleet does not exhibit a wait, the entry says "unmeasured" — an honest lineage
    /// includes what the data could not calibrate, and a bar on an absent wait is set by what the wait
    /// MEANS, not by where a fleet that never sees it sits.</para>
    ///
    /// <para><b>The method, from the alerting layer's bands (ServerHealthBands.cs, #3368):</b> the
    /// concerning bar sits at the top of the routine mode (≈ p99 of windows), so a typical window bands
    /// nothing and about one window in a hundred roots a story; the critical bar sits inside a measured
    /// empty interval — above every window measured — so nothing routine can saturate. A pair with a null
    /// critical is one the method did not need to ramp: either the bar is already at p99.9 (SOS), or the
    /// fleet never approaches it (the PAGEIOLATCH / CXPACKET / LCK_M_S / LCK_M_IS family), and lowering a
    /// bar on one fleet's silence would be calibrating to an absence.</para>
    ///
    /// <para><b>What the fraction is, and is not.</b> The numerator sums the wait time of every CONCURRENT
    /// task, so a fraction above 1.0 is legal and the same 0.25 means different things on 4 schedulers and
    /// on 64 (the measurement lane's A3 note, documented on the collectors). The bars here are calibrated
    /// on the fleet's RAW fraction — normalising by scheduler count would re-scale every measured figure
    /// and needs its own read, so it is deliberately not done here.</para>
    ///
    /// <para><b>The one measurably over-firing constant was WRITELOG.</b> Its inherited (0.10, null)
    /// saturated base 1.0 in 339 of 1,075 windows (31.5%) — a third of routine windows on a commit-heavy
    /// OLTP fleet read as a saturated log-flush finding. Nothing else in the table fired on more than 2
    /// windows in 1,075.</para>
    /// </summary>
    private static (double concerning, double? critical)? GetWaitThresholds(string waitType)
    {
        return waitType switch
        {
            // ── CPU pressure ──
            // Measured: p50 0.088, p90 0.335, p99 0.603, p99.9 0.744, max 0.835; 1 of 1,075 windows
            // reaches 0.75. The inherited bar sits at the 99.9th percentile of the fleet — kept as is.
            // No critical: SOS is the CPU story's root and reaches CRITICAL through its amplifiers
            // (CPU %, CXPACKET, THREADPOOL), not on its own fraction.
            "SOS_SCHEDULER_YIELD" => (0.75, null),
            // Dominated by the ScoreWaitFact gate (ThreadpoolMinWaitMsPerObservedHour = 0.25 of observed
            // time + 1 s average): any THREADPOOL fact that clears the gate has fraction >= 0.25 and
            // saturates here, so THREADPOOL's base is effectively 0 or 1.0 — deliberately: exhaustion is
            // an outage, not a gradient. Measured: max fraction 4.1e-4 (295 non-zero windows); the gate's
            // own lineage is on the constant. Not ramped with the other 0.01 entries because the gate
            // already refuses to let a trace score at all.
            "THREADPOOL"          => (0.01, null),

            // ── Memory pressure ──
            // Measured: SH p50 0.012, p90 0.047, p99 0.098, max 0.154 (10 windows >= 0.10, 0 >= 0.25);
            // EX p99 0.046, max 0.093. Neither reaches the inherited bar on this fleet — a buffer pool
            // that fits its working set. Conservative, not wrong; lineage only, not lowered on silence.
            "PAGEIOLATCH_SH"      => (0.25, null),
            "PAGEIOLATCH_EX"      => (0.25, null),
            // Unmeasured: RESOURCE_SEMAPHORE accrued ZERO wait time in 4 days on 43 servers, so the fleet
            // cannot place its concerning bar; it can only say what the inherited (0.01, null) did — base
            // 1.0 at 36 s of grant queueing per hour, on ANY trace — and that the ramp shape RS_QUERY_COMPILE
            // already carries is the honest one: 1% of observed time queued for a grant roots a story at
            // 0.5, and 10% (the Layer-3 cap-release bar in ScoreAll, "meaningful grant starvation")
            // saturates. The 0.01 floor is kept because a trace of RESOURCE_SEMAPHORE IS abnormal on a
            // healthy server (the fleet's zero says so); the ramp stops a trace from reading as a storm.
            "RESOURCE_SEMAPHORE"  => (0.01, 0.10),
            // Query-compile memory pressure — ramped: healthy servers see some compile-gateway waits, so
            // 1% of period is concerning but 10% is critical. Measured: 4 non-zero windows on 1 server,
            // max 2.2e-4 (a scheduled compile burst, ~3 s at the same hour daily) — under the floor.
            "RESOURCE_SEMAPHORE_QUERY_COMPILE" => (0.01, 0.10),

            // ── Parallelism (every CX* wait is grouped into CXPACKET by the collector) ──
            // Measured: 25 non-zero windows, max 0.0187 (CXPACKET) / 0.068 (CXCONSUMER) — this fleet runs
            // little parallelism, so the bar is unmeasured in the sense that matters. Inherited; the
            // Layer-3 tuning-class cap, not this bar, is what keeps parallelism out of the CRITICAL band.
            "CXPACKET"            => (0.25, null),

            // ── Log I/O ──
            // RE-DERIVED (#3538 A5). Inherited (0.10, null) saturated base 1.0 on 339 of 1,075 windows
            // (31.5%): p50 0.065, p90 0.163, p99 0.243, p99.9 0.302, max 0.313 — WRITELOG is this OLTP
            // fleet's steady-state commit cost, present in every window, and the bar sat below its median
            // times two. Concerning 0.25 ≈ p99: 10 of 1,075 windows (0.9%) reach 0.5 and root a story —
            // the top of the routine mode, the method's WARNING placement. Critical 0.50: above every
            // window measured (1.6x the max), and half of observed time spent in log-flush waits summed
            // across committers is a log that is genuinely not keeping up. The fleet's worst window
            // (0.313) scores 0.63 — a WARNING that roots, not a CRITICAL. The IO_WRITE_LATENCY_MS
            // amplifier's corroboration bar (HasSignificantWait 0.05) is a different question — "is
            // WRITELOG present enough to confirm a write-latency finding" — and is not moved by this.
            "WRITELOG"            => (0.25, 0.50),

            // ── Availability-group synchronous commit ──
            // NEW (#3538 A5). The second-largest wait on the fleet and, before this entry, invisible to the
            // engine: measured in 1,050 of 1,075 windows, p50 0.045, p90 0.129, p99 0.288, p99.9 0.474,
            // max 0.591; 14 windows >= 0.25. HADR_SYNC_COMMIT is the primary waiting for a synchronous
            // secondary to harden the log before a commit can return — the AG sibling of WRITELOG, and on
            // an AG fleet the larger half of commit latency. Concerning 0.30 ≈ p99 (≈ 1% of windows root);
            // critical 0.50 sits between p99.9 (0.474) and the max (0.591), so only the single worst
            // window measured saturates. Ramped, because every window carries some of it and a null
            // critical would make a routine 0.30 read the same as a 0.59 replica stall. The advice is
            // evidence-gated and names the counter-objective (FactAdvice): synchronous commit is a
            // durability policy, and the remediation is never "switch to async".
            "HADR_SYNC_COMMIT"    => (0.30, 0.50),

            // ── Lock waits: serializable / repeatable-read range-lock modes ──
            // RAMPED (#3538 A5) from the inherited (0.01, null), which saturated base 1.0 on any trace — 36 s
            // of range-lock waiting per hour read as a saturated finding. A range lock IS abnormal (it
            // means SERIALIZABLE, which nothing should be running by accident), so the 0.01 floor stays;
            // the ramp to 0.10 lets a trace root at 0.5 while only sustained range-locking saturates.
            // Measured: RS_U max 0.00118 (553 non-zero windows), RS_S max 0.00032 (14 windows) — both
            // under the floor; RIn_* and RX_* did not reach the measurement's top-70 cut, so their bars
            // are unmeasured and inherited by shape from RS_*.
            "LCK_M_RS_S"  => (0.01, 0.10),
            "LCK_M_RS_U"  => (0.01, 0.10),
            "LCK_M_RIn_NL" => (0.01, 0.10),
            "LCK_M_RIn_S" => (0.01, 0.10),
            "LCK_M_RIn_U" => (0.01, 0.10),
            "LCK_M_RIn_X" => (0.01, 0.10),
            "LCK_M_RX_S"  => (0.01, 0.10),
            "LCK_M_RX_U"  => (0.01, 0.10),
            "LCK_M_RX_X"  => (0.01, 0.10),

            // ── Reader/writer blocking locks (the RCSI signal) ──
            // Measured: LCK_M_S max 8.6e-4 (971 windows), LCK_M_IS max 0.0117 (104 windows) — never within
            // 4x of the inherited bar. Conservative; lineage only.
            "LCK_M_S"  => (0.05, null),
            "LCK_M_IS" => (0.05, null),

            // ── General lock contention (X, U, IX, SIX, BU, ... grouped into LCK by the collector) ──
            // Measured per constituent, not as the grouped sum the scorer sees: LCK_M_U max 0.129 (1 window
            // >= 0.10), LCK_M_IX max 0.048, LCK_M_X max 0.020. The grouped fraction is their sum, so at most
            // a handful of windows reach 0.10 on this fleet. Inherited; lineage only.
            "LCK" => (0.10, null),

            // ── Schema locks — DDL, index rebuilds ──
            // RAMPED (#3538 A5) from (0.01, null) for the same reason as the range locks: a trace of SCH_M
            // during maintenance saturated a finding. Unmeasured: below the measurement's top-70 cut
            // (< 1.7e-4 in every window).
            "SCH_M" => (0.01, 0.10),

            // ── Latch contention (page latch, not I/O latch — in-memory hot pages, tempdb allocation) ──
            // Measured: LATCH_EX p99 0.0051, p99.9 0.351, max 0.378 — 2 of 1,075 windows (0.19%) reach the
            // inherited 0.25, a heavy tail on an otherwise silent wait. At the bar already; kept. LATCH_SH
            // did not reach the top-70 cut: unmeasured, inherited.
            "LATCH_EX" => (0.25, null),
            "LATCH_SH" => (0.25, null),

            // ── Benign by measurement: no bar, spelled out so nobody adds one ──
            // PREEMPTIVE_OS_QUERYREGISTRY is the fleet's #7 wait by fraction (present in ALL 1,075 windows,
            // p50 0.016, p90 0.067, p99 0.114, max 0.175) and is fleet-UNIFORM: ~152,881 waiting tasks per
            // hour per server (min 10,011, max 293,171), ~1 ms each — about 42 registry queries a second on
            // every server around the clock, unrelated to workload and not this product's collectors (the
            // agent-status collector's dm_server_services reads are ~36/hr, 0.02% of it). Uniformity at
            // that rate is the managed platform's own service/state polling. A bar on it either never
            // fires or fires on platform noise, so it has NO threshold — the same outcome the default arm
            // below gives every unlisted wait, written as an entry so the measurement travels with the
            // decision. (This is the scorer's benign tier; it is unrelated to sp_HealthParser's
            // #ignore_waits copy in SystemHealthSignificance, which gates discrete >= 500 ms XE wait_info
            // events that a 1 ms registry call can never produce, and to the collector-side
            // IgnoredWaitDefaults, which decides what is STORED and is a collection-policy list.)
            "PREEMPTIVE_OS_QUERYREGISTRY" => null,

            _ => null
        };
    }
}

/// <summary>
/// An amplifier definition: a named predicate that boosts severity when matched.
/// </summary>
internal sealed class AmplifierDefinition
{
    public string Description { get; set; } = string.Empty;
    public double Boost { get; set; }
    public Func<Dictionary<string, Fact>, bool> Predicate { get; set; } = _ => false;
}
