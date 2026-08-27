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
///
/// Formula: severity = min(base * (1.0 + sum(amplifiers)), 2.0)
/// </summary>
public class FactScorer
{
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
                _ => 0.0
            };
        }

        // Build lookup for amplifier evaluation (include context facts that amplifiers reference)
        var contextSources = new HashSet<string>
            { "config", "cpu", "io", "tempdb", "memory", "queries", "perfmon",
              "database_config", "jobs", "sessions", "disk", "bad_actor", "anomaly" };
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
        // self-gating: ScoreWaitFact requires >= 1hr total AND >= 1s avg before THREADPOOL scores at
        // all, so BaseSeverity > 0 there already means real exhaustion — keep the presence check.
        // SOS_SCHEDULER_YIELD (0.75, null) and RESOURCE_SEMAPHORE (0.01, null) have NO minimum guard,
        // so their BaseSeverity > 0 fires on ANY trace of the wait; and SOS physically co-occurs with
        // high CXPACKET (parallel workers yield -> SOS) and is emitted for any delta_wait_time_ms > 0,
        // so a trivial SOS (e.g. 500ms over an hour) would release the cap on exactly the busy servers
        // the cap targets — re-admitting the CXPACKET=CRITICAL noise the cap exists to kill. Gate
        // SOS/RS on SIGNIFICANCE (fraction of period) via the same HasSignificantWait helper the
        // amplifiers use, not on mere presence: SOS at 0.25 (matches the CXPACKET SOS amplifier bar);
        // RS at 0.10 (RESOURCE_SEMAPHORE has no HasSignificantWait amplifier bar, so pick a bar here —
        // 0.10 of period is meaningful grant starvation). Caps numeric Severity only — SeverityBand is
        // derived from it downstream, so a capped fact stays in WARNING without a separate band edit
        // and Lite parity is preserved.
        var impactPeerCoFired =
            (factsByKey.TryGetValue("THREADPOOL", out var tpPeer) && tpPeer.BaseSeverity > 0)
            || HasSignificantWait(factsByKey, "SOS_SCHEDULER_YIELD", 0.25)
            || HasSignificantWait(factsByKey, "RESOURCE_SEMAPHORE", 0.10);

        if (!impactPeerCoFired)
        {
            foreach (var fact in facts)
            {
                if (IsTuningClassKey(fact.Key))
                    fact.Severity = Math.Min(fact.Severity, TuningClassSeverityCeiling);
            }
        }
    }

    /// <summary>
    /// Scores a wait fact using the fraction-of-period formula.
    /// Some waits have absolute minimum thresholds to filter out background noise.
    /// </summary>
    private static double ScoreWaitFact(Fact fact)
    {
        var fraction = fact.Value;
        if (fraction <= 0) return 0.0;

        // THREADPOOL: require both meaningful total wait time AND meaningful average.
        // Tiny amounts are normal thread pool grow/shrink housekeeping, not exhaustion.
        if (fact.Key == "THREADPOOL")
        {
            var waitTimeMs = fact.Metadata.GetValueOrDefault("wait_time_ms");
            var avgMs = fact.Metadata.GetValueOrDefault("avg_ms_per_wait");
            if (waitTimeMs < 3_600_000 || avgMs < 1_000) return 0.0;
        }

        // PAGELATCH_UP (tempdb allocation contention) is scored on ABSOLUTE wait_time_ms, not
        // fraction-of-period, because its source — the Dashboard's report.tempdb_contention_analysis
        // contention_level CASE — trips on an absolute PAGELATCH_UP total (install/47:2515:
        // pagelatch_up_ms > 10000 -> "MEDIUM - PAGELATCH_UP contention"). PAGELATCH_UP is the canonical
        // PFS/GAM/SGAM allocation-page latch (the fix is add tempdb data files / TF 1118), and the source
        // reads the SAME server-wide wait_stats this fact is built from, so scoring the wait total is a
        // faithful port. Flat 0.5 (MEDIUM) at the source's single PAGELATCH_UP tier — there is no higher
        // band for it there; the view's CRITICAL "allocation contention" comes from a tempdb-scoped
        // dm_os_waiting_tasks flag (allocation_contention_warning, install/47:2503) that is NOT carried in
        // this fact. Absolute-ms is consistent with the THREADPOOL gate just above (the analysis window is
        // hours-scale; the source's window is 1 hour).
        if (fact.Key == "PAGELATCH_UP")
            return fact.Metadata.GetValueOrDefault("wait_time_ms") > 10_000 ? 0.5 : 0.0;

        var thresholds = GetWaitThresholds(fact.Key);
        if (thresholds == null) return 0.0;

        return ApplyThresholdFormula(fraction, thresholds.Value.concerning, thresholds.Value.critical);
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
            // Blocking: concerning >10/hr, critical >50/hr
            "BLOCKING_EVENTS" => ApplyThresholdFormula(value, 10, 50),
            // Deadlocks: concerning >5/hr (no critical — any sustained deadlocking is bad)
            "DEADLOCKS" => ApplyThresholdFormula(value, 5, null),
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
    // DefaultRatioThreshold; these are HONEST per-second-scale starting values — CALIBRATE ON SQL2025/HAMMERDB.
    private const double WaitProfileRatioFloor = 4.0;
    private const double WaitProfileRatioSpan = 8.0;

    // Bounded-metric low-quality fallback ramp (see the z-score anomaly arm). When the quality gate fires
    // on a thin baseline (baseline_low_quality=1) the stored deviation_sigma is the real (small) z that the
    // 2σ gate would zero out — so grade off the absolute exceedance (peak ÷ the absolute-fallback bar, which
    // is >= 1.0 on a fire) instead: floor 0.5 AT the bar (clears InferenceEngine's 0.5 entry-point), ramping
    // to 1.0 at 2× the bar. Sensible default — CALIBRATE ON SQL2025/HAMMERDB.
    private const double LowQualityFallbackSpan = 1.0;

    // Layer-3 tuning-class severity ceiling (see ScoreAll). Parallelism/anomaly signals describe a tuning
    // opportunity, not an outage — their FINAL severity is capped here (bands are >= 1.5 CRITICAL) unless an
    // impact peer co-fired. 1.49 keeps a capped fact in the WARNING band without touching SeverityBand.
    private const double TuningClassSeverityCeiling = 1.49;

    /// <summary>
    /// Tuning-class keys whose FINAL severity is capped at the WARNING ceiling (Layer 3) unless an
    /// impact peer co-fired: parallelism (CXPACKET/CXCONSUMER), excessive-DOP queries, and every
    /// anomaly fact. Today only CXPACKET can exceed the ceiling on amplifiers (ANOMALY_* and
    /// QUERY_HIGH_DOP already max at 1.0) — the rest is forward-safety as those ramps evolve.
    /// </summary>
    private static bool IsTuningClassKey(string key) =>
        key is "CXPACKET" or "CXCONSUMER" or "QUERY_HIGH_DOP"
        || key.StartsWith("ANOMALY_", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Scores anomaly facts based on deviation from baseline.
    /// At 2σ → 0.5, at 4σ → 1.0. Higher deviations are more severe.
    /// For count-based anomalies (blocking/deadlock spikes), uses ratio instead.
    /// </summary>
    private static double ScoreAnomalyFact(Fact fact)
    {
        if (fact.Key.StartsWith("ANOMALY_CPU_SPIKE", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_READ_LATENCY", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_WRITE_LATENCY", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_BATCH_REQUESTS", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_SESSION_SPIKE", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_QUERY_DURATION", StringComparison.OrdinalIgnoreCase)
            || fact.Key.StartsWith("ANOMALY_MEMORY_PRESSURE", StringComparison.OrdinalIgnoreCase))
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
            var anchor = fact.Metadata.GetValueOrDefault("fire_threshold", 2.0);
            if (anchor <= 0) anchor = 2.0;
            if (deviation < anchor) return 0.0;
            var base_score = 0.5 + 0.5 * Math.Min((deviation - anchor) / anchor, 1.0);
            return base_score * confidence;
        }

        // Wait-profile (the one ANOMALY_WAIT_PROFILE fact) — must precede the generic ANOMALY_WAIT_
        // branch below, which it also prefix-matches. The ratio is now the HONEST per-second scale
        // (peak window all-types ms/sec ÷ baseline mean), so the ramp is far smaller than the old
        // 5×/20× that was calibrated to a ~240×-inflated per-hour-vs-per-interval input: 4× → 0.5,
        // saturating to 1.0 at 12×. Starting values matching the detectors' DefaultRatioThreshold;
        // CALIBRATE ON THE SQL2025/HAMMERDB BOX.
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
            _ => []
        };
    }

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
    /// Default thresholds for wait types (fraction of examined period).
    /// Returns null for unrecognized waits — they get severity 0.
    /// </summary>
    private static (double concerning, double? critical)? GetWaitThresholds(string waitType)
    {
        return waitType switch
        {
            // CPU pressure
            "SOS_SCHEDULER_YIELD" => (0.75, null),
            "THREADPOOL"          => (0.01, null),

            // Memory pressure
            "PAGEIOLATCH_SH"      => (0.25, null),
            "PAGEIOLATCH_EX"      => (0.25, null),
            "RESOURCE_SEMAPHORE"  => (0.01, null),
            // Query-compile memory pressure — ramped: healthy servers see some compile-gateway
            // waits, so 1% of period is concerning but 10% is critical.
            "RESOURCE_SEMAPHORE_QUERY_COMPILE" => (0.01, 0.10),

            // Parallelism (CXCONSUMER is grouped into CXPACKET by collector)
            "CXPACKET"            => (0.25, null),

            // Log I/O
            "WRITELOG"            => (0.10, null),

            // Lock waits — serializable/repeatable read lock modes
            "LCK_M_RS_S"  => (0.01, null),
            "LCK_M_RS_U"  => (0.01, null),
            "LCK_M_RIn_NL" => (0.01, null),
            "LCK_M_RIn_S" => (0.01, null),
            "LCK_M_RIn_U" => (0.01, null),
            "LCK_M_RIn_X" => (0.01, null),
            "LCK_M_RX_S"  => (0.01, null),
            "LCK_M_RX_U"  => (0.01, null),
            "LCK_M_RX_X"  => (0.01, null),

            // Reader/writer blocking locks
            "LCK_M_S"  => (0.05, null),
            "LCK_M_IS" => (0.05, null),

            // General lock contention (grouped X, U, IX, SIX, BU, etc.)
            "LCK" => (0.10, null),

            // Schema locks — DDL operations, index rebuilds
            "SCH_M" => (0.01, null),

            // Latch contention — page latch (not I/O latch) indicates
            // in-memory contention, often TempDB allocation or hot pages
            "LATCH_EX" => (0.25, null),
            "LATCH_SH" => (0.25, null),

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
