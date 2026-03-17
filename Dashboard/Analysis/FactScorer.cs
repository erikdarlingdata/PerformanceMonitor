using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitorDashboard.Analysis;

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
                "perfmon" => ScorePerfmonFact(fact),
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
            .ToDictionary(f => f.Key, f => f);

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
            _ => 0.0
        };
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
            _ => 0.0
        };
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
    /// Scores TempDB usage. Value is usage fraction (reserved / total space).
    /// </summary>
    private static double ScoreTempDbFact(Fact fact)
    {
        return fact.Key switch
        {
            // TempDB usage: concerning at 75%, critical at 90%
            "TEMPDB_USAGE" => ApplyThresholdFormula(fact.Value, 0.75, 0.90),
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores memory grant facts. Only MEMORY_GRANT_PENDING (from resource semaphore) for now.
    /// </summary>
    private static double ScoreMemoryFact(Fact fact)
    {
        return fact.Key switch
        {
            // Grant waiters: concerning at 1, critical at 5
            "MEMORY_GRANT_PENDING" => ApplyThresholdFormula(fact.Value, 1, 5),
            _ => 0.0
        };
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
            _ => 0.0
        };
    }

    /// <summary>
    /// Scores perfmon counter facts. PLE is the classic memory pressure indicator.
    /// </summary>
    private static double ScorePerfmonFact(Fact fact)
    {
        return fact.Key switch
        {
            // PLE: lower is worse. Invert: concerning < 300, critical < 60
            "PERFMON_PLE" when fact.Value <= 0 => 1.0,
            "PERFMON_PLE" when fact.Value < 60 => 1.0,
            "PERFMON_PLE" when fact.Value < 300 => 0.5 + 0.5 * (300 - fact.Value) / 240,
            "PERFMON_PLE" => 0.0,
            _ => 0.0
        };
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

    /// <summary>
    /// Scores anomaly facts based on deviation from baseline.
    /// At 2σ → 0.5, at 4σ → 1.0. Higher deviations are more severe.
    /// For count-based anomalies (blocking/deadlock spikes), uses ratio instead.
    /// </summary>
    private static double ScoreAnomalyFact(Fact fact)
    {
        if (fact.Key.StartsWith("ANOMALY_CPU_SPIKE") || fact.Key.StartsWith("ANOMALY_READ_LATENCY")
            || fact.Key.StartsWith("ANOMALY_WRITE_LATENCY"))
        {
            // Deviation-based scoring: 2σ = 0.5, 4σ = 1.0
            var deviation = fact.Metadata.GetValueOrDefault("deviation_sigma");
            var confidence = fact.Metadata.GetValueOrDefault("confidence", 1.0);
            if (deviation < 2.0) return 0.0;
            var base_score = 0.5 + 0.5 * Math.Min((deviation - 2.0) / 2.0, 1.0);
            return base_score * confidence;
        }

        if (fact.Key.StartsWith("ANOMALY_WAIT_"))
        {
            // Ratio-based scoring: 5x = 0.5, 20x = 1.0
            var ratio = fact.Metadata.GetValueOrDefault("ratio");
            if (ratio < 5) return 0.0;
            return 0.5 + 0.5 * Math.Min((ratio - 5.0) / 15.0, 1.0);
        }

        if (fact.Key.StartsWith("ANOMALY_BLOCKING_SPIKE") || fact.Key.StartsWith("ANOMALY_DEADLOCK_SPIKE"))
        {
            // Ratio-based: 3x = 0.5, 10x = 1.0
            var ratio = fact.Metadata.GetValueOrDefault("ratio");
            if (ratio < 3) return 0.0;
            return 0.5 + 0.5 * Math.Min((ratio - 3.0) / 7.0, 1.0);
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
            "DEADLOCKS" => DeadlockAmplifiers(),
            "LCK" => LckAmplifiers(),
            "CPU_SQL_PERCENT" => CpuSqlPercentAmplifiers(),
            "CPU_SPIKE" => CpuSpikeAmplifiers(),
            "IO_READ_LATENCY_MS" => IoReadLatencyAmplifiers(),
            "IO_WRITE_LATENCY_MS" => IoWriteLatencyAmplifiers(),
            "MEMORY_GRANT_PENDING" => MemoryGrantAmplifiers(),
            "QUERY_SPILLS" => QuerySpillAmplifiers(),
            "PERFMON_PLE" => PleAmplifiers(),
            "DB_CONFIG" => DbConfigAmplifiers(),
            "DISK_SPACE" => DiskSpaceAmplifiers(),
            _ => []
        };
    }

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
    /// THREADPOOL: thread exhaustion confirmed by parallelism pressure.
    /// Blocking and config amplifiers added later.
    /// </summary>
    private static List<AmplifierDefinition> ThreadpoolAmplifiers() =>
    [
        new()
        {
            Description = "CXPACKET significant — parallel queries consuming thread pool",
            Boost = 0.2,
            Predicate = facts => HasSignificantWait(facts, "CXPACKET", 0.10)
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
    /// PLE: memory pressure confirmed by PAGEIOLATCH and RESOURCE_SEMAPHORE.
    /// </summary>
    private static List<AmplifierDefinition> PleAmplifiers() =>
    [
        new()
        {
            Description = "PAGEIOLATCH waits present — buffer pool misses confirm memory pressure",
            Boost = 0.3,
            Predicate = facts => HasSignificantWait(facts, "PAGEIOLATCH_SH", 0.10)
                              || HasSignificantWait(facts, "PAGEIOLATCH_EX", 0.10)
        },
        new()
        {
            Description = "RESOURCE_SEMAPHORE waits — memory grants competing with buffer pool",
            Boost = 0.2,
            Predicate = facts => facts.ContainsKey("RESOURCE_SEMAPHORE") && facts["RESOURCE_SEMAPHORE"].BaseSeverity > 0
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
internal class AmplifierDefinition
{
    public string Description { get; set; } = string.Empty;
    public double Boost { get; set; }
    public Func<Dictionary<string, Fact>, bool> Predicate { get; set; } = _ => false;
}
