using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorDashboard.Tests;

/// <summary>
/// Tests FactScorer Layer 1 (base severity) and Layer 2 (amplifiers).
/// Validates threshold formulas, amplifier firing, and severity capping.
/// </summary>
public class FactScorerTests
{
    /* ── Threshold formula unit tests ── */

    [Theory]
    [InlineData(0.0, 0.25, null, 0.0)]        // Zero → 0.0
    [InlineData(0.125, 0.25, null, 0.5)]       // Half of concerning → 0.5
    [InlineData(0.25, 0.25, null, 1.0)]        // At concerning (no critical) → 1.0
    [InlineData(0.50, 0.25, null, 1.0)]        // Above concerning (no critical) → capped at 1.0
    [InlineData(0.0, 0.25, 0.75, 0.0)]         // Zero → 0.0
    [InlineData(0.125, 0.25, 0.75, 0.25)]      // Half of concerning → 0.25
    [InlineData(0.25, 0.25, 0.75, 0.5)]        // At concerning → 0.5
    [InlineData(0.50, 0.25, 0.75, 0.75)]       // Midway → 0.75
    [InlineData(0.75, 0.25, 0.75, 1.0)]        // At critical → 1.0
    [InlineData(1.00, 0.25, 0.75, 1.0)]        // Above critical → 1.0
    public void ApplyThresholdFormula_ReturnsExpected(
        double value, double concerning, double? critical, double expected)
    {
        var result = FactScorer.ApplyThresholdFormula(value, concerning, critical);
        Assert.Equal(expected, result, precision: 4);
    }

    /* ── Unknown wait types ── */

    [Fact]
    public void Score_UnknownWaitType_GetsSeverityZero()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "UNKNOWN_WAIT_XYZ", Value = 0.50 }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity);
    }

    /* ── Wait-profile anomaly scoring (change 1) ── */

    // The shared ANOMALY_WAIT_PROFILE arm scores on the HONEST per-second ratio: below 4x → 0, 4x →
    // 0.5, saturating to 1.0 at 12x. Same shared FactScorer as Lite — pinned here for parity.
    [Theory]
    [InlineData(3.0, 0.0)]
    [InlineData(4.0, 0.5)]
    [InlineData(12.0, 1.0)]
    [InlineData(100.0, 1.0)]
    public void Score_WaitProfile_RampsFromHonestRatio(double ratio, double expected)
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_WAIT_PROFILE",
            Metadata = new() { ["ratio"] = ratio }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.Equal(expected, fact.BaseSeverity, precision: 4);
    }

    [Fact]
    public void WaitProfile_HasAdviceBlock()
    {
        var advice = FactAdvice.GetForFactKey("ANOMALY_WAIT_PROFILE");
        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }

    /* ── Bounded-metric low-quality (thin-baseline) fallback scoring (finding 1) ── */

    // When the quality gate fires on a thin/untrustworthy baseline (baseline_low_quality=1) the stored
    // deviation_sigma is the real (small) z — the 2σ gate would zero it and InferenceEngine would drop the
    // finding. The scorer grades off the absolute exceedance (peak ÷ the fallback bar) instead: floor 0.5
    // AT the bar, ramping to 1.0 at 2× the bar. Same shared FactScorer as Lite — pinned here for parity.
    [Theory]
    [InlineData("ANOMALY_CPU_SPIKE", 1.0, 0.5)]        // exactly at the bar → the 0.5 floor
    [InlineData("ANOMALY_MEMORY_PRESSURE", 1.25, 0.625)]
    [InlineData("ANOMALY_READ_LATENCY", 1.5, 0.75)]    // 1.5× the bar → midway
    [InlineData("ANOMALY_WRITE_LATENCY", 2.0, 1.0)]    // 2× the bar → saturated
    [InlineData("ANOMALY_BATCH_REQUESTS", 5.0, 1.0)]   // far past → clamped to 1.0
    public void Score_LowQualityFallback_GradesOffExceedance_NotSigma(
        string key, double exceedance, double expected)
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = key,
            Metadata = new()
            {
                ["deviation_sigma"] = 0.7,          // a real, sub-2σ z the gate must ignore
                ["baseline_low_quality"] = 1.0,
                ["fallback_exceedance"] = exceedance,
                ["confidence"] = 1.0
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.Equal(expected, fact.BaseSeverity, precision: 4);
    }

    // The finding's exact regression: memory 96% on a thin baseline (95% fallback bar) used to score 0
    // (small z → 2σ gate → 0 → InferenceEngine drops it → NO finding). It must now clear the 0.5 entry.
    [Fact]
    public void Score_Memory96PercentOnThinBaseline_ClearsInferenceEntryPoint()
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_MEMORY_PRESSURE",
            Metadata = new()
            {
                ["deviation_sigma"] = 0.4,
                ["baseline_low_quality"] = 1.0,
                ["fallback_exceedance"] = 96.0 / 95.0,
                ["confidence"] = 1.0
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.True(fact.BaseSeverity >= 0.5, "a thin-baseline memory-96% fallback must surface, not vanish");
    }

    // The trustworthy z-path is unchanged: with baseline_low_quality=0 a sub-2σ deviation still scores 0.
    [Fact]
    public void Score_TrustworthyBaseline_SubTwoSigma_StillScoresZero()
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_MEMORY_PRESSURE",
            Metadata = new()
            {
                ["deviation_sigma"] = 1.5,
                ["baseline_low_quality"] = 0.0,
                ["fallback_exceedance"] = 0.0,
                ["confidence"] = 1.0
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.Equal(0.0, fact.BaseSeverity, precision: 4);
    }

    /* ── Layer 2: Amplifier tests ── */

    [Fact]
    public void Amplifier_SeverityCappedAt2()
    {
        // Synthetic: create a fact set where amplifiers would push past 2.0
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },           // base = 1.0
            new() { Source = "waits", Key = "SOS_SCHEDULER_YIELD", Value = 0.50 }, // > 25% threshold
            new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,           // real thread exhaustion
                Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } },  // 2h total, 3.6s avg
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },          // bad CTFP
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },        // bad MAXDOP
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");

        // base 1.0 * (1.0 + 0.3 SOS + 0.4 THREADPOOL + 0.3 CTFP + 0.2 MAXDOP) = 2.2 → capped at 2.0.
        // (SOS + THREADPOOL are impact peers, so the Layer-3 tuning cap is released here.)
        Assert.True(cx.Severity <= 2.0, "Severity should never exceed 2.0");
        Assert.Equal(2.0, cx.Severity);
    }

    /* ── Layer 3: tuning-class severity cap + impact escalation (change 4) ── */

    // Parallelism is a tuning opportunity, not an outage. CXPACKET saturates its base to 1.0 and
    // non-impact amplifiers (CTFP + MAXDOP + high-DOP queries) push the Layer-2 product to 1.7, which
    // WOULD land in the CRITICAL band (>= 1.5). With no impact peer co-firing, Layer 3 caps it at the
    // 1.49 WARNING ceiling. Same shared FactScorer as Lite — pinned here for parity.
    [Fact]
    public void Score_CxPacketAlone_CappedAtWarningCeiling()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },        // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },       // +0.3 (not an impact peer)
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },     // +0.2 (not an impact peer)
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },  // +0.2 (not an impact peer)
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        // Layer-2 product = 1.0 * (1 + 0.3 + 0.2 + 0.2) = 1.7; Layer 3 caps it to 1.49.
        Assert.Equal(1.49, cx.Severity, precision: 4);
        Assert.True(cx.Severity < 1.5, "parallelism alone must stay in the WARNING band, never CRITICAL");
    }

    // Escape hatch: when an impact-bearing peer co-fires (THREADPOOL = real thread exhaustion) the cap
    // is released and CXPACKET keeps its full amplified severity into the CRITICAL band.
    [Fact]
    public void Score_CxPacketWithThreadpool_CapReleased()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },        // base 1.0
            new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,        // real thread exhaustion (+0.4 on CXPACKET)
                Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } },
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },       // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        // 1.0 * (1 + 0.4 THREADPOOL + 0.3 CTFP) = 1.7, uncapped because an impact peer co-fired.
        Assert.Equal(1.7, cx.Severity, precision: 4);
        Assert.True(cx.Severity >= 1.5, "an impact peer co-firing releases the cap into CRITICAL");
    }

    // REGRESSION (escape-hatch significance gate): the cap-release peers SOS_SCHEDULER_YIELD and
    // RESOURCE_SEMAPHORE must be SIGNIFICANT to release the cap, not merely present. Neither has a
    // minimum guard in ScoreWaitFact, so BaseSeverity > 0 fires on any trace of the wait — and SOS
    // physically co-occurs with high CXPACKET (parallel workers yield -> SOS). A trivial SOS used to
    // release the cap on exactly the busy servers the cap targets, re-admitting CXPACKET=CRITICAL
    // noise. These pin the significance bars (SOS 0.25, RS 0.10) the escape hatch now uses. Same shared
    // FactScorer as Lite — pinned here for parity.

    // A TRIVIAL SOS (0.1% of period, far below the 0.25 bar) must NOT release the cap: CXPACKET stays
    // capped at the WARNING ceiling even though a sub-threshold SOS wait is present.
    [Fact]
    public void Score_CxPacketWithTrivialSos_CapHolds()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },           // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },          // +0.3
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },        // +0.2
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },     // +0.2
            new() { Source = "waits", Key = "SOS_SCHEDULER_YIELD", Value = 0.001 }, // TRIVIAL — 0.1%, < 0.25 bar
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        // Trivial SOS neither amplifies CXPACKET (amp bar is 0.25) nor releases the cap: 1.7 -> 1.49.
        Assert.Equal(1.49, cx.Severity, precision: 4);
        Assert.True(cx.Severity <= 1.49, "a trivial SOS must NOT release the tuning cap");
    }

    // A SIGNIFICANT SOS (30% of period, >= the 0.25 bar) DOES release the cap: CXPACKET reaches CRITICAL.
    [Fact]
    public void Score_CxPacketWithSignificantSos_CapReleased()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },           // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },          // +0.3
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },        // +0.2
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },     // +0.2
            new() { Source = "waits", Key = "SOS_SCHEDULER_YIELD", Value = 0.30 }, // SIGNIFICANT — >= 0.25 bar
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        // Significant SOS also fires the CXPACKET SOS amplifier (+0.3): 1.0*(1+0.3+0.3+0.2+0.2)=2.0, uncapped.
        Assert.Equal(2.0, cx.Severity, precision: 4);
        Assert.True(cx.Severity >= 1.5, "a significant SOS releases the cap into CRITICAL");
    }

    // A TRIVIAL RESOURCE_SEMAPHORE (0.1% of period, below the 0.10 bar) must NOT release the cap. RS
    // never amplifies CXPACKET, so pre-cap CXPACKET is 1.7 either way — this isolates the escape hatch.
    [Fact]
    public void Score_CxPacketWithTrivialResourceSemaphore_CapHolds()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },            // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },           // +0.3
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },         // +0.2
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },      // +0.2
            new() { Source = "waits", Key = "RESOURCE_SEMAPHORE", Value = 0.001 }, // TRIVIAL — 0.1%, < 0.10 bar
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        Assert.Equal(1.49, cx.Severity, precision: 4);
        Assert.True(cx.Severity <= 1.49, "a trivial RESOURCE_SEMAPHORE must NOT release the tuning cap");
    }

    // A SIGNIFICANT RESOURCE_SEMAPHORE (15% of period, >= the 0.10 bar) DOES release the cap. RS does
    // not amplify CXPACKET, so the pre-cap 1.7 passes through uncapped — the escape hatch in isolation.
    [Fact]
    public void Score_CxPacketWithSignificantResourceSemaphore_CapReleased()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },           // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },          // +0.3
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },        // +0.2
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },     // +0.2
            new() { Source = "waits", Key = "RESOURCE_SEMAPHORE", Value = 0.15 }, // SIGNIFICANT — >= 0.10 bar
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        // RS does not amplify CXPACKET; the pre-cap 1.7 is released unchanged.
        Assert.Equal(1.7, cx.Severity, precision: 4);
        Assert.True(cx.Severity >= 1.5, "a significant RESOURCE_SEMAPHORE releases the cap into CRITICAL");
    }

    // THREADPOOL is the impact-bearing escalation path and is NEVER capped. The raised CXPACKET
    // amplifier (+0.5) and the new runnable-queue amplifier (+0.5, off the RUNNABLE_TASKS context fact)
    // drive a genuine parallelism -> thread-exhaustion meltdown to CRITICAL through THREADPOOL.
    [Fact]
    public void Score_Threadpool_ReachesCritical_ViaCxPacketAndRunnableQueue()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,        // base 1.0
                Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } },
            new() { Source = "waits", Key = "CXPACKET", Value = 0.40 },        // >= 0.10 -> raised amp +0.5
            new() { Source = "cpu", Key = "RUNNABLE_TASKS", Value = 48,        // runnable_tasks_warning set -> amp +0.5
                Metadata = new() { ["total_runnable_tasks"] = 48, ["runnable_tasks_warning"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        var tp = facts.First(f => f.Key == "THREADPOOL");
        // 1.0 * (1 + 0.5 CXPACKET + 0.5 runnable-queue) = 2.0, uncapped (THREADPOOL is an impact key).
        Assert.Equal(2.0, tp.Severity, precision: 4);
        Assert.True(tp.Severity >= 1.5, "THREADPOOL must reach CRITICAL on a real meltdown");

        var cxAmp = tp.AmplifierResults.First(a => a.Description.Contains("CXPACKET"));
        Assert.True(cxAmp.Matched);
        Assert.Equal(0.5, cxAmp.Boost);

        var rqAmp = tp.AmplifierResults.First(a => a.Description.Contains("Runnable-task queue"));
        Assert.True(rqAmp.Matched);
        Assert.Equal(0.5, rqAmp.Boost);
    }

    // The runnable-queue amplifier only fires on genuine scheduler pressure: a shallow runnable queue
    // (below the trip level) does not boost THREADPOOL.
    [Fact]
    public void Score_Threadpool_RunnableQueueAmplifier_DoesNotFireBelowThreshold()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "THREADPOOL", Value = 0.05,
                Metadata = new() { ["wait_time_ms"] = 7_200_000, ["avg_ms_per_wait"] = 3_600 } },
            new() { Source = "cpu", Key = "RUNNABLE_TASKS", Value = 4,         // warning clear -> no boost
                Metadata = new() { ["total_runnable_tasks"] = 4, ["runnable_tasks_warning"] = 0 } },
        };

        new FactScorer().ScoreAll(facts);

        var tp = facts.First(f => f.Key == "THREADPOOL");
        var rqAmp = tp.AmplifierResults.First(a => a.Description.Contains("Runnable-task queue"));
        Assert.False(rqAmp.Matched);
    }

    // ── Tier-2 parity arms (batch 1): runnable-queue / security-cache / QS-off ──────────────────
    // All three are additions to the SHARED FactScorer, so they fire identically for Lite, Dashboard,
    // and Darling findings. Sourced thresholds are cited per arm against the deprecated Dashboard SQL.

    // ARM 3 — runnable-task-queue depth roots a STANDALONE finding off the collected cpu_scheduler_stats
    // snapshot, tiered to the Dashboard's report.cpu_scheduler_pressure CASE (install/47:1839-1841):
    // > 50 CRITICAL-tier (base 1.0), > 20 HIGH (0.75), > 10 MEDIUM (0.5 — the 0.5 root entry point).
    // Base maxes at 1.0 (top of WARNING) like every base fact; CRITICAL band is reached only via the
    // #1494 runnable-queue -> THREADPOOL amplifier, which still fires independently.
    [Theory]
    [InlineData(60, 1.0)]   // > 50 -> CRITICAL tier (install/47:1839)
    [InlineData(30, 0.75)]  // > 20 -> HIGH tier     (install/47:1840)
    [InlineData(15, 0.5)]   // > 10 -> MEDIUM tier   (install/47:1841)
    public void Score_RunnableTasks_TiersMatchDashboard(double total, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "cpu", Key = "RUNNABLE_TASKS", Value = total,
                Metadata = new() { ["total_runnable_tasks"] = total, ["runnable_tasks_warning"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        var rt = facts.First(f => f.Key == "RUNNABLE_TASKS");
        // No amplifiers on RUNNABLE_TASKS and it is not tuning-class, so Severity == base.
        Assert.Equal(expected, rt.Severity, precision: 4);
        Assert.True(rt.Severity >= 0.5, "a non-NORMAL runnable queue must clear the 0.5 root threshold");
    }

    // A shallow queue below the > 10 bar with the warning flag CLEAR does not score (context-only).
    [Fact]
    public void Score_RunnableTasks_BelowThreshold_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "cpu", Key = "RUNNABLE_TASKS", Value = 5,
                Metadata = new() { ["total_runnable_tasks"] = 5, ["runnable_tasks_warning"] = 0 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "RUNNABLE_TASKS").Severity, precision: 4);
    }

    // Small-box fallback: below the absolute > 10 bar, the collector's runnable_tasks_warning flag
    // (SUM(runnable_tasks_count) >= cpu_count) still scores HIGH (install/47:1844).
    [Fact]
    public void Score_RunnableTasks_WarningFlagOnSmallBox_ScoresHigh()
    {
        var facts = new List<Fact>
        {
            new() { Source = "cpu", Key = "RUNNABLE_TASKS", Value = 6,
                Metadata = new() { ["total_runnable_tasks"] = 6, ["runnable_tasks_warning"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.75, facts.First(f => f.Key == "RUNNABLE_TASKS").Severity, precision: 4);
    }

    // ARM 2 — TokenAndPermUserStore (security cache) growth scores a flat WARNING at >= 1 GB off the
    // otherwise context-only MEMORY_CLERKS fact (install/50:562 severity=WARNING, :583 threshold 1 GB).
    // The clerk size rides in metadata keyed by clerk_type (= sys.dm_os_memory_clerks.type).
    [Fact]
    public void Score_MemoryClerks_SecurityCacheOver1Gb_ScoresWarning()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_CLERKS", Value = 52_048,
                Metadata = new()
                {
                    ["MEMORYCLERK_SQLBUFFERPOOL"] = 50_000,
                    ["USERSTORE_TOKENPERM"] = 2_048, // 2 GB
                    ["total_top_clerks_mb"] = 52_048,
                    ["clerk_count"] = 2
                } },
        };

        new FactScorer().ScoreAll(facts);

        var mc = facts.First(f => f.Key == "MEMORY_CLERKS");
        Assert.True(mc.Severity >= 0.75 && mc.Severity < 1.5, "security-cache growth bands WARNING");
    }

    // Under 1 GB does not score; MEMORY_CLERKS stays context-only.
    [Fact]
    public void Score_MemoryClerks_SecurityCacheUnder1Gb_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_CLERKS", Value = 50_512,
                Metadata = new()
                {
                    ["MEMORYCLERK_SQLBUFFERPOOL"] = 50_000,
                    ["USERSTORE_TOKENPERM"] = 512, // 0.5 GB — below the 1 GB bar
                } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "MEMORY_CLERKS").Severity, precision: 4);
    }

    // A normal clerk set with no TokenAndPermUserStore entry scores 0 (context-only preserved).
    [Fact]
    public void Score_MemoryClerks_NoSecurityCacheClerk_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_CLERKS", Value = 50_000,
                Metadata = new() { ["MEMORYCLERK_SQLBUFFERPOOL"] = 50_000, ["clerk_count"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "MEMORY_CLERKS").Severity, precision: 4);
    }

    // ── Tier-2 ARM: plan-cache single-use bloat scores off PLAN_CACHE_BLOAT (Value = single_use_percent),
    // tiered > 50/30/20% (install/47:1485-1487) behind a single_use_size_mb >= 100 noise guard. ──
    [Fact]
    public void Score_PlanCacheBloat_Over50Percent_BandsWarningCeiling()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "PLAN_CACHE_BLOAT", Value = 62,
                Metadata = new()
                {
                    ["single_use_plans"] = 6_200,
                    ["total_plans"] = 10_000,
                    ["single_use_size_mb"] = 800, // >= 100 MB — real bloat
                    ["total_size_mb"] = 1_300,
                    ["single_use_percent"] = 62
                } },
        };

        new FactScorer().ScoreAll(facts);

        // > 50% is the Dashboard's CRITICAL tier, but base caps at the WARNING ceiling (1.0); CRITICAL is
        // earned only via corroboration.
        Assert.Equal(1.0, facts.First(f => f.Key == "PLAN_CACHE_BLOAT").Severity, precision: 4);
    }

    [Fact]
    public void Score_PlanCacheBloat_Between30And50_ScoresHigh()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "PLAN_CACHE_BLOAT", Value = 38,
                Metadata = new() { ["single_use_size_mb"] = 400, ["single_use_percent"] = 38 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.75, facts.First(f => f.Key == "PLAN_CACHE_BLOAT").Severity, precision: 4);
    }

    [Fact]
    public void Score_PlanCacheBloat_Between20And30_ScoresMedium()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "PLAN_CACHE_BLOAT", Value = 24,
                Metadata = new() { ["single_use_size_mb"] = 250, ["single_use_percent"] = 24 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.5, facts.First(f => f.Key == "PLAN_CACHE_BLOAT").Severity, precision: 4);
    }

    // Under 20% single-use is healthy plan-cache composition — dormant.
    [Fact]
    public void Score_PlanCacheBloat_Under20Percent_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "PLAN_CACHE_BLOAT", Value = 12,
                Metadata = new() { ["single_use_size_mb"] = 400, ["single_use_percent"] = 12 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "PLAN_CACHE_BLOAT").Severity, precision: 4);
    }

    // A high single-use % on a tiny footprint (< 100 MB) is noise — the size guard keeps it dormant even
    // above the 20% bar.
    [Fact]
    public void Score_PlanCacheBloat_HighPercentButTinyFootprint_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "PLAN_CACHE_BLOAT", Value = 70,
                Metadata = new() { ["single_use_size_mb"] = 40, ["single_use_percent"] = 70 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "PLAN_CACHE_BLOAT").Severity, precision: 4);
    }

    // ── Tier-2 ARM: ring-buffer physical-memory-pressure scores off MEMORY_PRESSURE_EVENTS (Value = the
    // max indicator), banded >= 3 HIGH (0.9) / >= 2 MEDIUM (0.5) per report.memory_pressure_events
    // (install/47:229-236). ──
    [Fact]
    public void Score_MemoryPressureEvents_Indicator3_BandsWarning()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_PRESSURE_EVENTS", Value = 3,
                Metadata = new() { ["event_count"] = 12, ["max_process_indicator"] = 3, ["max_system_indicator"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        var mp = facts.First(f => f.Key == "MEMORY_PRESSURE_EVENTS");
        Assert.Equal(0.9, mp.Severity, precision: 4);
        Assert.True(mp.Severity >= 0.75 && mp.Severity < 1.5, "HIGH pressure bands WARNING");
    }

    [Fact]
    public void Score_MemoryPressureEvents_Indicator2_ScoresMedium()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_PRESSURE_EVENTS", Value = 2,
                Metadata = new() { ["event_count"] = 4, ["max_process_indicator"] = 2, ["max_system_indicator"] = 0 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.5, facts.First(f => f.Key == "MEMORY_PRESSURE_EVENTS").Severity, precision: 4);
    }

    // Indicator 1 (steady / LOW) does not score — a defensive floor (the collector already gates < 2 out).
    [Fact]
    public void Score_MemoryPressureEvents_Indicator1_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "memory", Key = "MEMORY_PRESSURE_EVENTS", Value = 1,
                Metadata = new() { ["event_count"] = 0, ["max_process_indicator"] = 1, ["max_system_indicator"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "MEMORY_PRESSURE_EVENTS").Severity, precision: 4);
    }

    // batch-2b — priority boost enabled is a Dashboard WARNING (install/50:368). CONFIG_PRIORITY_BOOST
    // bands WARNING (0.9) when value_in_use == 1, and scores 0 when disabled.
    [Fact]
    public void Score_ConfigPriorityBoost_Enabled_BandsWarning()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_PRIORITY_BOOST", Value = 1, Metadata = new() { ["value_in_use"] = 1 } },
        };
        new FactScorer().ScoreAll(facts);
        var f = facts.First(f => f.Key == "CONFIG_PRIORITY_BOOST");
        Assert.True(f.Severity >= 0.75 && f.Severity < 1.5, "priority boost bands WARNING when enabled");
    }

    [Fact]
    public void Score_ConfigPriorityBoost_Disabled_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_PRIORITY_BOOST", Value = 0, Metadata = new() { ["value_in_use"] = 0 } },
        };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.0, facts.First(f => f.Key == "CONFIG_PRIORITY_BOOST").Severity, precision: 4);
    }

    // Lightweight pooling enabled is a Dashboard WARNING (install/50:401) — same shape.
    [Fact]
    public void Score_ConfigLightweightPooling_Enabled_BandsWarning()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_LIGHTWEIGHT_POOLING", Value = 1, Metadata = new() { ["value_in_use"] = 1 } },
        };
        new FactScorer().ScoreAll(facts);
        var f = facts.First(f => f.Key == "CONFIG_LIGHTWEIGHT_POOLING");
        Assert.True(f.Severity >= 0.75 && f.Severity < 1.5, "lightweight pooling bands WARNING when enabled");
    }

    [Fact]
    public void Score_ConfigLightweightPooling_Disabled_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_LIGHTWEIGHT_POOLING", Value = 0, Metadata = new() { ["value_in_use"] = 0 } },
        };
        new FactScorer().ScoreAll(facts);
        Assert.Equal(0.0, facts.First(f => f.Key == "CONFIG_LIGHTWEIGHT_POOLING").Severity, precision: 4);
    }

    // ARM 1 — Query Store off on a user database is an INFO advisory carried by the DB_CONFIG fact,
    // detected as query_store_on_count < database_count (install/50:83 severity=INFO). Low 0.3 base =
    // INFO band, and DB_CONFIG roots at any positive severity (a ConfigAdvisoryRootKey).
    [Fact]
    public void Score_DbConfig_QueryStoreOff_ScoresInfoAdvisory()
    {
        var facts = new List<Fact>
        {
            new() { Source = "database_config", Key = "DB_CONFIG", Value = 3,
                Metadata = new() { ["database_count"] = 3, ["query_store_on_count"] = 1 } },
        };

        new FactScorer().ScoreAll(facts);

        var db = facts.First(f => f.Key == "DB_CONFIG");
        Assert.Equal(0.3, db.Severity, precision: 4);
        Assert.True(db.Severity > 0 && db.Severity < 0.75, "QS-off is an INFO-band advisory");
    }

    // Every user DB has Query Store on and nothing else drifted -> DB_CONFIG does not score.
    [Fact]
    public void Score_DbConfig_QueryStoreAllOn_NoContribution()
    {
        var facts = new List<Fact>
        {
            new() { Source = "database_config", Key = "DB_CONFIG", Value = 3,
                Metadata = new() { ["database_count"] = 3, ["query_store_on_count"] = 3 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "DB_CONFIG").Severity, precision: 4);
    }

    // QS-off never lowers a worse co-present DB_CONFIG issue (existing arms unchanged): AUTO_SHRINK on
    // 3 DBs stays 0.9 even with Query Store also off on all of them.
    [Fact]
    public void Score_DbConfig_QueryStoreOff_DoesNotLowerWorseIssue()
    {
        var facts = new List<Fact>
        {
            new() { Source = "database_config", Key = "DB_CONFIG", Value = 3,
                Metadata = new()
                {
                    ["database_count"] = 3,
                    ["query_store_on_count"] = 0,   // all off
                    ["auto_shrink_on_count"] = 3    // worse issue -> 3 * 0.3 = 0.9
                } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.9, facts.First(f => f.Key == "DB_CONFIG").Severity, precision: 4);
    }

    // ── Tier-2 parity arms (batch 2): tempdb-deep — version-store size + PAGELATCH_UP allocation ─────
    // Both are additions to the SHARED FactScorer, so they fire identically for Lite, Dashboard, and
    // Darling findings. Every trip point is cited to the deprecated Dashboard's install SQL.

    // ARM 1 — tempdb VERSION-STORE pressure by ABSOLUTE size (max_version_store_mb in the TEMPDB_USAGE
    // fact metadata), scored as the WORSE of space-fraction and version-store size. Tiers mirror
    // report.tempdb_pressure's pressure_level CASE (install/47:1431-1433): > 5000 MB CRITICAL-tier
    // (base 1.0), > 2000 MB HIGH (0.75), > 1000 MB MEDIUM (0.5 — the 0.5 root entry point). Space is only
    // 20% full, whose sub-0.5 fraction score is dominated by the version-store arm via Math.Max, so the
    // asserted severity IS the version-store tier — proving the arm scores independently of fill.
    [Theory]
    [InlineData(6000, 1.0)]   // > 5000 -> CRITICAL tier (install/47:1431)
    [InlineData(3000, 0.75)]  // > 2000 -> HIGH tier     (install/47:1432)
    [InlineData(1500, 0.5)]   // > 1000 -> MEDIUM tier   (install/47:1433)
    public void Score_TempDbVersionStore_TiersMatchDashboard(double versionMb, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "tempdb", Key = "TEMPDB_USAGE", Value = 0.20, // space only 20% full
                Metadata = new() { ["max_version_store_mb"] = versionMb } },
        };

        new FactScorer().ScoreAll(facts);

        var t = facts.First(f => f.Key == "TEMPDB_USAGE");
        Assert.Equal(expected, t.Severity, precision: 4);
        Assert.True(t.Severity >= 0.5, "a version store over 1 GB must clear the 0.5 root threshold");
    }

    // A version store below the > 1000 MB bar does not score. Space fraction 0 isolates the version-store
    // arm (the space-fraction arm yields a small sub-0.5 base for any positive fill), proving a 900 MB
    // store trips nothing on its own.
    [Fact]
    public void Score_TempDbVersionStore_BelowThreshold_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "tempdb", Key = "TEMPDB_USAGE", Value = 0.0,
                Metadata = new() { ["max_version_store_mb"] = 900 } }, // 900 MB — below the 1 GB bar
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "TEMPDB_USAGE").Severity, precision: 4);
    }

    // WORSE-of: the version-store arm never LOWERS a higher space-fraction score. Space 95% full (the
    // fraction arm hits its 0.90 critical -> 1.0) with a small version store stays 1.0.
    [Fact]
    public void Score_TempDbVersionStore_DoesNotLowerWorseSpaceFraction()
    {
        var facts = new List<Fact>
        {
            new() { Source = "tempdb", Key = "TEMPDB_USAGE", Value = 0.95,
                Metadata = new() { ["max_version_store_mb"] = 200 } }, // small VS, but space is critical
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(1.0, facts.First(f => f.Key == "TEMPDB_USAGE").Severity, precision: 4);
    }

    // ARM 2 — tempdb allocation / PFS-GAM-SGAM contention scored off the PAGELATCH_UP wait fact by
    // wait_time_ms PER OBSERVED HOUR (server-wide wait_stats, the SAME data the source view reads),
    // tripping at > 10000 ms/hr -> MEDIUM (install/47:2411 sums the last hour; :2515: pagelatch_up_ms >
    // 10000 -> "MEDIUM - PAGELATCH_UP contention"). Flat 0.5 — the view has no higher PAGELATCH_UP band.
    // Value (fraction-of-period) only has to be > 0 to clear the wait guard; the hourly wait_time_ms is
    // what scores. These fixtures carry no period_duration_ms, which the shared scorer reads as a one-hour
    // window (#3538 A7), so 15,000 ms is 15 s/hr and 8,000 ms is 8 s/hr and the pins hold unchanged.
    [Fact]
    public void Score_PageLatchUp_Over10Sec_ScoresMedium()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "PAGELATCH_UP", Value = 0.01,
                Metadata = new() { ["wait_time_ms"] = 15_000, ["waiting_tasks_count"] = 800 } },
        };

        new FactScorer().ScoreAll(facts);

        var p = facts.First(f => f.Key == "PAGELATCH_UP");
        Assert.Equal(0.5, p.Severity, precision: 4);
        Assert.True(p.Severity >= 0.5, "PAGELATCH_UP over the 10s bar must clear the 0.5 root threshold");
    }

    // At/under the 10000 ms bar PAGELATCH_UP stays context-only (a little allocation latching is normal).
    [Fact]
    public void Score_PageLatchUp_Under10Sec_DoesNotScore()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "PAGELATCH_UP", Value = 0.01,
                Metadata = new() { ["wait_time_ms"] = 8_000, ["waiting_tasks_count"] = 300 } },
        };

        new FactScorer().ScoreAll(facts);

        Assert.Equal(0.0, facts.First(f => f.Key == "PAGELATCH_UP").Severity, precision: 4);
    }

    // Confidence-hardening: the low-quality fallback floors at 0.5 AFTER the confidence multiply, so a
    // fired thin-baseline anomaly stays >= InferenceEngine's 0.5 root entry-point even if confidence
    // ever drops below 1.0 (it is a hardcoded 1.0 today; this guards the future). Same shared scorer.
    [Theory]
    [InlineData(1.0, 0.8, 0.5)]   // at the bar, conf 0.8: 0.5*0.8 = 0.4 -> floored to 0.5
    [InlineData(1.5, 0.6, 0.5)]   // 0.75*0.6 = 0.45 -> floored to 0.5
    [InlineData(2.0, 0.6, 0.6)]   // 1.0*0.6 = 0.6 -> above the floor, kept
    [InlineData(2.0, 1.0, 1.0)]   // conf 1.0 unchanged
    public void Score_LowQualityFallback_FloorsAtHalf_EvenWithSubUnitConfidence(
        double exceedance, double confidence, double expected)
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_CPU_SPIKE",
            Metadata = new()
            {
                ["deviation_sigma"] = 0.5,
                ["baseline_low_quality"] = 1.0,
                ["fallback_exceedance"] = exceedance,
                ["confidence"] = confidence
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.Equal(expected, fact.BaseSeverity, precision: 4);
    }

    /* ── Regression: duplicate fact keys must not crash scoring ── */

    // A duplicated fact key (e.g. a config collector returning two rows for the same setting)
    // once aborted the ENTIRE analysis for a server: ScoreAll built its amplifier lookup with a
    // raw ToDictionary(f => f.Key), which throws on a duplicate. The exception was swallowed
    // upstream, silently blanking every finding. ScoreAll must now dedupe and survive.
    [Fact]
    public void ScoreAll_WithDuplicateFactKeys_DoesNotThrow()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },   // duplicate key
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },
        };

        var scorer = new FactScorer();
        var ex = Record.Exception(() => scorer.ScoreAll(facts));

        Assert.Null(ex);
    }

    // The backstop helper keeps the first fact per key and never throws on duplicates.
    [Fact]
    public void ToFactLookup_DedupesByKey_KeepsFirst()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 99 },
        };

        var lookup = facts.ToFactLookup();

        Assert.Single(lookup);
        Assert.Equal(5, lookup["CONFIG_CTFP"].Value);   // first wins
    }

    /* ── WS3: percent-autogrowth-on-large-files config fact ── */

    // A FILE_AUTOGROWTH_PERCENT fact scores the 0.3 advisory base when at least one large
    // percent-growth file was found (file_count > 0) — mirroring DB_CONFIG's single-misconfig
    // base. It is deliberately below the 0.5 incident threshold; it surfaces only because it
    // is a config-advisory root key (see the InferenceEngine rooting test).
    [Fact]
    public void FileAutogrowthPercent_ScoresAdvisoryBase_WhenFilesPresent()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "FILE_AUTOGROWTH_PERCENT", Value = 2,
                    Metadata = new() { ["file_count"] = 2, ["database_count"] = 1 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.3, facts[0].BaseSeverity, precision: 4);
    }

    // No offending files (file_count == 0) → no severity. Defends the collector contract that
    // the fact is emitted only when file_count > 0, and the scorer's own guard.
    [Fact]
    public void FileAutogrowthPercent_ScoresZero_WhenNoFiles()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "FILE_AUTOGROWTH_PERCENT", Value = 0,
                    Metadata = new() { ["file_count"] = 0, ["database_count"] = 0 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for the fact key (a missing AdviceBlock is the P1 dead-fact bug class —
    // a fact that roots but renders no advice). Headline must be the authored copy.
    [Fact]
    public void FileAutogrowthPercent_HasAdviceBlock()
    {
        var advice = FactAdvice.GetForFactKey("FILE_AUTOGROWTH_PERCENT");

        Assert.NotNull(advice);
        Assert.Equal("Large file(s) growing in percentage steps", advice!.Headline);
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }

    /* ── WS3: server-level config facts (MAXDOP / CTFP / max & min memory) ── */

    // Each per-setting CONFIG_* fact scores the 0.4 advisory base ONLY when the value is bad, and
    // 0 otherwise — so audit_config still sees every CONFIG_* fact (it reads the raw value) but
    // only a BAD one roots a recommendation card.
    [Theory]
    // MAXDOP: 0 is bad (unlimited); any other value is operator-chosen.
    [InlineData("CONFIG_MAXDOP", 0, 0.4)]
    [InlineData("CONFIG_MAXDOP", 4, 0.0)]
    [InlineData("CONFIG_MAXDOP", 8, 0.0)]
    // CTFP: <= 5 is bad (default-and-below); above is fine.
    [InlineData("CONFIG_CTFP", 5, 0.4)]
    [InlineData("CONFIG_CTFP", 1, 0.4)]
    [InlineData("CONFIG_CTFP", 50, 0.0)]
    [InlineData("CONFIG_CTFP", 6, 0.0)]
    // max server memory: the 2 PB default (2147483647) is bad; a real cap is fine.
    [InlineData("CONFIG_MAX_MEMORY_MB", 2147483647, 0.4)]
    [InlineData("CONFIG_MAX_MEMORY_MB", 28672, 0.0)]
    public void ServerConfigFact_ScoresBadValueOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = value,
                    Metadata = new() { ["value_in_use"] = value } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // CONFIG_MIN_MAX_MEMORY_NARROW is emitted by the collector ONLY when bad, so any presence of
    // the fact scores 0.4 (the fact's existence is the flag).
    [Fact]
    public void NarrowMemoryFact_AlwaysScoresAdvisoryBase()
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = "CONFIG_MIN_MAX_MEMORY_NARROW", Value = 24000,
                    Metadata = new() { ["min_memory_mb"] = 24000, ["max_memory_mb"] = 28672 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.4, facts[0].BaseSeverity, precision: 4);
    }

    // CONFIG_MIN_MEMORY_MB and CONFIG_MAX_WORKER_THREADS are leaf/context facts — never scored
    // (they feed audit_config / the narrow-memory derivation, not a card of their own).
    [Theory]
    [InlineData("CONFIG_MIN_MEMORY_MB")]
    [InlineData("CONFIG_MAX_WORKER_THREADS")]
    public void ServerConfigLeafFacts_ScoreZero(string key)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = 1024, Metadata = new() { ["value_in_use"] = 1024 } }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(0.0, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each of the four server-config root keys (dead-fact guard).
    [Theory]
    [InlineData("CONFIG_MAXDOP")]
    [InlineData("CONFIG_CTFP")]
    [InlineData("CONFIG_MAX_MEMORY_MB")]
    [InlineData("CONFIG_MIN_MAX_MEMORY_NARROW")]
    public void ServerConfigKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }

    // The shared narrow-memory builder: emitted only when max is CONFIGURED and min >= 80% of max.
    [Theory]
    [InlineData(28672, 24000, true)]    // min 83.7% of max -> narrow
    [InlineData(28672, 22938, true)]    // just over 80% (threshold 22937.6) -> narrow
    [InlineData(28672, 22000, false)]   // ~76.7% -> just under 80%, not narrow
    [InlineData(28672, 14000, false)]   // min ~49% -> not narrow
    [InlineData(2147483647, 2000000000, false)] // max unconfigured -> never narrow
    [InlineData(0, 0, false)]           // max 0 -> guard
    public void BuildNarrowMemoryFact_FollowsThreshold(long maxMb, long minMb, bool emitted)
    {
        var fact = FactRemediation.BuildNarrowMemoryFact(1, maxMb, minMb);

        if (!emitted)
        {
            Assert.Null(fact);
            return;
        }

        Assert.NotNull(fact);
        Assert.Equal("CONFIG_MIN_MAX_MEMORY_NARROW", fact!.Key);
        Assert.Equal(minMb, fact.Value);
        Assert.Equal(minMb, fact.Metadata["min_memory_mb"]);
        Assert.Equal(maxMb, fact.Metadata["max_memory_mb"]);
    }

    [Fact]
    public void BuildNarrowMemoryFact_NullInputs_ReturnsNull()
    {
        Assert.Null(FactRemediation.BuildNarrowMemoryFact(1, null, 24000));
        Assert.Null(FactRemediation.BuildNarrowMemoryFact(1, 28672, null));
    }

    /* ── WS5: server-health advisory facts (LPIM / IFI / memory dumps) ── */

    // Each WS5 server-health fact scores its 0.4 advisory base ONLY when the value is bad, and 0
    // otherwise — mirroring the WS3 server-config keys. The collectors gate emission (Express /
    // small-RAM / dumps>0) so a fact that would score 0 is normally never emitted, but the scorer
    // is still independently bad-only so it can be unit-tested in isolation.
    [Theory]
    // IFI: disabled (Value 0) is bad; enabled (Value 1) is fine.
    [InlineData("CONFIG_IFI_DISABLED", 0, 0.4)]
    [InlineData("CONFIG_IFI_DISABLED", 1, 0.0)]
    // LPIM: disabled (Value 0) is bad; enabled (Value 1) is fine.
    [InlineData("CONFIG_LPIM_DISABLED", 0, 0.4)]
    [InlineData("CONFIG_LPIM_DISABLED", 1, 0.0)]
    // Memory dumps: any count > 0 is bad; 0 is fine.
    [InlineData("SERVER_MEMORY_DUMPS", 1, 0.4)]
    [InlineData("SERVER_MEMORY_DUMPS", 7, 0.4)]
    [InlineData("SERVER_MEMORY_DUMPS", 0, 0.0)]
    public void ServerHealthFact_ScoresBadValueOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "config", Key = key, Value = value }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each WS5 server-health root key (dead-fact guard — a fact that roots but
    // renders no advice is the P1 dead-fact bug class).
    [Theory]
    [InlineData("CONFIG_IFI_DISABLED")]
    [InlineData("CONFIG_LPIM_DISABLED")]
    [InlineData("SERVER_MEMORY_DUMPS")]
    public void ServerHealthKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
        // Advise-only: no generated Apply T-SQL is attached to the bare advice block.
        Assert.Null(advice.RemediationTsql);
    }

    // WS4: plan-XML advisories. MISSING_INDEX / PLAN_WARNING (Source "queries", Value = count)
    // score only when the count is > 0, and 0 otherwise. Advise-only. PLAN_WARNING keeps the 0.4
    // advisory base; MISSING_INDEX is demoted to the Information rung (#3805,
    // FactScorer.MissingIndexCorroborationSeverity = 0.25) — a request is corroboration and never
    // outranks a standing misconfiguration — and the pin reads the constant so the ORDERING, not the
    // digit, is what a future change has to argue with.
    [Theory]
    [InlineData("MISSING_INDEX", 1, FactScorer.MissingIndexCorroborationSeverity)]
    [InlineData("MISSING_INDEX", 5, FactScorer.MissingIndexCorroborationSeverity)]
    [InlineData("MISSING_INDEX", 0, 0.0)]
    [InlineData("PLAN_WARNING", 1, 0.4)]
    [InlineData("PLAN_WARNING", 0, 0.0)]
    public void PlanAdvisoryFact_ScoresWhenPresentOnly(string key, long value, double expected)
    {
        var facts = new List<Fact>
        {
            new() { Source = "queries", Key = key, Value = value }
        };

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        Assert.Equal(expected, facts[0].BaseSeverity, precision: 4);
    }

    // Advice exists for each WS4 plan-advisory root key (dead-fact guard); advise-only (no Apply T-SQL).
    [Theory]
    [InlineData("MISSING_INDEX")]
    [InlineData("PLAN_WARNING")]
    public void PlanAdvisoryKeys_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
        Assert.Null(advice.RemediationTsql);
    }

    // C (workload-aware MAXDOP/CTFP): every THREADPOOL attribution variant InferenceEngine can root
    // on (parallel/blocking/mixed) plus the generic fallback has an advice block — a relabeled root
    // with no advice would render an empty card (the dead-fact bug class).
    [Theory]
    [InlineData("THREADPOOL")]
    [InlineData("THREADPOOL_PARALLEL")]
    [InlineData("THREADPOOL_BLOCKING")]
    [InlineData("THREADPOOL_MIXED")]
    public void ThreadpoolVariants_HaveAdviceBlocks(string key)
    {
        var advice = FactAdvice.GetForFactKey(key);

        Assert.NotNull(advice);
        Assert.False(string.IsNullOrWhiteSpace(advice!.Headline));
        Assert.False(string.IsNullOrWhiteSpace(advice.Investigation));
        Assert.False(string.IsNullOrWhiteSpace(advice.Remediation));
    }
}
