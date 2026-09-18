using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests FactScorer Layer 1 (base severity) and Layer 2 (amplifiers).
/// Validates threshold formulas, amplifier firing, and severity capping.
/// </summary>
public class FactScorerTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public FactScorerTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

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

    /* ── Integration: MemoryStarved scenario ── */

    [Fact]
    public async Task Score_MemoryStarved_PageioLatchHasHighSeverity()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedMemoryStarvedServerAsync());

        var pageio = facts.First(f => f.Key == "PAGEIOLATCH_SH");

        // 69.4% of period, concerning = 25% (no critical) → base = 1.0 (capped)
        Assert.Equal(1.0, pageio.BaseSeverity, precision: 2);

        // SOS at 20.8% > 15% threshold → PAGEIOLATCH amplifier fires (+0.1)
        // severity = 1.0 * (1.0 + 0.1) = 1.1
        Assert.True(pageio.Severity > pageio.BaseSeverity,
            "PAGEIOLATCH should be amplified by SOS_SCHEDULER_YIELD presence");
    }

    [Fact]
    public async Task Score_MemoryStarved_SosSchedulerBelowConcerning()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedMemoryStarvedServerAsync());

        var sos = facts.First(f => f.Key == "SOS_SCHEDULER_YIELD");

        // 20.8% of period, concerning = 75% (no critical) → base = 0.208 / 0.75 ≈ 0.278
        Assert.InRange(sos.BaseSeverity, 0.25, 0.32);
    }

    [Fact]
    public async Task Score_MemoryStarved_WritelogLow()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedMemoryStarvedServerAsync());

        var writelog = facts.First(f => f.Key == "WRITELOG");

        // 1.4% of period, concerning = 10% (no critical) → base = 0.014 / 0.10 ≈ 0.139
        Assert.InRange(writelog.BaseSeverity, 0.12, 0.16);
    }

    /* ── Integration: BadParallelism scenario ── */

    [Fact]
    public async Task Score_BadParallelism_CxPacketHigh()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedBadParallelismServerAsync());

        var cx = facts.First(f => f.Key == "CXPACKET");

        // 55.6% of period, concerning = 25% (no critical) → 1.0 (capped)
        Assert.Equal(1.0, cx.BaseSeverity, precision: 2);
    }

    [Fact]
    public async Task Score_BadParallelism_SosSchedulerBelowConcerning()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedBadParallelismServerAsync());

        var sos = facts.First(f => f.Key == "SOS_SCHEDULER_YIELD");

        // 41.7% of period, concerning = 75% (no critical) → base = 0.417 / 0.75 ≈ 0.556
        Assert.InRange(sos.BaseSeverity, 0.53, 0.58);
    }

    /* ── Integration: Clean scenario ── */

    [Fact]
    public async Task Score_CleanServer_AllSeveritiesLow()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedCleanServerAsync());

        // All waits well below 5% → all severities should be low
        Assert.All(facts, f => Assert.True(f.BaseSeverity < 0.10,
            $"{f.Key} severity {f.BaseSeverity:F3} should be < 0.10"));
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

    // The ANOMALY_WAIT_PROFILE arm scores on the HONEST per-second ratio: below 4x → 0, 4x → 0.5
    // (floor), saturating to 1.0 at 12x. (Precedes the legacy ANOMALY_WAIT_ branch it prefix-matches.)
    [Theory]
    [InlineData(3.0, 0.0)]    // below the floor
    [InlineData(4.0, 0.5)]    // at the floor
    [InlineData(8.0, 0.75)]   // midway up the ramp
    [InlineData(12.0, 1.0)]   // saturated
    [InlineData(100.0, 1.0)]  // is_new sentinel — clamped to 1.0
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

    // Dead-fact guard: ANOMALY_WAIT_PROFILE must resolve to an advice block (a root fact that renders
    // no advice is the P1 dead-fact bug class).
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
    // finding. The scorer must instead grade off the absolute exceedance (peak ÷ the fallback bar): floor
    // 0.5 AT the bar (clears InferenceEngine's 0.5 entry-point), ramping to 1.0 at 2× the bar. A sub-2σ
    // deviation_sigma is planted to prove the 2σ gate is bypassed on this path.
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

    // The finding's exact regression: an over-bar memory reading on a thin baseline used to score 0
    // (small z → 2σ gate → 0 → InferenceEngine drops it → NO finding). It must clear the 0.5 entry.
    // (#1996 moved the memory fallback bar from 95 to 101 — total=target=100 is the healthy state,
    // so the bar now means genuine over-target pressure; the scorer contract here is unchanged.)
    [Fact]
    public void Score_OverBarMemoryOnThinBaseline_ClearsInferenceEntryPoint()
    {
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_MEMORY_PRESSURE",
            Metadata = new()
            {
                ["deviation_sigma"] = 0.4,
                ["baseline_low_quality"] = 1.0,
                ["fallback_exceedance"] = 103.0 / 101.0,
                ["confidence"] = 1.0
            }
        };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.True(fact.BaseSeverity >= 0.5, "a thin-baseline memory-96% fallback must surface, not vanish");
    }

    // The trustworthy z-path is unchanged: with baseline_low_quality=0 a sub-2σ deviation still scores 0
    // (the fallback fix must not leak into the trusted path).
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
    public async Task Amplifier_BadParallelism_CxPacketBoostedBySos()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedBadParallelismServerAsync());

        var cx = facts.First(f => f.Key == "CXPACKET");

        // CXPACKET base ≈ 1.0 (combined CX fraction > threshold)
        // SOS at 41.7% > 25% (+0.3), CTFP=5 (+0.3), MAXDOP=0 (+0.2),
        // CPU at 90% (+0.2) → total boost ≥ 1.0, hits 2.0 cap
        Assert.True(cx.Severity > cx.BaseSeverity, "CXPACKET should be amplified by SOS + config");
        Assert.InRange(cx.Severity, 1.7, 2.0);

        var sosAmp = cx.AmplifierResults.First(a => a.Description.Contains("SOS_SCHEDULER_YIELD"));
        Assert.True(sosAmp.Matched);
        Assert.Equal(0.3, sosAmp.Boost);
    }

    [Fact]
    public async Task Amplifier_BadParallelism_SosBoostedByCxPacket()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedBadParallelismServerAsync());

        var sos = facts.First(f => f.Key == "SOS_SCHEDULER_YIELD");

        // SOS base ≈ 0.556, CXPACKET at 55.6% > 10% threshold → amplifier fires (+0.2)
        // severity = 0.556 * (1.0 + 0.2) = 0.667
        Assert.True(sos.Severity > sos.BaseSeverity, "SOS should be amplified by CXPACKET");

        var cxAmp = sos.AmplifierResults.First(a => a.Description.Contains("CXPACKET"));
        Assert.True(cxAmp.Matched);
    }

    [Fact]
    public async Task Amplifier_CleanServer_NoAmplifiersFire()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedCleanServerAsync());

        // Clean server has very low waits — no amplifiers should fire
        foreach (var fact in facts)
        {
            Assert.Equal(fact.BaseSeverity, fact.Severity,
                precision: 10); // Severity == base (no boost)
        }
    }

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
    // 1.49 WARNING ceiling.
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
    // noise. These pin the significance bars (SOS 0.25, RS 0.10) the escape hatch now uses.

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

    /* ── Layer 3: #3526 anomaly extremity escape + the anomaly co-fire amplifier arm ── */

    // Before #3526 every ANOMALY_* fact was capped at 1.49 by the tuning-class cap, had NO amplifier arm
    // (so its final severity was its <= 1.0 base), and the shipped notify floor is 1.5: the baseline
    // engine could never page. These tests pin the two halves of the fix — an anomaly whose deviation is
    // EXTREME (>= 3x the cutoff it fired at) leaves the cap, and the new co-fire arm can carry an
    // extreme, CORROBORATED anomaly past 1.5 — and, just as deliberately, everything the fix must NOT do.

    // A deviation-scored anomaly on a TRUSTWORTHY baseline (baseline_low_quality = 0). fireThreshold 0
    // means "omit the key" — a pre-#1743 fact, which the scorer anchors at the classical 2.0.
    private static Fact TrustedAnomaly(string key, double deviationSigma, double fireThreshold = 0.0)
    {
        var metadata = new Dictionary<string, double>
        {
            ["deviation_sigma"] = deviationSigma,
            ["baseline_low_quality"] = 0.0,
            ["fallback_exceedance"] = 0.0,
            ["confidence"] = 1.0
        };
        if (fireThreshold > 0) metadata["fire_threshold"] = fireThreshold;
        return new Fact { Source = "anomaly", Key = key, Value = 1, Metadata = metadata };
    }

    // (a) The issue's own 3am shape: a 20σ session spike (robust 3.5 cutoff → the escape opens at 10.5σ)
    // beside a 15σ batch-request anomaly and SQL CPU at 85%. Before the fix the best it could do was 1.0.
    // Now: base 1.0 (saturated), extreme → cap released, two co-fires (+0.3 batch, +0.3 CPU >= 80) → 1.6.
    // The batch anomaly is extreme too (15 >= 10.5) and is corroborated symmetrically (+0.3 sessions,
    // +0.3 CPU) → 1.6 as well. Both clear the shipped 1.5 notify floor for the first time.
    [Fact]
    public void Score_ExtremeCorroboratedAnomaly_CrossesNotifyFloor()
    {
        var facts = new List<Fact>
        {
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 20.0, 3.5),
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 15.0, 3.5),
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 },
        };

        new FactScorer().ScoreAll(facts);

        var sessions = facts.First(f => f.Key == "ANOMALY_SESSION_SPIKE");
        Assert.Equal(1.0, sessions.BaseSeverity, precision: 4);
        Assert.Equal(1.6, sessions.Severity, precision: 4);
        Assert.True(sessions.Severity >= 1.5, "an extreme, twice-corroborated anomaly must reach the CRITICAL band");
        Assert.Contains(sessions.AmplifierResults, a => a.Matched && a.Description.StartsWith("Batch-request anomaly co-fired"));
        Assert.Contains(sessions.AmplifierResults, a => a.Matched && a.Description.StartsWith("SQL Server CPU >= 80%"));

        var batch = facts.First(f => f.Key == "ANOMALY_BATCH_REQUESTS");
        Assert.Equal(1.6, batch.Severity, precision: 4);
    }

    // (b) The SAME fact set with the session spike at exactly its fire threshold: base 0.5, the two
    // co-fires lift it to 0.8, and it stays far inside WARNING. Corroboration alone cannot page a
    // fact that only just fired.
    [Fact]
    public void Score_AnomalyAtFireThreshold_WithCoFires_StaysWarning()
    {
        var facts = new List<Fact>
        {
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 3.5, 3.5),      // exactly at the cutoff → base 0.5
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 15.0, 3.5),
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 },
        };

        new FactScorer().ScoreAll(facts);

        var sessions = facts.First(f => f.Key == "ANOMALY_SESSION_SPIKE");
        Assert.Equal(0.5, sessions.BaseSeverity, precision: 4);
        Assert.Equal(0.8, sessions.Severity, precision: 4);   // 0.5 x (1 + 0.3 + 0.3)
        Assert.True(sessions.Severity <= 1.49, "an anomaly at its fire threshold must stay in WARNING");
    }

    // The routine-but-saturated case the cap exists for: 7σ on the 3.5 cutoff is 2x the anchor — the
    // ramp's saturation point (base 1.0) but NOT extreme (the escape needs 10.5σ). Three co-fires push
    // the Layer-2 product to 1.9, and the cap holds it at 1.49: a busy evening does not page.
    [Fact]
    public void Score_SaturatedButRoutineAnomaly_WithThreeCoFires_CappedAtWarningCeiling()
    {
        var facts = new List<Fact>
        {
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 7.0, 3.5),      // 2x anchor → base 1.0, not extreme
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 7.0, 3.5),     // +0.3
            TrustedAnomaly("ANOMALY_CPU_SPIKE", 7.0, 3.5),          // +0.3
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 }, // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var sessions = facts.First(f => f.Key == "ANOMALY_SESSION_SPIKE");
        Assert.Equal(1.0, sessions.BaseSeverity, precision: 4);
        // 1.0 x (1 + 0.3 + 0.3 + 0.3) = 1.9 pre-cap; the anomaly is not extreme, so Layer 3 caps it.
        Assert.Equal(1.49, sessions.Severity, precision: 4);
        Assert.True(sessions.Severity < 1.5, "saturation is not extremity — a routine 2x-anchor anomaly must stay in WARNING");
    }

    // The escape bar is 3x the cutoff the fact FIRED at, on every path the detectors take: classical 2.0
    // (a pre-#1743 fact with no fire_threshold) → 6σ; robust 3.5 → 10.5σ; heavy-tail 5.0 → 15σ. Each
    // row plants the root beside the same two co-fires (+0.3 +0.3 → 1.6 when released, 1.49 when
    // capped) and probes one side of the bar. The root's base is 1.0 in every row (all are >= 2x anchor).
    [Theory]
    [InlineData(6.0, 0.0, 1.6)]      // classical: 3 x 2.0 → released
    [InlineData(5.99, 0.0, 1.49)]    // classical: a hair under → capped
    [InlineData(10.5, 3.5, 1.6)]     // robust: 3 x 3.5 → released
    [InlineData(10.49, 3.5, 1.49)]   // robust: a hair under → capped
    [InlineData(15.0, 5.0, 1.6)]     // heavy-tail: 3 x 5.0 → released
    [InlineData(14.99, 5.0, 1.49)]   // heavy-tail: a hair under → capped
    public void Score_AnomalyExtremityEscape_OpensAtThreeTimesTheFireThreshold(
        double deviationSigma, double fireThreshold, double expected)
    {
        var facts = new List<Fact>
        {
            TrustedAnomaly("ANOMALY_QUERY_DURATION", deviationSigma, fireThreshold),
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 15.0, 3.5),           // +0.3
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 }, // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var root = facts.First(f => f.Key == "ANOMALY_QUERY_DURATION");
        Assert.Equal(1.0, root.BaseSeverity, precision: 4);
        Assert.Equal(expected, root.Severity, precision: 4);
    }

    // (c) The never-blind fallback path: on an untrustworthy baseline the stored deviation_sigma is a
    // meaningless z the detector refused to trust, so the escape must NOT read it — a planted 30σ (far
    // past any sigma bar) must not release the cap. The fallback path escapes only when the ABSOLUTE
    // exceedance is equally extreme: 3x the fallback bar (for batch requests, 15,000/s against the
    // 5,000/s bar). 2.5x saturates the base at 1.0 but stays capped; 3.0x is released.
    [Theory]
    [InlineData(2.5, 1.49)]   // past saturation (2x), under the 3x escape bar → capped
    [InlineData(2.99, 1.49)]  // a hair under → capped
    [InlineData(3.0, 1.6)]    // 3x the absolute bar → released, corroborated → pages
    public void Score_LowQualityAnomaly_EscapesOnlyAtThreeTimesItsAbsoluteBar_NeverOnSigma(
        double exceedance, double expected)
    {
        var facts = new List<Fact>
        {
            new()
            {
                Source = "anomaly",
                Key = "ANOMALY_BATCH_REQUESTS",
                Value = 1,
                Metadata = new()
                {
                    ["deviation_sigma"] = 30.0,          // would be extreme on the trusted path; must be ignored here
                    ["fire_threshold"] = 3.5,
                    ["baseline_low_quality"] = 1.0,
                    ["fallback_exceedance"] = exceedance,
                    ["confidence"] = 1.0
                }
            },
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 15.0, 3.5),           // +0.3
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 }, // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var batch = facts.First(f => f.Key == "ANOMALY_BATCH_REQUESTS");
        Assert.Equal(1.0, batch.BaseSeverity, precision: 4);   // the fallback ramp saturates at 2x the bar
        Assert.Equal(expected, batch.Severity, precision: 4);
    }

    // (d) The cap's original target is untouched: CXPACKET with its non-impact amplifiers (1.7 pre-cap)
    // stays at 1.49 even when two EXTREME anomalies co-fire beside it — anomalies are not impact peers,
    // and the extremity escape is per-fact, for ANOMALY_* only. In the same fact set the anomalies
    // themselves ARE released (each is extreme and corroborated by the other + CPU): the two escapes
    // are independent.
    [Fact]
    public void Score_CxPacketWithExtremeAnomalyCoFires_StillCapped_AnomaliesReleased()
    {
        var facts = new List<Fact>
        {
            new() { Source = "waits", Key = "CXPACKET", Value = 0.80 },        // base 1.0
            new() { Source = "config", Key = "CONFIG_CTFP", Value = 5 },       // +0.3
            new() { Source = "config", Key = "CONFIG_MAXDOP", Value = 0 },     // +0.2
            new() { Source = "queries", Key = "QUERY_HIGH_DOP", Value = 10 },  // +0.2
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 20.0, 3.5),
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 15.0, 3.5),
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 },
        };

        new FactScorer().ScoreAll(facts);

        var cx = facts.First(f => f.Key == "CXPACKET");
        Assert.Equal(1.49, cx.Severity, precision: 4);
        Assert.True(cx.Severity < 1.5, "extreme anomalies are not impact peers — CXPACKET must stay capped");

        Assert.Equal(1.6, facts.First(f => f.Key == "ANOMALY_SESSION_SPIKE").Severity, precision: 4);
        Assert.Equal(1.6, facts.First(f => f.Key == "ANOMALY_BATCH_REQUESTS").Severity, precision: 4);
    }

    // (e) A LONE extreme anomaly does not page. 25σ is the detectors' display cap — the most extreme
    // reading a fact can carry — and with nothing else moving it scores exactly its 1.0 base: the cap
    // is released but there is nothing for the release to preserve. The CRITICAL band is earned only
    // with corroboration (the rule every base fact follows), and a solitary extreme reading is the shape
    // a collector hiccup or a variance-collapsed baseline pinned at the display cap produces.
    [Fact]
    public void Score_LoneExtremeAnomaly_StaysAtBase_DoesNotPage()
    {
        var facts = new List<Fact> { TrustedAnomaly("ANOMALY_SESSION_SPIKE", 25.0, 3.5) };

        new FactScorer().ScoreAll(facts);

        var sessions = facts[0];
        Assert.Equal(1.0, sessions.BaseSeverity, precision: 4);
        Assert.Equal(1.0, sessions.Severity, precision: 4);
        Assert.True(sessions.Severity < 1.5, "a single uncorroborated anomaly must not page, however extreme");
        Assert.All(sessions.AmplifierResults, a => Assert.False(a.Matched));
    }

    // One corroborator is not enough either: extreme + a single +0.3 co-fire = 1.3, WARNING. Two
    // independent corroborators is the bar (see the AnomalyAmplifiers worked numbers).
    [Fact]
    public void Score_ExtremeAnomaly_SingleCoFire_StaysWarning()
    {
        var facts = new List<Fact>
        {
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 20.0, 3.5),
            TrustedAnomaly("ANOMALY_BATCH_REQUESTS", 15.0, 3.5),   // the only co-fire: +0.3
        };

        new FactScorer().ScoreAll(facts);

        var sessions = facts.First(f => f.Key == "ANOMALY_SESSION_SPIKE");
        Assert.Equal(1.3, sessions.Severity, precision: 4);
        Assert.True(sessions.Severity < 1.5, "one corroborator lifts an extreme anomaly, but not into CRITICAL");
    }

    // A family never corroborates itself: the CPU anomaly's arm carries the other three load siblings and
    // the measured-CPU confirmation, but no "CPU anomaly co-fired" entry.
    [Fact]
    public void Score_AnomalyCoFireArm_DoesNotSelfCorroborate()
    {
        var facts = new List<Fact> { TrustedAnomaly("ANOMALY_CPU_SPIKE", 12.0, 3.5) };

        new FactScorer().ScoreAll(facts);

        var cpu = facts[0];
        Assert.DoesNotContain(cpu.AmplifierResults, a => a.Description.StartsWith("CPU anomaly co-fired"));
        Assert.Contains(cpu.AmplifierResults, a => a.Description.StartsWith("Session-count anomaly co-fired"));
        Assert.Contains(cpu.AmplifierResults, a => a.Description.StartsWith("Batch-request anomaly co-fired"));
        Assert.Contains(cpu.AmplifierResults, a => a.Description.StartsWith("Query-duration anomaly co-fired"));
        Assert.Contains(cpu.AmplifierResults, a => a.Description.StartsWith("SQL Server CPU >= 80%"));
    }

    // The wait-profile family: graded and escaped off the SAME statistic. On the robust trigger the
    // escape opens at 3 x the 5.0 heavy-tail cutoff = 15σ (also the ramp's saturation point); read-
    // latency and query-duration anomalies corroborate at +0.3 each → 1.6. A hair under 15σ stays
    // capped at 1.49 with the same co-fires.
    [Theory]
    [InlineData(15.0, 1.6)]
    [InlineData(14.99, 1.49)]
    public void Score_WaitProfileAnomaly_EscapesAtThreeTimesTheHeavyTailCutoff(double modifiedZ, double expected)
    {
        var facts = new List<Fact>
        {
            new()
            {
                Source = "anomaly",
                Key = "ANOMALY_WAIT_PROFILE",
                Value = 1,
                Metadata = new() { ["modified_z"] = modifiedZ, ["ratio"] = 3.0, ["is_new"] = 0 }
            },
            TrustedAnomaly("ANOMALY_READ_LATENCY", 12.0, 3.5),      // +0.3
            TrustedAnomaly("ANOMALY_QUERY_DURATION", 15.0, 5.0),    // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var profile = facts.First(f => f.Key == "ANOMALY_WAIT_PROFILE");
        Assert.Equal(expected, profile.Severity, precision: 4);
    }

    // A first-occurrence wait profile (is_new) carries the NoBaselineRatio sentinel (100 — far past any
    // ratio bar) as a scoring device, not a measurement. It must never escape: "no baseline" cannot be
    // "extreme against baseline". With the same two co-fires the 1.6 product is capped to 1.49.
    [Fact]
    public void Score_IsNewWaitProfile_NeverEscapes_DespiteSentinelRatio()
    {
        var facts = new List<Fact>
        {
            new()
            {
                Source = "anomaly",
                Key = "ANOMALY_WAIT_PROFILE",
                Value = 1,
                Metadata = new() { ["modified_z"] = 0.0, ["ratio"] = PerformanceMonitor.Analysis.Baselines.AnomalyThresholds.NoBaselineRatio, ["is_new"] = 1 }
            },
            TrustedAnomaly("ANOMALY_READ_LATENCY", 12.0, 3.5),      // +0.3
            TrustedAnomaly("ANOMALY_QUERY_DURATION", 15.0, 5.0),    // +0.3
        };

        new FactScorer().ScoreAll(facts);

        var profile = facts.First(f => f.Key == "ANOMALY_WAIT_PROFILE");
        Assert.Equal(1.0, profile.BaseSeverity, precision: 4);   // the sentinel ratio saturates the ramp
        Assert.Equal(1.49, profile.Severity, precision: 4);
    }

    // The ratio/count families are NOT released: a blocking spike at 50x its baseline rate (base 1.0,
    // saturated) has no extremity arm and no co-fire arm — it scores its base, and the cap is moot. Its
    // impact reaches CRITICAL through the never-capped BLOCKING_EVENTS / DEADLOCKS keys instead.
    [Fact]
    public void Score_BlockingSpikeAnomaly_NoEscape_NoArm()
    {
        var facts = new List<Fact>
        {
            new() { Source = "anomaly", Key = "ANOMALY_BLOCKING_SPIKE", Value = 1, Metadata = new() { ["ratio"] = 50.0, ["is_new"] = 0 } },
            TrustedAnomaly("ANOMALY_SESSION_SPIKE", 20.0, 3.5),
            new() { Source = "cpu", Key = "CPU_SQL_PERCENT", Value = 85 },
        };

        new FactScorer().ScoreAll(facts);

        var spike = facts.First(f => f.Key == "ANOMALY_BLOCKING_SPIKE");
        Assert.Equal(1.0, spike.BaseSeverity, precision: 4);
        Assert.Equal(1.0, spike.Severity, precision: 4);
        Assert.Empty(spike.AmplifierResults);
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
    // ABSOLUTE wait_time_ms (server-wide wait_stats, the SAME data the source view reads), tripping at
    // > 10000 ms -> MEDIUM (install/47:2515: pagelatch_up_ms > 10000 -> "MEDIUM - PAGELATCH_UP
    // contention"). Flat 0.5 — the view has no higher PAGELATCH_UP band. Value (fraction-of-period) only
    // has to be > 0 to clear the wait guard; the absolute wait_time_ms is what scores.
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
    // ever drops below 1.0 (it is a hardcoded 1.0 today; this guards the future).
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

    /* ── Helper ── */

    /* ── Blocking-chain & compile-wait scoring ── */

    [Theory]
    [InlineData(0.10, 1.0)]    // at the critical threshold → 1.0
    [InlineData(0.055, 0.75)]  // midway between concerning (0.01) and critical (0.10)
    public void Score_ResourceSemaphoreQueryCompile_Ramped(double value, double expected)
    {
        var fact = new Fact { Source = "waits", Key = "RESOURCE_SEMAPHORE_QUERY_COMPILE", Value = value };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        Assert.Equal(expected, fact.BaseSeverity, precision: 2);
    }

    [Fact]
    public async Task Score_DeepBlockingChain_HighSeverityWithSleepingApexAmplifier()
    {
        var facts = await CollectAndScoreAsync(s => s.SeedDeepBlockingChainServerAsync());
        var chain = facts.First(f => f.Key == "BLOCKING_CHAIN");

        // Worst chain is depth 4 → ApplyThresholdFormula(4, 3, 8) = 0.6 base.
        Assert.Equal(0.6, chain.BaseSeverity, precision: 2);
        // Sleeping apex, deadlocks, and THREADPOOL all amplify above the base.
        Assert.True(chain.Severity > chain.BaseSeverity,
            "BLOCKING_CHAIN should be amplified by the sleeping apex and corroborating facts");
    }

    private async Task<List<Fact>> CollectAndScoreAsync(Func<TestDataSeeder, Task> seedAction)
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        return facts;
    }
}
