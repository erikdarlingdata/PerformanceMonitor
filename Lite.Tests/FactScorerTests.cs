using System;
using System.Collections.Generic;
using System.IO;
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
public class FactScorerTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public FactScorerTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(_dbPath);
    }

    public void Dispose()
    {
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch { /* Best-effort cleanup */ }
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

        // base 1.0 * (1.0 + 0.3 SOS + 0.4 THREADPOOL + 0.3 CTFP + 0.2 MAXDOP) = 2.2 → capped at 2.0
        Assert.True(cx.Severity <= 2.0, "Severity should never exceed 2.0");
        Assert.Equal(2.0, cx.Severity);
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
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        return facts;
    }
}
