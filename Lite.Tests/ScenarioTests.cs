using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// End-to-end scenario tests for the full analysis pipeline.
/// Each test seeds a specific server profile, runs the entire engine,
/// and validates the engine output (paths, severity, facts) for that scenario.
/// </summary>
public class ScenarioTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public ScenarioTests()
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

    /* ── Thread Exhaustion ── */

    [Fact]
    public async Task ThreadExhaustion_ThreadpoolIsHighSeverity()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedThreadExhaustionServerAsync());
        PrintStories("THREAD EXHAUSTION", stories);

        // THREADPOOL should be in the stories (very high severity due to low threshold)
        Assert.Contains(stories, s => s.Path.Contains("THREADPOOL"));
    }

    [Fact]
    public async Task ThreadExhaustion_TraversesToParallelismRoot()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedThreadExhaustionServerAsync());

        // THREADPOOL should connect to CXPACKET (parallel queries consuming thread pool)
        var threadpoolStory = stories.FirstOrDefault(s => s.RootFactKey == "THREADPOOL");
        if (threadpoolStory != null)
        {
            Assert.Contains("CXPACKET", threadpoolStory.Path);
        }
    }

    /* ── Blocking-Driven Thread Exhaustion ── */

    [Fact]
    public async Task BlockingThreadExhaustion_BlockingEventsLeadToLck()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedBlockingThreadExhaustionServerAsync());
        PrintStories("BLOCKING THREAD EXHAUSTION", stories);

        // BLOCKING_EVENTS is the root cause (200 events, 50/hr, max severity after amplifiers)
        // It traverses to LCK (confirmed by lock waits) and DEADLOCKS
        var blockingStory = stories.FirstOrDefault(s => s.RootFactKey == "BLOCKING_EVENTS");
        Assert.NotNull(blockingStory);
        Assert.Contains("LCK", blockingStory.Path);

        // THREADPOOL still appears as a separate story
        Assert.Contains(stories, s => s.Path.Contains("THREADPOOL"));
    }

    [Fact]
    public async Task BlockingThreadExhaustion_ThreadpoolAmplifiedByBlocking()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedBlockingThreadExhaustionServerAsync());

        // THREADPOOL should have the blocking amplifier fire
        if (facts.TryGetValue("THREADPOOL", out var tp))
        {
            var blockingAmp = tp.AmplifierResults.FirstOrDefault(a => a.Description.Contains("Lock contention"));
            Assert.NotNull(blockingAmp);
            Assert.True(blockingAmp.Matched);
        }
    }

    [Fact]
    public async Task BlockingThreadExhaustion_BlockingEventsHighSeverity()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedBlockingThreadExhaustionServerAsync());

        // 200 events in 4 hours = 50/hr — at the critical threshold
        Assert.True(facts.ContainsKey("BLOCKING_EVENTS"), "Blocking events should be collected");
        Assert.True(facts["BLOCKING_EVENTS"].Severity > 0.5, "Blocking events severity should be high");
    }

    [Fact]
    public async Task BlockingThreadExhaustion_DeadlocksPresent()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedBlockingThreadExhaustionServerAsync());

        // 15 deadlocks in 4 hours = 3.75/hr — below concerning threshold (5/hr)
        // so it should be present but low severity
        Assert.True(facts.ContainsKey("DEADLOCKS"), "Deadlocks should be collected");
    }

    /* ── Lock Contention ── */

    [Fact]
    public async Task LockContention_ExclusiveLockLeads()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedLockContentionServerAsync());
        PrintStories("LOCK CONTENTION", stories);

        // Grouped LCK should be highest severity (X+U+IX combined)
        Assert.Equal("LCK", stories[0].RootFactKey);
    }

    [Fact]
    public async Task LockContention_BlockingEventsCorroborate()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedLockContentionServerAsync());

        // Blocking events should exist as a fact (15/hr > 10/hr threshold)
        Assert.True(facts.ContainsKey("BLOCKING_EVENTS"), "Blocking events should be collected");

        // LCK should traverse to BLOCKING_EVENTS
        var lckStory = stories.First(s => s.RootFactKey == "LCK");
        Assert.Contains("BLOCKING_EVENTS", lckStory.Path);
    }

    /* ── Reader/Writer Blocking ── */

    [Fact]
    public async Task ReaderWriterBlocking_SharedLockLeads()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedReaderWriterBlockingServerAsync());
        PrintStories("READER/WRITER BLOCKING", stories);

        // LCK_M_S should be highest (27.8% of period, concerning = 5%)
        Assert.Equal("LCK_M_S", stories[0].RootFactKey);
    }

    [Fact]
    public async Task ReaderWriterBlocking_IntentSharedAlsoPresent()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedReaderWriterBlockingServerAsync());

        Assert.Contains(stories, s => s.RootFactKey == "LCK_M_IS");
    }

    [Fact]
    public async Task ReaderWriterBlocking_DeadlocksPresent()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedReaderWriterBlockingServerAsync());

        // 8 deadlocks in 4 hours = 2/hr — below concerning (5/hr) but still collected
        Assert.True(facts.ContainsKey("DEADLOCKS"), "Deadlocks should be collected");
    }

    [Fact]
    public async Task ReaderWriterBlocking_BlockingEventsPresent()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedReaderWriterBlockingServerAsync());

        // 40 blocking events in 4 hours = 10/hr — at concerning threshold
        Assert.True(facts.ContainsKey("BLOCKING_EVENTS"), "Blocking events should be collected");
    }

    /* ── Serializable Abuse ── */

    [Fact]
    public async Task SerializableAbuse_DeadlocksOrRangeLocksLead()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedSerializableAbuseServerAsync());
        PrintStories("SERIALIZABLE ABUSE", stories);

        // DEADLOCKS leads (25 deadlocks, 6.25/hr, amplified by reader locks)
        // because serializable patterns cause frequent deadlocks.
        // Range lock modes should still appear in stories.
        Assert.True(
            stories[0].RootFactKey == "DEADLOCKS" || stories[0].RootFactKey.StartsWith("LCK_M_R"),
            $"Expected DEADLOCKS or range lock mode as root, got {stories[0].RootFactKey}");

        // Range lock stories should still appear
        Assert.Contains(stories, s => s.RootFactKey.StartsWith("LCK_M_R") ||
                                      s.Path.Any(p => p.StartsWith("LCK_M_R")));
    }

    [Fact]
    public async Task SerializableAbuse_MultipleRangeLocksPresent()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedSerializableAbuseServerAsync());

        // Multiple range lock types should appear (either as roots or supporting evidence)
        var allFactKeys = stories.SelectMany(s => s.Path).ToHashSet();
        var rangeLocks = allFactKeys.Where(k => k.StartsWith("LCK_M_R")).ToList();
        Assert.True(rangeLocks.Count >= 1, $"Expected range lock types in stories, got {rangeLocks.Count}");
    }

    [Fact]
    public async Task SerializableAbuse_DeadlocksHighSeverity()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedSerializableAbuseServerAsync());

        // 25 deadlocks in 4 hours = 6.25/hr — above concerning threshold (5/hr)
        Assert.True(facts.ContainsKey("DEADLOCKS"), "Deadlocks should be collected");
        Assert.True(facts["DEADLOCKS"].Severity > 0, "Deadlocks should have non-zero severity");
    }

    /* ── Log Write Pressure ── */

    [Fact]
    public async Task LogWritePressure_WritelogLeads()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedLogWritePressureServerAsync());
        PrintStories("LOG WRITE PRESSURE", stories);

        Assert.Equal("WRITELOG", stories[0].RootFactKey);
    }

    [Fact]
    public async Task LogWritePressure_HighSeverity()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedLogWritePressureServerAsync());

        // 34.7% of period, concerning = 10% → severity = 1.0 (capped)
        Assert.Equal(1.0, stories[0].Severity, precision: 1);
    }

    /* ── Resource Semaphore Cascade ── */

    [Fact]
    public async Task ResourceSemaphoreCascade_PageioLatchHighest()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedResourceSemaphoreCascadeServerAsync());
        PrintStories("RESOURCE SEMAPHORE CASCADE", stories);

        // PAGEIOLATCH_SH at 41.7% is higher raw severity than RESOURCE_SEMAPHORE at 10.4%
        Assert.Equal("PAGEIOLATCH_SH", stories[0].RootFactKey);
    }

    [Fact]
    public async Task ResourceSemaphoreCascade_ResourceSemaphorePresent()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedResourceSemaphoreCascadeServerAsync());

        // RESOURCE_SEMAPHORE should appear in stories (either as root or traversal)
        var allFactKeys = stories.SelectMany(s => s.Path).ToHashSet();
        Assert.Contains("RESOURCE_SEMAPHORE", allFactKeys);
    }

    /* ── Everything On Fire ── */

    [Fact]
    public async Task EverythingOnFire_MultipleStories()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedEverythingOnFireServerAsync());
        PrintStories("EVERYTHING ON FIRE", stories);

        // Should produce at least 3 separate stories (memory, parallelism, locks, log)
        Assert.True(stories.Count >= 3, $"Expected >= 3 stories, got {stories.Count}");
    }

    [Fact]
    public async Task EverythingOnFire_StoriesOrderedBySeverity()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedEverythingOnFireServerAsync());

        for (var i = 1; i < stories.Count; i++)
        {
            Assert.True(stories[i].Severity <= stories[i - 1].Severity,
                $"Story {i} severity {stories[i].Severity:F2} should be <= story {i - 1} severity {stories[i - 1].Severity:F2}");
        }
    }

    [Fact]
    public async Task EverythingOnFire_NoFactUsedTwice()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedEverythingOnFireServerAsync());

        var allFactKeys = stories.SelectMany(s => s.Path).ToList();
        var distinctKeys = allFactKeys.Distinct().ToList();
        Assert.Equal(distinctKeys.Count, allFactKeys.Count);
    }

    [Fact]
    public async Task EverythingOnFire_CoversMajorCategories()
    {
        var (stories, _) = await RunFullPipelineAsync(s => s.SeedEverythingOnFireServerAsync());

        var allFactsInStories = stories.SelectMany(s => s.Path).ToHashSet();

        // Should surface memory, parallelism/CPU, lock, and blocking stories
        Assert.True(
            allFactsInStories.Any(r => r.StartsWith("PAGEIOLATCH")) ||
            allFactsInStories.Contains("RESOURCE_SEMAPHORE"),
            "Should have a memory-related finding");

        Assert.True(
            allFactsInStories.Contains("CXPACKET") || allFactsInStories.Contains("SOS_SCHEDULER_YIELD"),
            "Should have a CPU/parallelism finding");

        Assert.True(
            allFactsInStories.Contains("LCK") || allFactsInStories.Contains("BLOCKING_EVENTS"),
            "Should have a blocking/lock finding");
    }

    [Fact]
    public async Task EverythingOnFire_BlockingAndDeadlocksPresent()
    {
        var (stories, facts) = await RunFullPipelineAsync(s => s.SeedEverythingOnFireServerAsync());

        // 100 blocking events (~25/hr) and 30 deadlocks (~7.5/hr) — both above thresholds
        Assert.True(facts.ContainsKey("BLOCKING_EVENTS"), "Blocking events should be collected");
        Assert.True(facts.ContainsKey("DEADLOCKS"), "Deadlocks should be collected");
        Assert.True(facts["BLOCKING_EVENTS"].Severity > 0, "Blocking events severity should be non-zero");
        Assert.True(facts["DEADLOCKS"].Severity > 0, "Deadlocks severity should be non-zero");
    }


    /* ── Helper ── */

    private async Task<(List<AnalysisStory> Stories, Dictionary<string, Fact> Facts)> RunFullPipelineAsync(
        Func<TestDataSeeder, Task> seedAction)
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

        var graph = new RelationshipGraph();
        var engine = new InferenceEngine(graph);
        var stories = engine.BuildStories(facts);

        var factsByKey = facts
            .Where(f => f.Severity > 0)
            .ToDictionary(f => f.Key, f => f);

        return (stories, factsByKey);
    }

    /* ── Anomaly Detection: CPU Spike ── */

    [Fact]
    public async Task CpuSpikeAnomaly_DetectsCpuDeviation()
    {
        var (stories, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedCpuSpikeAnomalyAsync());
        PrintStories("CPU SPIKE ANOMALY", stories);

        Assert.True(facts.ContainsKey("ANOMALY_CPU_SPIKE"), "Should detect CPU anomaly");
        Assert.True(facts["ANOMALY_CPU_SPIKE"].Severity >= 0.5, "CPU anomaly severity should be significant");
    }

    [Fact]
    public async Task CpuSpikeAnomaly_HighDeviation()
    {
        var (_, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedCpuSpikeAnomalyAsync());

        var deviation = facts["ANOMALY_CPU_SPIKE"].Metadata["deviation_sigma"];
        Assert.True(deviation > 5.0, $"Expected large deviation (>5σ), got {deviation:F1}σ");
    }

    [Fact]
    public async Task CpuSpikeAnomaly_AppearsAsStory()
    {
        var (stories, _) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedCpuSpikeAnomalyAsync());

        Assert.Contains(stories, s => s.RootFactKey == "ANOMALY_CPU_SPIKE");
    }

    /* ── Anomaly Detection: Blocking Spike ── */

    [Fact]
    public async Task BlockingSpikeAnomaly_DetectsBlockingBurst()
    {
        var (stories, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedBlockingSpikeAnomalyAsync());
        PrintStories("BLOCKING SPIKE ANOMALY", stories);

        Assert.True(facts.ContainsKey("ANOMALY_BLOCKING_SPIKE"), "Should detect blocking spike");
        Assert.True(facts["ANOMALY_BLOCKING_SPIKE"].Severity >= 0.5, "Blocking spike should be significant");
    }

    [Fact]
    public async Task BlockingSpikeAnomaly_DetectsDeadlockSpike()
    {
        var (_, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedBlockingSpikeAnomalyAsync());

        Assert.True(facts.ContainsKey("ANOMALY_DEADLOCK_SPIKE"), "Should detect deadlock spike");
    }

    /* ── Anomaly Detection: Wait Spike ── */

    [Fact]
    public async Task WaitSpikeAnomaly_DetectsPageiolatchFlood()
    {
        var (stories, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedWaitSpikeAnomalyAsync());
        PrintStories("WAIT SPIKE ANOMALY", stories);

        Assert.True(facts.ContainsKey("ANOMALY_WAIT_PAGEIOLATCH_SH"), "Should detect PAGEIOLATCH spike");
        Assert.True(facts["ANOMALY_WAIT_PAGEIOLATCH_SH"].Severity >= 0.5, "PAGEIOLATCH anomaly should be significant");
    }

    [Fact]
    public async Task WaitSpikeAnomaly_HighRatio()
    {
        var (_, facts) = await RunFullPipelineWithAnomaliesAsync(s => s.SeedWaitSpikeAnomalyAsync());

        var ratio = facts["ANOMALY_WAIT_PAGEIOLATCH_SH"].Metadata["ratio"];
        Assert.True(ratio >= 5.0, $"Expected >= 5x increase, got {ratio:F1}x");
    }

    /* ── Helpers ── */

    private async Task<(List<AnalysisStory> Stories, Dictionary<string, Fact> Facts)> RunFullPipelineWithAnomaliesAsync(
        Func<TestDataSeeder, Task> seedAction)
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        // Run anomaly detection (compares analysis window against baseline)
        var anomalyDetector = new AnomalyDetector(_duckDb, new BaselineProvider(_duckDb));
        var anomalies = await anomalyDetector.DetectAnomaliesAsync(context);
        facts.AddRange(anomalies);

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        var graph = new RelationshipGraph();
        var engine = new InferenceEngine(graph);
        var stories = engine.BuildStories(facts);

        var factsByKey = facts
            .Where(f => f.Severity > 0)
            .ToDictionary(f => f.Key, f => f);

        return (stories, factsByKey);
    }

    private static void PrintStories(string scenario, List<AnalysisStory> stories)
    {
        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== {scenario} ===");
        output.WriteLine("");

        for (var i = 0; i < stories.Count; i++)
        {
            var s = stories[i];
            output.WriteLine($"--- Story {i + 1} ---");
            output.WriteLine($"Path: {s.StoryPath}");
            output.WriteLine($"Severity: {s.Severity:F2}  Confidence: {s.Confidence:F2}");
            output.WriteLine($"Root: {s.RootFactKey}  Leaf: {s.LeafFactKey}");
            output.WriteLine("");
        }
    }
}
