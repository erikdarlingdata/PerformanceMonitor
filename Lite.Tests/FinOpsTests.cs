using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// End-to-end scenario tests for the FinOps recommendation engine and High Impact scorer.
/// Each test seeds a specific server profile into DuckDB, runs the recommendation or
/// scoring engine, and validates the output (categories, findings, severity, savings).
/// </summary>
public class FinOpsTests : IDisposable
{
    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly DuckDbInitializer _duckDb;

    public FinOpsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "FinOpsTests_" + Guid.NewGuid().ToString("N")[..8]);
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

    /* ── Over-Provisioned Enterprise ── */

    [Fact]
    public async Task OverProvisionedEnterprise_CpuRightSizingFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedOverProvisionedEnterpriseAsync());
        PrintRecommendations("OVER-PROVISIONED ENTERPRISE (CPU)", recs);

        Assert.Contains(recs, r => r.Category == "Compute" && r.Finding.Contains("CPU", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task OverProvisionedEnterprise_MemoryRightSizingFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedOverProvisionedEnterpriseAsync());
        PrintRecommendations("OVER-PROVISIONED ENTERPRISE (Memory)", recs);

        Assert.Contains(recs, r => r.Category == "Memory" && r.Finding.Contains("memory", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task OverProvisionedEnterprise_VmRightSizingPrescribesTargets()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedOverProvisionedEnterpriseAsync());
        PrintRecommendations("OVER-PROVISIONED ENTERPRISE (VM)", recs);

        var hwRecs = recs.Where(r => r.Category == "Hardware").ToList();
        Assert.True(hwRecs.Count > 0 || recs.Any(r => r.Finding.Contains("reduce", StringComparison.OrdinalIgnoreCase)),
            "Should have prescriptive hardware or compute recommendations");
    }

    /* ── Idle Databases ── */

    [Fact]
    public async Task IdleDatabases_DormantDetectionFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesAsync());
        PrintRecommendations("IDLE DATABASES (Dormant)", recs);

        Assert.Contains(recs, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task IdleDatabases_CostShareCalculated()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesAsync(), monthlyCost: 10000m);
        PrintRecommendations("IDLE DATABASES (Cost Share)", recs);

        var dormant = recs.FirstOrDefault(r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(dormant);
        Assert.True(dormant.EstMonthlySavings > 0, "Should calculate cost share when monthly budget is set");
    }

    [Fact]
    public async Task IdleDatabases_NoCostShareWhenNoBudget()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesAsync(), monthlyCost: 0m);
        PrintRecommendations("IDLE DATABASES (No Budget)", recs);

        var dormant = recs.FirstOrDefault(r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.NotNull(dormant);
        Assert.Null(dormant.EstMonthlySavings);
    }

    /* ── High Impact Query Skew ── */

    [Fact]
    public async Task HighImpactSkew_DominantQueryScoresHighest()
    {
        var results = await RunHighImpactAsync(s => s.SeedHighImpactQuerySkewAsync());
        PrintHighImpact("HIGH IMPACT SKEW (Dominant)", results);

        Assert.True(results.Count > 0, "Should find high-impact queries");
        Assert.True(results[0].CpuShare > 50, $"Top query should have >50% CPU share, got {results[0].CpuShare}");
    }

    [Fact]
    public async Task HighImpactSkew_DominantQueryHighScore()
    {
        var results = await RunHighImpactAsync(s => s.SeedHighImpactQuerySkewAsync());
        PrintHighImpact("HIGH IMPACT SKEW (Score)", results);

        Assert.True(results.Count > 0);
        Assert.True(results[0].ImpactScore >= 80, $"Dominant query should score >= 80, got {results[0].ImpactScore}");
    }

    /* ── HighImpactScorer Pure Function Tests ── */

    [Fact]
    public void HighImpactScorer_ScoresKnownData()
    {
        var rows = new List<HighImpactQueryRow>
        {
            new() { QueryHash = "A", TotalCpuMs = 1000, TotalDurationMs = 2000, TotalReads = 500000, TotalWrites = 1000, TotalMemoryMb = 100, TotalExecutions = 100 },
            new() { QueryHash = "B", TotalCpuMs = 100, TotalDurationMs = 200, TotalReads = 50000, TotalWrites = 100, TotalMemoryMb = 10, TotalExecutions = 1000 },
            new() { QueryHash = "C", TotalCpuMs = 50, TotalDurationMs = 100, TotalReads = 25000, TotalWrites = 50, TotalMemoryMb = 5, TotalExecutions = 500 },
        };

        var scored = HighImpactScorer.Score(rows, topN: 3);

        Assert.Equal(3, scored.Count);
        // Query A should have highest impact score (dominates CPU, duration, reads, writes, memory)
        Assert.Equal("A", scored[0].QueryHash);
        Assert.True(scored[0].ImpactScore > scored[1].ImpactScore);
        // CPU share for A should be ~87% (1000/1150)
        Assert.True(scored[0].CpuShare > 80);
    }

    [Fact]
    public void HighImpactScorer_EmptyInput_ReturnsEmpty()
    {
        var scored = HighImpactScorer.Score(new List<HighImpactQueryRow>(), topN: 10);
        Assert.Empty(scored);
    }

    /* ── Long-Running Jobs ── */

    [Fact]
    public async Task LongRunningJobs_MaintenanceRecommendationFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedLongRunningJobsAsync());
        PrintRecommendations("LONG RUNNING JOBS", recs);

        Assert.Contains(recs, r => r.Category == "Maintenance");
    }

    /* ── Clean Server ── */

    [Fact]
    public async Task CleanServer_NoDuckDbRecommendations()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedCleanFinOpsServerAsync());
        PrintRecommendations("CLEAN SERVER", recs);

        // Filter to only DuckDB-based checks (exclude live SQL failures that silently catch)
        var duckDbCategories = new HashSet<string>
        {
            "Compute", "Memory", "Hardware", "Databases", "Maintenance", "Storage", "Cloud"
        };
        var duckDbRecs = recs.Where(r => duckDbCategories.Contains(r.Category)).ToList();
        Assert.Empty(duckDbRecs);
    }

    /* ── Stable CPU — Reserved Capacity ── */

    [Fact]
    public async Task StableCpu_ReservedCapacityFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedStableCpuForReservedCapacityAsync());
        PrintRecommendations("STABLE CPU (Reserved)", recs);

        Assert.Contains(recs, r => r.Category == "Cloud" &&
            (r.Finding.Contains("reserved", StringComparison.OrdinalIgnoreCase) ||
             r.Finding.Contains("Reserved", StringComparison.Ordinal)));
    }

    /* ── Bursty CPU — Reserved Capacity Should NOT Fire ── */

    [Fact]
    public async Task BurstyCpu_ReservedCapacityDoesNotFire()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedBurstyCpuAsync());
        PrintRecommendations("BURSTY CPU", recs);

        Assert.DoesNotContain(recs, r => r.Category == "Cloud");
    }

    /* ── VM Right-Sizing ── */

    [Fact]
    public async Task VmRightSizing_PrescribesSpecificTargets()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedVmRightSizingTargetAsync());
        PrintRecommendations("VM RIGHT-SIZING", recs);

        var hwRecs = recs.Where(r => r.Category == "Hardware").ToList();
        Assert.True(hwRecs.Count > 0, "Should produce hardware recommendations");
        // Should mention specific numbers like core counts or GB values
        Assert.True(hwRecs.Any(r => r.Detail.Contains("8") || r.Detail.Contains("64") || r.Detail.Contains("reduce", StringComparison.OrdinalIgnoreCase)),
            "Should prescribe specific reduction targets");
    }

    /* ── Low IO Latency — Storage Tier ── */

    [Fact]
    public async Task LowIoLatency_StorageTierFires()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedLowIoLatencyAsync());
        PrintRecommendations("LOW IO LATENCY", recs);

        Assert.Contains(recs, r => r.Category == "Storage");
    }

    /* ── Helpers ── */

    private async Task<List<RecommendationRow>> RunRecommendationsAsync(
        Func<TestDataSeeder, Task> seedAction, decimal monthlyCost = 10000m)
    {
        await _duckDb.InitializeAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var dataService = new LocalDataService(_duckDb);
        return await dataService.GetRecommendationsAsync(TestDataSeeder.TestServerId, "", "", monthlyCost);
    }

    private async Task<List<HighImpactQueryRow>> RunHighImpactAsync(
        Func<TestDataSeeder, Task> seedAction, int hoursBack = 24)
    {
        await _duckDb.InitializeAsync();

        var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var dataService = new LocalDataService(_duckDb);
        return await dataService.GetHighImpactQueriesAsync(TestDataSeeder.TestServerId, hoursBack);
    }

    private static void PrintRecommendations(string scenario, List<RecommendationRow> recs)
    {
        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== {scenario} ===");
        output.WriteLine("");

        for (var i = 0; i < recs.Count; i++)
        {
            var r = recs[i];
            output.WriteLine($"--- Rec {i + 1} ---");
            output.WriteLine($"Category: {r.Category}  Severity: {r.Severity}  Confidence: {r.Confidence}");
            output.WriteLine($"Finding: {r.Finding}");
            output.WriteLine($"Detail: {r.Detail}");
            output.WriteLine($"Est Savings: {r.EstMonthlySavingsDisplay}");
            output.WriteLine("");
        }
    }

    private static void PrintHighImpact(string scenario, List<HighImpactQueryRow> rows)
    {
        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== {scenario} ===");
        output.WriteLine("");

        for (var i = 0; i < rows.Count; i++)
        {
            var r = rows[i];
            output.WriteLine($"--- Query {i + 1} ---");
            output.WriteLine($"Hash: {r.QueryHash}  Impact: {r.ImpactScore}  CPU%: {r.CpuShare}");
            output.WriteLine($"CPU: {r.TotalCpuMs:N0}ms  Duration: {r.TotalDurationMs:N0}ms  Reads: {r.TotalReads:N0}  Execs: {r.TotalExecutions:N0}");
            output.WriteLine("");
        }
    }
}
