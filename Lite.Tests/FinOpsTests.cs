using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
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
public class FinOpsTests : IClassFixture<SharedDuckDbFixture>
{
    private readonly DuckDbInitializer _duckDb;

    public FinOpsTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
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

    /* ── Right-sizing advice needs a measurement, and a server whose hardware is its own ── */

    [Fact]
    public async Task NoCpuSamples_CpuAndVmRightSizingAdviseNothing()
    {
        // Size and memory rows, no CPU rows in the window: the CPU P95 reads 0 only because nothing was measured.
        var recs = await RunRecommendationsAsync(s => s.SeedRightSizingScenarioAsync(engineEdition: 3, withCpuSamples: false));
        PrintRecommendations("NO CPU SAMPLES", recs);

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("CPU over-provisioned", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Category == "Hardware");
        // Only the CPU-dependent rules stand down: memory is its own measurement and still advises.
        Assert.Contains(recs, r => r.Finding.StartsWith("Memory over-provisioned", StringComparison.Ordinal));
    }

    [Fact]
    public async Task AzureSqlDatabase_MemoryAndVmRightSizingAdviseNothing_BecauseItsMemoryComesWithItsServiceObjective()
    {
        // The database uses 40,960 MB of its own 167,117 MB memory limit, a share that advises on any other edition. Its
        // service objective names 32 vCores, which is the CPU it is given. Its memory cannot be resized on its own,
        // so the memory and VM rules have nothing to recommend.
        var recs = await RunRecommendationsAsync(s => s.SeedRightSizingScenarioAsync(engineEdition: 5, withCpuSamples: true, vcoreCount: 32));
        PrintRecommendations("AZURE SQL DATABASE MEMORY AND VM RULES", recs);

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("Memory over-provisioned", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Category == "Hardware");
        // The CPU rule is not one of the two that stand down on a database: it reads the vCores the objective names, and it
        // calls them vCores, as the utilization card does, in the finding and in the detail.
        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU over-provisioned", StringComparison.Ordinal));
        Assert.StartsWith("CPU over-provisioned (32 vCores, P95 = ", cpu.Finding, StringComparison.Ordinal);
        Assert.Contains("across 32 vCores. Consider reducing to ~", cpu.Detail, StringComparison.Ordinal);
        Assert.EndsWith(" vCores.", cpu.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain(" cores", cpu.Finding + cpu.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AzureSqlDatabaseWithNoVcores_CpuRightSizingAdvisesNothing_BecauseTheStoredSchedulerCountIsNotTheCpuItIsGiven()
    {
        // A DTU-model objective names no vCores. The stored cpu_count (32 here) is the schedulers the database can see, not the
        // CPU it is given, so the CPU rule has no count to work from.
        var recs = await RunRecommendationsAsync(s => s.SeedRightSizingScenarioAsync(engineEdition: 5, withCpuSamples: true));
        PrintRecommendations("AZURE SQL DATABASE, NO VCORES", recs);

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("CPU over-provisioned", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Finding.Contains("32 cores", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Category == "Hardware");
    }

    [Theory]
    [InlineData(3)]   // SQL Server Enterprise
    [InlineData(8)]   // Azure SQL Managed Instance: its memory is its own
    public async Task WithCpuSamplesOffAzureSqlDatabase_CpuMemoryAndVmRightSizingStillAdvise(int engineEdition)
    {
        var recs = await RunRecommendationsAsync(s => s.SeedRightSizingScenarioAsync(engineEdition, withCpuSamples: true));
        PrintRecommendations($"RIGHT-SIZING UNCHANGED (edition {engineEdition})", recs);

        // Off an Azure SQL Database the count is a CPU count and the word is the one it always was.
        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU over-provisioned", StringComparison.Ordinal));
        Assert.StartsWith("CPU over-provisioned (32 cores, P95 = ", cpu.Finding, StringComparison.Ordinal);
        Assert.Contains("across 32 cores. Consider reducing to ~", cpu.Detail, StringComparison.Ordinal);
        Assert.EndsWith(" cores.", cpu.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("vCores", cpu.Finding + cpu.Detail, StringComparison.Ordinal);
        Assert.Contains(recs, r => r.Finding.StartsWith("Memory over-provisioned", StringComparison.Ordinal));
        Assert.Contains(recs, r => r.Category == "Hardware" && r.Finding.StartsWith("CPU: reduce from 32", StringComparison.Ordinal));
        Assert.Contains(recs, r => r.Category == "Hardware" && r.Finding.StartsWith("Memory: reduce from 256GB", StringComparison.Ordinal));
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

    [Fact]
    public async Task IdleDatabases_FourHoursOfHistory_AdviseNothing_BecauseSevenDaysWereNotObserved()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesWithFourHoursOfHistoryAsync());

        Assert.DoesNotContain(recs, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task IdleDatabases_SixAndAHalfDaysOfHistory_AdviseNothing()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesWithSixAndAHalfDaysOfHistoryAsync());

        Assert.DoesNotContain(recs, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public async Task IdleDatabases_SevenDaysOfHistory_StillAdvise()
    {
        var recs = await RunRecommendationsAsync(s => s.SeedIdleDatabasesAsync());

        var idle = Assert.Single(recs, r => r.Category == "Databases" && r.Finding.Contains("idle", StringComparison.OrdinalIgnoreCase));
        Assert.Contains("No query activity in 7 days", idle.Detail, StringComparison.Ordinal);
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

    /// <summary>
    /// Pure-function proof for <see cref="HoursBackCoveringTestPeriod"/>: the read window it computes must
    /// cover the seeded rows (TestPeriodEnd - 30m) at the worst minute of day, 03:59 UTC — one minute before
    /// TestDataSeeder's UTC-midnight anchor rolls TestPeriodEnd forward to today, which is when a fixed
    /// hoursBack: 24 window (measured from "now") is furthest from the seed. Mirrors
    /// TestDataSeeder.AnchorPeriodEndToUtcMidnight's logic directly since that helper is private.
    /// </summary>
    [Fact]
    public void HighImpactWindowCoversSeedAtWorstHour()
    {
        var worstNowUtc = DateTime.UtcNow.Date.AddHours(3).AddMinutes(59); // 03:59 UTC today
        var midnight = worstNowUtc.Date;
        if (worstNowUtc.Hour < 4) midnight = midnight.AddDays(-1);
        var periodEnd = midnight.AddHours(4);
        var periodStart = periodEnd.AddHours(-4);
        var seedTime = periodEnd.AddMinutes(-30);

        var hoursBack = (int)Math.Ceiling((worstNowUtc - periodStart).TotalHours) + 1;
        var cutoff = worstNowUtc.AddHours(-hoursBack);

        Assert.True(cutoff <= seedTime,
            $"Read window (cutoff {cutoff:O}) must cover the seeded row ({seedTime:O}) at 03:59 UTC, hoursBack={hoursBack}");
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

        var storage = Assert.Single(recs, r => r.Category == "Storage");
        // The seed's samples span 225 minutes: the text names that, not "7 days".
        Assert.Contains("under 3ms across 16 samples over 3 hours", storage.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("the last", storage.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("7 days", storage.Detail, StringComparison.Ordinal);
    }

    /* ── Right-sizing advice: no "X to X", and the window the data covers ── */

    /// <summary>Seeds a 32-core host with the given RAM, <paramref name="cpuSamples"/> CPU samples
    /// <paramref name="spacingMinutes"/> apart, and 16 memory samples.</summary>
    private static Func<TestDataSeeder, Task> RightSizingSeed(long physMb, int cpuSamples, int spacingMinutes) => async s =>
    {
        await s.ClearTestDataAsync();
        await s.SeedFinOpsCpuUtilizationAsync(8, 2, cpuSamples, spacingMinutes);
        await s.SeedMemoryStatsAsync(totalPhysicalMb: physMb, bufferPoolMb: 512, targetMb: physMb);
        await s.SeedServerPropertiesAsync(cpuCount: 32, htRatio: 2, physicalMemMb: physMb);
    };

    [Fact]
    public async Task VmRightSizing_MemoryTargetThatRoundsToTheCurrentGb_GivesNoAdvice()
    {
        // 5000 MB is "4GB" as displayed; the 4096 MB floor is "4GB" too. "reduce from 4GB to 4GB" is not advice.
        var recs = await RunRecommendationsAsync(RightSizingSeed(5000, 9, 15));

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("Memory: reduce from", StringComparison.Ordinal));
        Assert.DoesNotContain(recs, r => r.Finding.Contains("4GB to 4GB", StringComparison.Ordinal));
    }

    [Fact]
    public async Task VmRightSizing_TwoHoursOfSamples_StatesTwoHours_NotSevenDays()
    {
        var recs = await RunRecommendationsAsync(RightSizingSeed(262_144, 9, 15));

        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU: reduce from 32", StringComparison.Ordinal));
        Assert.StartsWith("From 9 samples over 2 hours, P95 CPU", cpu.Detail, StringComparison.Ordinal);
        var memory = Assert.Single(recs, r => r.Finding.StartsWith("Memory: reduce from 256GB", StringComparison.Ordinal));
        Assert.DoesNotContain("7 days", memory.Detail, StringComparison.Ordinal);
        // 16 memory samples 15 minutes apart span 225 minutes: three whole hours, never rounded up to four.
        Assert.Contains("P95 SQL Server memory from 16 samples over 3 hours", memory.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MemoryOverProvisioned_TargetThatPrintsAsTheCurrentGb_GivesNoAdvice()
    {
        // 8704 MB is "8GB" as displayed and the 8192 MB floor is "8GB" too: "of 8GB RAM ... reducing to ~8GB" is not advice.
        var recs = await RunRecommendationsAsync(RightSizingSeed(8704, 9, 15));

        Assert.DoesNotContain(recs, r => r.Finding.StartsWith("Memory over-provisioned", StringComparison.Ordinal));
    }

    [Fact]
    public async Task MemoryOverProvisioned_AtTwoHundredFiftySixGb_NamesTheWindowAndTheTarget()
    {
        var recs = await RunRecommendationsAsync(RightSizingSeed(262_144, 9, 15));

        var memory = Assert.Single(recs, r => r.Finding.StartsWith("Memory over-provisioned", StringComparison.Ordinal));
        Assert.Contains("P95 SQL Server memory from 16 samples over 3 hours", memory.Detail, StringComparison.Ordinal);
        Assert.Contains("Consider reducing to ~8GB", memory.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task VmRightSizing_AWeekOfSamples_StatesTheWholeDaysObserved_NeverMore()
    {
        // 17 samples fall inside the 7-day read: 160 hours = 6 days 16 hours, which reads as 6 whole days (never rounded up to 7).
        var recs = await RunRecommendationsAsync(RightSizingSeed(262_144, 20, 10 * 60));

        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU: reduce from 32", StringComparison.Ordinal));
        Assert.StartsWith("From 17 samples over 6 days, P95 CPU", cpu.Detail, StringComparison.Ordinal);
    }

    [Fact]
    public async Task VmRightSizing_TwoClustersFourDaysApart_NamesTheCountAndSpan_NeverTheLast()
    {
        // Five samples now and five 4 days earlier: the 4 days between them were never observed, so the text
        // says "10 samples over 4 days" and does not claim "the last 4 days".
        var recs = await RunRecommendationsAsync(async s =>
        {
            await RightSizingSeed(262_144, 5, 15)(s);
            await s.SeedFinOpsCpuUtilizationAsync(8, 2, 5, 15, daysBack: 4);
        });

        var cpu = Assert.Single(recs, r => r.Finding.StartsWith("CPU: reduce from 32", StringComparison.Ordinal));
        Assert.StartsWith("From 10 samples over 4 days, P95 CPU", cpu.Detail, StringComparison.Ordinal);
        Assert.DoesNotContain("the last", cpu.Detail, StringComparison.Ordinal);
    }

    /* ── Helpers ── */

    private async Task<List<RecommendationRow>> RunRecommendationsAsync(
        Func<TestDataSeeder, Task> seedAction, decimal monthlyCost = 10000m)
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var dataService = new LocalDataService(_duckDb);
        return await dataService.GetRecommendationsAsync(TestDataSeeder.TestServerId, "", "", monthlyCost);
    }

    private async Task<List<HighImpactQueryRow>> RunHighImpactAsync(
        Func<TestDataSeeder, Task> seedAction, int? hoursBack = null)
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seedAction(seeder);

        var dataService = new LocalDataService(_duckDb);

        // GetHighImpactQueriesAsync reads collection_time >= DateTime.UtcNow.AddHours(-hoursBack), but the
        // seed rows are stamped relative to TestDataSeeder.TestPeriodEnd, which is anchored to the most
        // recent UTC-midnight-plus-4h boundary (#4385) rather than to "now". A fixed hoursBack: 24 window
        // read from "now" can land up to ~28h after that anchor, so shortly before 04:00 UTC each day the
        // seeded rows (TestPeriodEnd - 30m) fall outside a naive 24h lookback. Compute hoursBack from
        // "now" back to TestPeriodStart instead, so the read window always covers the seed at any hour.
        var effectiveHoursBack = hoursBack ?? HoursBackCoveringTestPeriod();
        return await dataService.GetHighImpactQueriesAsync(TestDataSeeder.TestServerId, effectiveHoursBack);
    }

    /// <summary>
    /// Smallest whole hour count such that DateTime.UtcNow.AddHours(-hoursBack) is at or before
    /// TestDataSeeder.TestPeriodStart, at any time of day the suite runs. See <see cref="HighImpactWindowCoversSeedAtWorstHour"/>
    /// for the boundary-case proof (03:59 UTC, the worst minute before the anchor rolls forward a day).
    /// </summary>
    private static int HoursBackCoveringTestPeriod()
        => (int)Math.Ceiling((DateTime.UtcNow - TestDataSeeder.TestPeriodStart).TotalHours) + 1;

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
