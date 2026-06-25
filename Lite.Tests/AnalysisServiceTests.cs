using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for AnalysisService — the full orchestration pipeline.
/// </summary>
public class AnalysisServiceTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;

    public AnalysisServiceTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        var dbPath = Path.Combine(_tempDir, "test.duckdb");
        _duckDb = new DuckDbInitializer(dbPath);
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

    [Fact]
    public async Task AnalyzeAsync_MemoryStarved_ProducesFindings()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var service = CreateTestService();
        var context = TestDataSeeder.CreateTestContext();
        var findings = await service.AnalyzeAsync(context);

        Assert.NotEmpty(findings);
        Assert.Contains(findings, f => f.RootFactKey.StartsWith("PAGEIOLATCH"));

        // Output for inspection
        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== AnalysisService: {findings.Count} findings ===");
        foreach (var f in findings)
        {
            output.WriteLine($"[{f.Severity:F2}] {f.StoryPath}");
            output.WriteLine(f.StoryText);
            output.WriteLine("");
        }
    }

    [Fact]
    public async Task AnalyzeAsync_CleanServer_ProducesNoFindings()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var service = CreateTestService();
        var context = TestDataSeeder.CreateTestContext();
        var findings = await service.AnalyzeAsync(context);

        // Absolution stories are not persisted (severity 0)
        Assert.Empty(findings);
    }

    [Fact]
    public async Task AnalyzeAsync_SetsLastAnalysisTime()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedCleanServerAsync();

        var service = CreateTestService();
        Assert.Null(service.LastAnalysisTime);

        await service.AnalyzeAsync(TestDataSeeder.CreateTestContext());

        Assert.NotNull(service.LastAnalysisTime);
    }

    [Fact]
    public async Task AnalyzeAsync_RaisesAnalysisCompletedEvent()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var service = CreateTestService();
        AnalysisCompletedEventArgs? eventArgs = null;
        service.AnalysisCompleted += (_, args) => eventArgs = args;

        var context = TestDataSeeder.CreateTestContext();
        await service.AnalyzeAsync(context);

        Assert.NotNull(eventArgs);
        Assert.Equal(context.ServerId, eventArgs.ServerId);
        Assert.NotEmpty(eventArgs.Findings);
    }

    [Fact]
    public async Task GetLatestFindings_ReturnsPersistedResults()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedLockContentionServerAsync();

        var service = CreateTestService();
        var context = TestDataSeeder.CreateTestContext();

        // Run analysis to persist findings
        var findings = await service.AnalyzeAsync(context);
        Assert.NotEmpty(findings);

        // Retrieve without re-running
        var retrieved = await service.GetLatestFindingsAsync(context.ServerId);
        Assert.Equal(findings.Count, retrieved.Count);
    }

    [Fact]
    public async Task MuteFinding_ExcludesFromNextRun()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedLogWritePressureServerAsync();

        var service = CreateTestService();
        var context = TestDataSeeder.CreateTestContext();

        // First run
        var findings1 = await service.AnalyzeAsync(context);
        var writelogFinding = findings1.FirstOrDefault(f => f.RootFactKey == "WRITELOG");
        Assert.NotNull(writelogFinding);

        // Mute the WRITELOG finding
        await service.MuteFindingAsync(writelogFinding);

        // Re-seed and re-run — WRITELOG should be excluded
        await seeder.SeedLogWritePressureServerAsync();
        var findings2 = await service.AnalyzeAsync(context);

        Assert.DoesNotContain(findings2, f => f.RootFactKey == "WRITELOG");
    }

    [Fact]
    public async Task AnalyzeAsync_InsufficientData_ReturnsEmptyWithMessage()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        // Set 72h minimum — test data is only 4h, so this should be rejected
        var service = new AnalysisService(_duckDb) { MinimumDataHours = 72 };
        var context = TestDataSeeder.CreateTestContext();
        var findings = await service.AnalyzeAsync(context);

        Assert.Empty(findings);
        Assert.NotNull(service.InsufficientDataMessage);
        Assert.Contains("Not enough data", service.InsufficientDataMessage);
    }

    [Fact]
    public async Task AnalyzeAsync_BlockingScenario_IncludesBlockingFindings()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedBlockingThreadExhaustionServerAsync();

        var service = CreateTestService();
        var findings = await service.AnalyzeAsync(TestDataSeeder.CreateTestContext());

        Assert.NotEmpty(findings);

        // Should have blocking events in findings
        Assert.Contains(findings, f =>
            f.RootFactKey == "BLOCKING_EVENTS" || f.StoryPath.Contains("BLOCKING_EVENTS"));

        var output = TestContext.Current.TestOutputHelper!;
        output.WriteLine($"=== Blocking Thread Exhaustion: {findings.Count} findings ===");
        foreach (var f in findings)
        {
            output.WriteLine($"[{f.Severity:F2}] {f.StoryPath}");
            output.WriteLine(f.StoryText);
            output.WriteLine("");
        }
    }

    /// <summary>
    /// Creates an AnalysisService with MinimumDataHours=0 for testing.
    /// Test scenarios use a 4-hour window which is below the production 72h minimum.
    /// </summary>
    private AnalysisService CreateTestService()
    {
        return new AnalysisService(_duckDb) { MinimumDataHours = 0 };
    }
}
