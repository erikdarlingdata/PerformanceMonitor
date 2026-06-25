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
/// Tests for FindingStore: persist, retrieve, mute, and cleanup findings.
/// </summary>
public class FindingStoreTests : IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;

    public FindingStoreTests()
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

    private async Task InitializeWithAnalysisAsync()
    {
        await _duckDb.InitializeAsync();
        await _duckDb.InitializeAnalysisSchemaAsync();
    }

    [Fact]
    public async Task SaveFindings_PersistsAndReturnsFindings()
    {
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var stories = CreateTestStories();

        var saved = await store.SaveFindingsAsync(stories, context);

        Assert.Equal(2, saved.Count);
        Assert.All(saved, f => Assert.NotEmpty(f.StoryPathHash));
        Assert.All(saved, f => Assert.Equal(context.ServerId, f.ServerId));
    }

    [Fact]
    public async Task GetLatestFindings_ReturnsPersistedData()
    {
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var stories = CreateTestStories();

        await store.SaveFindingsAsync(stories, context);

        var findings = await store.GetLatestFindingsAsync(context.ServerId);

        Assert.Equal(2, findings.Count);
        // Should be ordered by severity descending
        Assert.True(findings[0].Severity >= findings[1].Severity);
    }

    [Fact]
    public async Task SaveFindings_RoundTripsIncidentId()
    {
        // Correlate-and-focus slice 2: the incident id persists through the schema (incident_id
        // column added at analysis-schema v3) and reads back on every finding.
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var stories = CreateTestStories();
        foreach (var s in stories)
            s.IncidentId = "incident_abc";

        await store.SaveFindingsAsync(stories, context);
        var findings = await store.GetLatestFindingsAsync(context.ServerId);

        Assert.Equal(2, findings.Count);
        Assert.All(findings, f => Assert.Equal("incident_abc", f.IncidentId));
    }

    [Fact]
    public async Task GetRecentFindings_RespectsTimeRange()
    {
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        await store.SaveFindingsAsync(CreateTestStories(), context);

        // Should find them within 1 hour
        var found = await store.GetRecentFindingsAsync(context.ServerId, hoursBack: 1);
        Assert.Equal(2, found.Count);

        // Different server should find nothing
        var empty = await store.GetRecentFindingsAsync(serverId: -1, hoursBack: 1);
        Assert.Empty(empty);
    }

    [Fact]
    public async Task MuteStory_ExcludesFromFutureSaves()
    {
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var stories = CreateTestStories();

        // Mute the first story's hash
        await store.MuteStoryAsync(context.ServerId, stories[0].StoryPathHash, stories[0].StoryPath, "Test mute");

        // Save — the muted story should be excluded
        var saved = await store.SaveFindingsAsync(stories, context);

        Assert.Single(saved);
        Assert.Equal(stories[1].StoryPathHash, saved[0].StoryPathHash);
    }

    [Fact]
    public async Task CleanupOldFindings_RemovesExpiredData()
    {
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        await store.SaveFindingsAsync(CreateTestStories(), context);

        // Cleanup with 0 days retention should remove everything
        await store.CleanupOldFindingsAsync(retentionDays: 0);

        var findings = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Empty(findings);
    }

    [Fact]
    public async Task FullPipeline_FindingStoreIntegration()
    {
        await InitializeWithAnalysisAsync();

        // Seed test data
        var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        // Run pipeline
        var collector = new DuckDbFactCollector(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var facts = await collector.CollectFactsAsync(context);

        var scorer = new FactScorer();
        scorer.ScoreAll(facts);

        var graph = new RelationshipGraph();
        var engine = new InferenceEngine(graph);
        var stories = engine.BuildStories(facts);

        // Persist
        var store = new FindingStore(_duckDb);
        var saved = await store.SaveFindingsAsync(stories, context);

        Assert.NotEmpty(saved);

        // Retrieve
        var retrieved = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Equal(saved.Count, retrieved.Count);

        // Verify story path hash survived round-trip
        var firstSaved = saved.OrderByDescending(f => f.Severity).First();
        var firstRetrieved = retrieved.First(); // Already ordered by severity desc
        Assert.Equal(firstSaved.StoryPathHash, firstRetrieved.StoryPathHash);
    }

    [Fact]
    public async Task SaveFindings_BatchedSingleCall_PersistsAllRows()
    {
        // D0 cost prereq: SaveFindingsAsync now uses ONE read lock + ONE connection per
        // call, reused for the mute read and every insert. This exercises a batch larger
        // than the two-story helper to confirm the connection-reuse path persists every
        // surviving row (and preserves severity-desc ordering on read-back).
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        var stories = new System.Collections.Generic.List<AnalysisStory>();
        for (int i = 0; i < 10; i++)
        {
            stories.Add(new AnalysisStory
            {
                RootFactKey = $"FACT_{i}",
                RootFactValue = 0.1 * (i + 1),
                Severity = 0.1 * (i + 1),
                Confidence = 1.0,
                Category = "waits",
                Path = [$"FACT_{i}"],
                StoryPath = $"FACT_{i}",
                StoryPathHash = $"hash_{i:D4}",
                StoryText = $"Batched story {i}.",
                FactCount = 1
            });
        }

        var saved = await store.SaveFindingsAsync(stories, context);
        Assert.Equal(10, saved.Count);

        var retrieved = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Equal(10, retrieved.Count);

        // Ordered by severity descending on read-back.
        for (int i = 1; i < retrieved.Count; i++)
            Assert.True(retrieved[i - 1].Severity >= retrieved[i].Severity);

        // Every hash survived the batched insert.
        var savedHashes = new System.Collections.Generic.HashSet<string>();
        foreach (var f in saved) savedHashes.Add(f.StoryPathHash);
        Assert.Equal(10, savedHashes.Count);
    }

    [Fact]
    public async Task SaveFindings_BatchedCall_StillFiltersMutedAndAbsolution()
    {
        // The batching refactor must preserve mute filtering and the severity<=0 skip
        // within the single-connection loop.
        await InitializeWithAnalysisAsync();

        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        var stories = CreateTestStories();

        // Add an absolution story (severity 0) that must be skipped.
        stories.Add(new AnalysisStory
        {
            RootFactKey = "HEALTHY",
            RootFactValue = 0.0,
            Severity = 0.0,
            Confidence = 1.0,
            Category = "waits",
            Path = ["HEALTHY"],
            StoryPath = "HEALTHY",
            StoryPathHash = "healthy_hash",
            StoryText = "All clear.",
            FactCount = 1
        });

        // Mute the first real story.
        await store.MuteStoryAsync(context.ServerId, stories[0].StoryPathHash, stories[0].StoryPath, "Test mute");

        var saved = await store.SaveFindingsAsync(stories, context);

        // Only the second real story survives (first muted, absolution skipped).
        Assert.Single(saved);
        Assert.Equal(stories[1].StoryPathHash, saved[0].StoryPathHash);
    }

    private static System.Collections.Generic.List<AnalysisStory> CreateTestStories()
    {
        return
        [
            new AnalysisStory
            {
                RootFactKey = "PAGEIOLATCH_SH",
                RootFactValue = 1.2,
                Severity = 1.2,
                Confidence = 0.75,
                Category = "waits",
                Path = ["PAGEIOLATCH_SH", "RESOURCE_SEMAPHORE"],
                StoryPath = "PAGEIOLATCH_SH → RESOURCE_SEMAPHORE",
                StoryPathHash = "abc123def456",
                StoryText = "Test story about memory pressure.",
                LeafFactKey = "RESOURCE_SEMAPHORE",
                LeafFactValue = 0.8,
                FactCount = 2
            },
            new AnalysisStory
            {
                RootFactKey = "SOS_SCHEDULER_YIELD",
                RootFactValue = 0.7,
                Severity = 0.7,
                Confidence = 1.0,
                Category = "waits",
                Path = ["SOS_SCHEDULER_YIELD"],
                StoryPath = "SOS_SCHEDULER_YIELD",
                StoryPathHash = "xyz789ghi012",
                StoryText = "Test story about CPU pressure.",
                FactCount = 1
            }
        ];
    }
}
