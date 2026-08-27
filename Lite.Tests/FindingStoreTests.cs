using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Analysis.Recommendations;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Tests for FindingStore: persist, retrieve, mute, and cleanup findings.
/// </summary>
public class FindingStoreTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly DuckDbInitializer _duckDb;
    private DuckDBConnection? _seedConn;

    public FindingStoreTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
    }

    public void Dispose() => _seedConn?.Dispose();

    /// <summary>
    /// One connection reused for every seeded row — opening a fresh connection per
    /// single-row INSERT measured ~90ms/row and dominated this class's runtime.
    /// </summary>
    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    [Fact]
    public async Task SaveFindings_PersistsAndReturnsFindings()
    {
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
        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        await store.SaveFindingsAsync(CreateTestStories(), context);

        // Cleanup with 0 days retention should remove everything
        await store.CleanupOldFindingsAsync(retentionDays: 0);

        var findings = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Empty(findings);
    }

    [Fact]
    public async Task AnalysisServiceCleanup_RemovesExpiredData()
    {
        // The collection background service schedules findings retention through
        // AnalysisService.CleanupAsync (previously declared but never called — analysis_findings
        // grew unbounded until a size-triggered DB reset wiped it). Prove the wrapper the
        // scheduler now invokes purges through to the store.
        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();
        await store.SaveFindingsAsync(CreateTestStories(), context);

        await new AnalysisService(_duckDb).CleanupAsync(retentionDays: 0);

        var findings = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Empty(findings);
    }

    [Fact]
    public async Task FullPipeline_FindingStoreIntegration()
    {
        // Seed test data
        using var seeder = new TestDataSeeder(_duckDb);
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

    [Fact]
    public async Task MuteStory_AllServersSentinel_PersistsNull_AndFiltersEveryServer()
    {
        // serverId 0 is the MCP "mute across all servers" sentinel. The store must persist it as
        // NULL (the canonical global marker), and the mute must then apply to any real server.
        var store = new FindingStore(_duckDb);
        var stories = CreateTestStories();

        await store.MuteStoryAsync(0, stories[0].StoryPathHash, stories[0].StoryPath, "All-servers mute");

        // Persisted as NULL, not 0.
        Assert.Null(await ReadMutedServerIdAsync(stories[0].StoryPathHash));

        // A save under an arbitrary real server drops the globally-muted story.
        var context = TestDataSeeder.CreateTestContext();
        var saved = await store.SaveFindingsAsync(stories, context);

        Assert.Single(saved);
        Assert.Equal(stories[1].StoryPathHash, saved[0].StoryPathHash);
    }

    [Fact]
    public async Task SaveFindings_HonorsLegacyZeroServerIdMute_AsGlobal()
    {
        // Rows written by the pre-fix all-servers tool path carry a literal server_id = 0. The reader
        // must still honor them as global so those legacy mutes keep muting everywhere.
        var store = new FindingStore(_duckDb);
        var stories = CreateTestStories();

        await InsertLegacyMutedRowAsync(serverId: 0, stories[0].StoryPathHash, stories[0].StoryPath);

        var context = TestDataSeeder.CreateTestContext();
        var saved = await store.SaveFindingsAsync(stories, context);

        Assert.Single(saved);
        Assert.Equal(stories[1].StoryPathHash, saved[0].StoryPathHash);
    }

    [Fact]
    public async Task SaveFindings_PerServerMuteForAnotherServer_DoesNotLeak()
    {
        // A mute scoped to one real server must never filter another server's findings.
        var store = new FindingStore(_duckDb);
        var stories = CreateTestStories();

        // Mute story[0] for a DIFFERENT server (context.ServerId is TestDataSeeder.TestServerId = -999).
        await store.MuteStoryAsync(12345, stories[0].StoryPathHash, stories[0].StoryPath, "Other-server mute");

        var context = TestDataSeeder.CreateTestContext();
        var saved = await store.SaveFindingsAsync(stories, context);

        // Both stories survive here — the other server's mute must not reach this server.
        Assert.Equal(2, saved.Count);
    }

    // ── Persisted RemediationAction round-trip (the Lite copy-paste command parity fix) ──────────

    [Fact]
    public async Task TwoPhaseWrite_PersistsRemediationAction_AndRendersCommandOnReadBack()
    {
        // Mirrors the AnalysisService pipeline: FilterMutedFindingsAsync -> attach the BUILT action ->
        // InsertFindingsAsync persists remediation_action_json. The read-back deserializes it via the
        // shared serializer, and the shared renderer turns it into the copy-paste command — proving the
        // typed action survives the DuckDB round-trip and Lite produces a runnable command from storage.
        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        var stories = new List<AnalysisStory>
        {
            new AnalysisStory
            {
                RootFactKey = "DB_CONFIG",
                RootFactValue = 1.2,
                Severity = 1.2,
                Confidence = 0.9,
                Category = "config",
                Path = ["DB_CONFIG"],
                StoryPath = "DB_CONFIG",
                StoryPathHash = "reco_action_hash",
                StoryText = "auto-shrink is on",
                DatabaseName = "StackOverflow",
                FactCount = 1
            }
        };

        // Phase 1: filter (no insert yet).
        var survivors = await store.FilterMutedFindingsAsync(stories, context);
        var survivor = Assert.Single(survivors);
        Assert.Null(survivor.Remediation); // not built yet

        // Attach the BUILT action between the phases, then insert.
        survivor.Remediation = new RemediationAction(
            "DB_CONFIG", "set", Array.Empty<ForcePlanTarget>(),
            DbConfigTargets: new[] { new DbConfigTarget("StackOverflow", DbConfigSetting.AutoShrinkOff) });
        await store.InsertFindingsAsync(survivors, context);

        // Read back: the action is hydrated and renders the byte-identical command.
        var readBack = await store.GetLatestFindingsAsync(context.ServerId);
        var finding = Assert.Single(readBack);
        Assert.NotNull(finding.Remediation);
        Assert.Equal("DB_CONFIG", finding.Remediation!.FactKey);
        Assert.Equal(
            "ALTER DATABASE [StackOverflow] SET AUTO_SHRINK OFF;",
            LiteRecommendationsReader.BuildCopyPasteSql(finding.Remediation));
    }

    [Fact]
    public async Task SaveFindings_WithNoAction_PersistsNullRemediation_ReadsBackNull()
    {
        // The single-pass SaveFindingsAsync wrapper attaches no action; the column persists NULL and
        // reads back as a null Remediation (no command) — the pre-fix behaviour for action-less rows.
        var store = new FindingStore(_duckDb);
        var context = TestDataSeeder.CreateTestContext();

        await store.SaveFindingsAsync(CreateTestStories(), context);

        var readBack = await store.GetLatestFindingsAsync(context.ServerId);
        Assert.Equal(2, readBack.Count);
        Assert.All(readBack, f => Assert.Null(f.Remediation));
        Assert.All(readBack, f => Assert.Null(LiteRecommendationsReader.BuildCopyPasteSql(f.Remediation)));
    }

    // ── Analysis-schema upgrade path (v3 -> v4 adds remediation_action_json without data loss) ──────

    [Fact]
    public async Task AnalysisSchemaUpgrade_FromV3_AddsRemediationColumn_PreservingExistingRows()
    {
        // Simulate an EXISTING Lite DB at analysis-schema v3 (the incident_id era, no
        // remediation_action_json). InitializeAnalysisSchemaAsync must ALTER the column in via the
        // v4 migration WITHOUT dropping the pre-existing finding row, then a fresh finding with an
        // action persists + reads back — the real upgrade path an existing user hits.
        //
        // This test hand-creates the legacy table shape, so it needs a database WITHOUT the
        // analysis schema — its own private file, NOT the class fixture's, whose schema is
        // already current (CREATE TABLE analysis_findings would collide there).
        var tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(tempDir);
        try
        {
            var legacyDb = new DuckDbInitializer(Path.Combine(tempDir, "test.duckdb"));
            await CreateLegacyV3AnalysisSchemaAsync(legacyDb, legacyFindingId: 4242);

            // Run the upgrade.
            await legacyDb.InitializeAnalysisSchemaAsync();

            // The column now exists.
            Assert.True(await ColumnExistsAsync(legacyDb, "analysis_findings", "remediation_action_json"),
                "v4 migration should add remediation_action_json");
            // The schema version advanced to v4.
            Assert.Equal(AnalysisSchema.CurrentVersion, await ReadAnalysisSchemaVersionAsync(legacyDb));
            // The legacy row survived (no data loss) and reads back with a null action.
            var store = new FindingStore(legacyDb);
            var afterUpgrade = await store.GetLatestFindingsAsync(TestDataSeeder.TestServerId);
            var legacy = Assert.Single(afterUpgrade);
            Assert.Equal(4242, legacy.FindingId);
            Assert.Null(legacy.Remediation);

            // Idempotent: running the init again does not throw or regress the version.
            await legacyDb.InitializeAnalysisSchemaAsync();
            Assert.Equal(AnalysisSchema.CurrentVersion, await ReadAnalysisSchemaVersionAsync(legacyDb));
        }
        finally
        {
            try { Directory.Delete(tempDir, recursive: true); }
            catch { /* Best-effort cleanup */ }
        }
    }

    /// <summary>
    /// Builds an analysis_findings table in the OLD (v3) shape — no remediation_action_json — with
    /// analysis_schema_version = 3 and one pre-existing finding row, exactly what an un-upgraded Lite
    /// DB looks like before this change.
    /// </summary>
    private static async Task CreateLegacyV3AnalysisSchemaAsync(DuckDbInitializer legacyDb, long legacyFindingId)
    {
        using var connection = legacyDb.CreateConnection();
        await connection.OpenAsync();

        // v3 DDL: every column through incident_id, but WITHOUT remediation_action_json.
        await ExecAsync(connection, @"
CREATE TABLE analysis_findings (
    finding_id BIGINT PRIMARY KEY,
    analysis_time TIMESTAMP NOT NULL,
    server_id INTEGER NOT NULL,
    server_name VARCHAR NOT NULL,
    database_name VARCHAR,
    time_range_start TIMESTAMP,
    time_range_end TIMESTAMP,
    severity DOUBLE PRECISION NOT NULL,
    confidence DOUBLE PRECISION NOT NULL,
    category VARCHAR NOT NULL,
    story_path VARCHAR NOT NULL,
    story_path_hash VARCHAR NOT NULL,
    story_text VARCHAR NOT NULL,
    root_fact_key VARCHAR NOT NULL,
    root_fact_value DOUBLE PRECISION,
    leaf_fact_key VARCHAR,
    leaf_fact_value DOUBLE PRECISION,
    fact_count INTEGER NOT NULL,
    incident_id VARCHAR
)");
        await ExecAsync(connection, "CREATE TABLE analysis_schema_version (version INTEGER NOT NULL)");
        await ExecAsync(connection, "INSERT INTO analysis_schema_version (version) VALUES (3)");

        using var insert = connection.CreateCommand();
        insert.CommandText = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)";
        insert.Parameters.Add(new DuckDBParameter { Value = legacyFindingId });
        insert.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        insert.Parameters.Add(new DuckDBParameter { Value = TestDataSeeder.TestServerId });
        insert.Parameters.Add(new DuckDBParameter { Value = "SQL2022" });
        insert.Parameters.Add(new DuckDBParameter { Value = "StackOverflow" });
        insert.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddHours(-4) });
        insert.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        insert.Parameters.Add(new DuckDBParameter { Value = 1.0 });
        insert.Parameters.Add(new DuckDBParameter { Value = 0.9 });
        insert.Parameters.Add(new DuckDBParameter { Value = "waits" });
        insert.Parameters.Add(new DuckDBParameter { Value = "LEGACY" });
        insert.Parameters.Add(new DuckDBParameter { Value = "legacy_v3_hash" });
        insert.Parameters.Add(new DuckDBParameter { Value = "legacy story" });
        insert.Parameters.Add(new DuckDBParameter { Value = "LEGACY" });
        insert.Parameters.Add(new DuckDBParameter { Value = 1.0 });
        insert.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        insert.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        insert.Parameters.Add(new DuckDBParameter { Value = 1 });
        insert.Parameters.Add(new DuckDBParameter { Value = "legacy_incident" });
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task<bool> ColumnExistsAsync(DuckDbInitializer legacyDb, string table, string column)
    {
        using var connection = legacyDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText =
            "SELECT COUNT(*) FROM information_schema.columns WHERE table_name = $1 AND column_name = $2";
        cmd.Parameters.Add(new DuckDBParameter { Value = table });
        cmd.Parameters.Add(new DuckDBParameter { Value = column });
        return Convert.ToInt32(await cmd.ExecuteScalarAsync()) == 1;
    }

    private static async Task<int> ReadAnalysisSchemaVersionAsync(DuckDbInitializer legacyDb)
    {
        using var connection = legacyDb.CreateConnection();
        await connection.OpenAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT COALESCE(MAX(version), 0) FROM analysis_schema_version";
        return Convert.ToInt32(await cmd.ExecuteScalarAsync());
    }

    private static async Task ExecAsync(DuckDBConnection connection, string sql)
    {
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>Reads the stored server_id for a muted story hash (null when persisted as NULL).</summary>
    private async Task<int?> ReadMutedServerIdAsync(string storyPathHash)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT server_id FROM analysis_muted WHERE story_path_hash = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });

        var result = await cmd.ExecuteScalarAsync();
        return result is null or DBNull ? null : Convert.ToInt32(result);
    }

    /// <summary>Inserts a muted row with a caller-chosen literal server_id, bypassing the store's
    /// 0-&gt;NULL collapse so a pre-fix legacy row (server_id = 0) can be simulated.</summary>
    private async Task InsertLegacyMutedRowAsync(int serverId, string storyPathHash, string storyPath)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_muted (mute_id, server_id, story_path_hash, story_path, muted_date, reason)
VALUES ($1, $2, $3, $4, $5, $6)";
        cmd.Parameters.Add(new DuckDBParameter { Value = 987654321L });
        cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPathHash });
        cmd.Parameters.Add(new DuckDBParameter { Value = storyPath });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        cmd.Parameters.Add(new DuckDBParameter { Value = DBNull.Value });
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// #2448: a finding batch that faults partway through must persist NOTHING, not the rows that
    /// happened to land first.
    ///
    /// <para>The damage this prevents is invisible by construction, which is why it is worth a
    /// behavioural test against a real DuckDB rather than a source pin. Every row in a batch shares
    /// one <c>analysis_time</c> and <see cref="FindingStore.GetLatestFindingsAsync"/> reads the
    /// newest <c>analysis_time</c>, so two committed rows of an intended five do not read as a
    /// truncated set — they read as a complete analysis that found two problems. The server looks
    /// HEALTHIER for the store having failed, and nothing anywhere says otherwise.</para>
    ///
    /// <para>The fault is a duplicate <c>finding_id</c>, which is the reachable per-row fault on
    /// this table (<c>finding_id BIGINT PRIMARY KEY</c>) and not a contrivance: the ids come from
    /// <c>_nextId++</c> seeded off <c>DateTime.UtcNow.Ticks</c>, so two stores in one process can
    /// genuinely produce one — see #2455.</para>
    ///
    /// <para>Reverted, rows 1 and 2 commit before row 3 throws and this fails on
    /// <c>Assert.Single</c> with three rows present — the truncated set stated exactly.</para>
    /// </summary>
    [Fact]
    public async Task AFaultedBatch_PersistsNothing_RatherThanATruncatedSetThatReadsAsComplete()
    {
        const int serverId = -646464;
        const long earlierPassId = 5_646_464L;

        var store = new FindingStore(_duckDb);
        var context = new AnalysisContext
        {
            ServerId = serverId,
            ServerName = "partial-batch",
            TimeRangeStart = DateTime.UtcNow.AddHours(-4),
            TimeRangeEnd = DateTime.UtcNow
        };

        /* An earlier pass's row, and the id the doomed batch collides with. */
        await store.InsertFindingsAsync(
            [PartialBatchFinding(earlierPassId, serverId, "an earlier pass")], context);

        /* Row 3 of 5 collides, so rows 1-2 have already run by the time it fails. */
        var doomed = new List<AnalysisFinding>
        {
            PartialBatchFinding(earlierPassId + 1, serverId, "row 1"),
            PartialBatchFinding(earlierPassId + 2, serverId, "row 2"),
            PartialBatchFinding(earlierPassId, serverId, "row 3 - collides"),
            PartialBatchFinding(earlierPassId + 3, serverId, "row 4"),
            PartialBatchFinding(earlierPassId + 4, serverId, "row 5")
        };

        await Assert.ThrowsAsync<DuckDBException>(() => store.InsertFindingsAsync(doomed, context));

        /* The assertion #2448 exists for: not "fewer rows", NO rows from the doomed batch. Only the
           earlier pass survives, and it is still stamped with its own analysis_time — stale and
           saying so, rather than fresh and understating the server. */
        var persisted = await store.GetRecentFindingsAsync(serverId);
        Assert.Equal(earlierPassId, Assert.Single(persisted).FindingId);

        /* And the rollback is not the store simply refusing to write: the same five rows without
           the collision commit in full, through the same code path. */
        var clean = new List<AnalysisFinding>
        {
            PartialBatchFinding(earlierPassId + 10, serverId, "row 1"),
            PartialBatchFinding(earlierPassId + 11, serverId, "row 2"),
            PartialBatchFinding(earlierPassId + 12, serverId, "row 3"),
            PartialBatchFinding(earlierPassId + 13, serverId, "row 4"),
            PartialBatchFinding(earlierPassId + 14, serverId, "row 5")
        };

        await store.InsertFindingsAsync(clean, context);
        Assert.Equal(6, (await store.GetRecentFindingsAsync(serverId)).Count);
    }

    private static AnalysisFinding PartialBatchFinding(long findingId, int serverId, string storyText) =>
        new()
        {
            FindingId = findingId,
            AnalysisTime = DateTime.UtcNow,
            ServerId = serverId,
            ServerName = "partial-batch",
            Severity = 1.0,
            Confidence = 0.9,
            Category = "waits",
            StoryPath = "WRITELOG",
            StoryPathHash = "an1-partial-batch",
            StoryText = storyText,
            RootFactKey = "WRITELOG",
            FactCount = 1
        };

    /// #2455: two FindingStore instances must never issue the same id.
    ///
    /// <para>The filed defect was that <c>_nextId++</c> ran under a lock that admits concurrent
    /// holders, and it was real. The bigger half is that <c>Interlocked.Increment</c> on that field
    /// would not have fixed anything, because there is no shared field to make atomic: Lite builds
    /// TWO stores — <c>AnalysisService</c> and <c>RecommendationsTab</c> — each seeding its own
    /// counter from <c>DateTime.UtcNow.Ticks</c> at construction. Two built in the same timer tick
    /// start from the same value and then walk the same range independently.</para>
    ///
    /// <para><c>finding_id</c> and <c>mute_id</c> are both PRIMARY KEY in DuckDB, so a collision is a
    /// hard INSERT failure, and since #2448 made the batch atomic it costs the entire analysis rather
    /// than one row.</para>
    ///
    /// <para>The two stores are constructed on adjacent lines and each issues 1,000 ids, so the old
    /// per-instance seeds would have to differ by more than 1,000 ticks (100 microseconds) to avoid
    /// overlapping — orders of magnitude more clock than two adjacent constructor calls consume. On
    /// Windows, where this suite runs, the seeds are simply identical: the interrupt-timer granularity
    /// behind <c>DateTime.UtcNow</c> is ~15.6 ms, which is ~156,000 ticks.</para>
    /// </summary>
    [Fact]
    public async Task TwoFindingStoresNeverIssueTheSameId()
    {
        /* Adjacent on purpose: this is the case the old seeding could not survive. */
        var analysisPass = new FindingStore(_duckDb);
        var recommendationsTab = new FindingStore(_duckDb);

        var context = TestDataSeeder.CreateTestContext();
        var stories = ManyStories(1000);

        var fromPass = await analysisPass.FilterMutedFindingsAsync(stories, context);
        var fromTab = await recommendationsTab.FilterMutedFindingsAsync(stories, context);

        Assert.Equal(1000, fromPass.Count);
        Assert.Equal(1000, fromTab.Count);

        var ids = new HashSet<long>();
        foreach (var finding in fromPass)
            Assert.True(ids.Add(finding.FindingId), $"the analysis pass reissued {finding.FindingId}");
        foreach (var finding in fromTab)
            Assert.True(ids.Add(finding.FindingId), $"the second store reissued {finding.FindingId}");

        Assert.Equal(2000, ids.Count);
    }

    /// <summary>
    /// #2455: the read lock around the three WRITE paths is deliberate, and the reason has to stay
    /// written down.
    ///
    /// <para>An unexplained read-lock-to-write reads as a bug on every inspection, and the obvious
    /// "fix" — swapping in <c>AcquireWriteLock</c> — is the one change that would actually cost
    /// something: it serializes every finding batch against every UI read for the length of the batch,
    /// and #2443 had just made the read-lock WAIT abandonable precisely so the analysis pass could
    /// yield to a long archival rather than become the thing archival waits on.</para>
    ///
    /// <para>The lock coordinates everyone against MAINTENANCE (CHECKPOINT, archive DELETEs,
    /// compaction), which takes the exclusive write lock; a held read lock blocks
    /// <c>EnterWriteLock</c>, so holding one is how a write says "not while I am in flight". That is
    /// the only exclusion these paths need — concurrency between writers is DuckDB's own job. So this
    /// pins the choice AND its explanation together: changing the lock should be a decision someone
    /// makes against the stated reason, not a tidy-up.</para>
    /// </summary>
    [Fact]
    public void TheWritePathsTakeAReadLockOnPurpose_AndSayWhy()
    {
        var source = File.ReadAllText(FindingStoreSourcePath()).Replace("\r\n", "\n", StringComparison.Ordinal);

        /* The choice. The write lock is not banned outright — it is banned from these three without a
           reason, which is what a failing test forces someone to supply. */
        Assert.DoesNotContain("AcquireWriteLock", source, StringComparison.Ordinal);
        Assert.Equal(6, CountOf(source, "_duckDb.AcquireReadLock("));

        /* The explanation, at the class and at each write site — whichever one a reader lands on. */
        Assert.Contains("The read lock around the WRITES is deliberate (#2455)", source, StringComparison.Ordinal);
        Assert.Contains("A READ lock", Between(source, "public async Task<List<AnalysisFinding>> InsertFindingsAsync(", "public async Task<List<AnalysisFinding>> SaveFindingsAsync("), StringComparison.Ordinal);
        Assert.Contains("see the class note (#2455)", Between(source, "public async Task MuteStoryAsync(", "await cmd.ExecuteNonQueryAsync();"), StringComparison.Ordinal);
        Assert.Contains("see the class note (#2455)", Between(source, "public async Task CleanupOldFindingsAsync(", "await cmd.ExecuteNonQueryAsync();"), StringComparison.Ordinal);

        /* And the reason the shared lock is SUFFICIENT: no read-modify-write is left under it. */
        Assert.DoesNotContain("private long _nextId", source, StringComparison.Ordinal);
        Assert.Contains("FindingId = NextId(),", source, StringComparison.Ordinal);
        Assert.Contains("Value = NextId() }", source, StringComparison.Ordinal);
        Assert.Contains("CollectionIdGenerator.Next()", source, StringComparison.Ordinal);
    }

    private static string FindingStoreSourcePath()
    {
        var dir = new DirectoryInfo(AppContext.BaseDirectory);
        while (dir is not null && !File.Exists(Path.Combine(dir.FullName, "Lite", "Analysis", "FindingStore.cs")))
            dir = dir.Parent;

        Assert.NotNull(dir);
        return Path.Combine(dir!.FullName, "Lite", "Analysis", "FindingStore.cs");
    }

    private static string Between(string source, string start, string end)
    {
        var from = source.IndexOf(start, StringComparison.Ordinal);
        Assert.True(from >= 0, $"anchor not found: {start}");
        var to = source.IndexOf(end, from, StringComparison.Ordinal);
        Assert.True(to > from, $"anchor not found after {start}: {end}");
        return source[from..to];
    }

    private static int CountOf(string source, string needle)
    {
        var count = 0;
        var at = 0;
        while ((at = source.IndexOf(needle, at, StringComparison.Ordinal)) >= 0)
        {
            count++;
            at += needle.Length;
        }

        return count;
    }

    private static System.Collections.Generic.List<AnalysisStory> ManyStories(int count)
    {
        var stories = new System.Collections.Generic.List<AnalysisStory>(count);

        for (var i = 0; i < count; i++)
        {
            stories.Add(new AnalysisStory
            {
                RootFactKey = "WRITELOG",
                RootFactValue = i,
                Severity = 1.0,
                Confidence = 1.0,
                Category = "waits",
                StoryPath = $"WRITELOG_{i}",
                StoryPathHash = $"id-collision-{i}",
                StoryText = $"story {i}",
                FactCount = 1
            });
        }

        return stories;
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
