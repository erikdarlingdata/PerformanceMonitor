/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #2506: the analysis family's <c>as_of</c> anchor on Lite. #2495 anchored the read surface and left
/// these out because their window is built inside <see cref="AnalysisService"/> rather than in the tool,
/// and because <c>analyze_server</c> PERSISTS what it finds.
///
/// <para>Three claims, each of which failed before this change: a past window is reachable at all; the
/// anchor survives the trip from the tool into the engine (the seam #2495's review found eight broken
/// tools on, one level up); and an anchored analysis writes NOTHING, which is what makes anchoring the
/// persisting tool safe rather than merely possible.</para>
/// </summary>
public sealed class AnalysisAsOfAnchorTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string HistoricHash = "an2506-lite-historic";
    private const string RecentHash = "an2506-lite-recent";
    private const string PastWait = "AN2506_LITE_PAST_WAIT";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -8_260_600;
    private DuckDBConnection? _seedConn;

    public AnalysisAsOfAnchorTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AnalysisAsOfTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store — no DPAPI side effects. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        /* The id the TOOLS resolve to. Seeding under any other one would make every assertion below
           pass or fail for a reason that has nothing to do with the anchor. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The findings read windows on ANALYSIS TIME. A pass that ran 30 hours ago is outside every default
    /// window (the tool's default is 24) and inside an anchored one — and, in the other direction, the
    /// run from an hour ago is inside the default and outside the anchored one. Both directions are
    /// asserted because only the second can fail if the upper bound is missing, which is exactly the
    /// half-open-window mistake this parameter makes so easy.
    /// </summary>
    [Fact]
    public async Task TheFindingsRead_SeesAPastRun_AndTheDefaultAnchorDoesNot()
    {
        var historicRun = DateTime.UtcNow.AddHours(-30);
        await PlantFindingAsync(historicRun, HistoricHash);
        await PlantFindingAsync(DateTime.UtcNow.AddHours(-1), RecentHash);

        var service = CreateTestService();

        var defaultWindow = await McpAnalysisTools.GetAnalysisFindings(service, _serverManager);
        Assert.Contains(RecentHash, defaultWindow, StringComparison.Ordinal);
        Assert.DoesNotContain(HistoricHash, defaultWindow, StringComparison.Ordinal);

        var anchored = await McpAnalysisTools.GetAnalysisFindings(
            service, _serverManager, null, 4, false, historicRun.AddMinutes(30).ToString("o"));
        Assert.Contains(HistoricHash, anchored, StringComparison.Ordinal);
        Assert.DoesNotContain(RecentHash, anchored, StringComparison.Ordinal);
        Assert.Equal(1, JsonDocument.Parse(anchored).RootElement.GetProperty("finding_count").GetInt32());
    }

    /// <summary>
    /// The UNANCHORED findings read keeps its half-open window, and that is a decision rather than an
    /// omission: bounding it at "now" broke a live test on this change's first attempt, and the
    /// mechanism behind that is real. <c>analysis_time</c> is stamped by the WRITER and filtered by the
    /// READER, so a default read bounded at the reader's clock drops a run written a moment earlier the
    /// day those two clocks stop being the same one — a findings read that "sometimes misses the
    /// analysis that just finished", with nothing in it to point at a clock. A row stamped ahead of now
    /// is the cheap, deterministic stand-in for that skew.
    /// </summary>
    [Fact]
    public async Task TheUnanchoredFindingsRead_StillHasNoUpperBound_AndTheAnchoredOneDoes()
    {
        await PlantFindingAsync(DateTime.UtcNow.AddMinutes(30), RecentHash);
        await PlantFindingAsync(DateTime.UtcNow.AddHours(-30), HistoricHash);

        var service = CreateTestService();

        Assert.Contains(
            RecentHash,
            await McpAnalysisTools.GetAnalysisFindings(service, _serverManager),
            StringComparison.Ordinal);

        /* Present exactly where it is needed and absent exactly where it would do harm. */
        Assert.DoesNotContain(
            RecentHash,
            await McpAnalysisTools.GetAnalysisFindings(
                service, _serverManager, null, 4, false, DateTime.UtcNow.AddHours(-29.5).ToString("o")),
            StringComparison.Ordinal);
    }

    /// <summary>
    /// The tool-to-engine seam. <c>get_analysis_facts</c> hands the anchor to
    /// <see cref="AnalysisService.CollectAndScoreFactsAsync"/>, which is where the window is actually
    /// built — a tool that resolved the anchor and then called the engine without it would validate,
    /// refuse bad input correctly, and score the present.
    /// </summary>
    [Fact]
    public async Task TheFactsRead_ScoresAPastWindow_AndTheDefaultAnchorFindsNothingThere()
    {
        /* 30 hours ago, i.e. outside every default window on this surface (the widest default is 24). */
        var incident = DateTime.UtcNow.AddHours(-30);
        await PlantWaitAsync(incident, 900_000L);
        await PlantWaitAsync(incident.AddMinutes(10), 800_000L);

        var service = CreateTestService();

        var anchored = await McpAnalysisTools.GetAnalysisFacts(
            service, _serverManager, null, 4, null, 0, incident.AddMinutes(30).ToString("o"));
        Assert.Contains(PastWait, anchored, StringComparison.Ordinal);

        var defaultWindow = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager);
        Assert.Equal("unavailable", StatusOf(defaultWindow));
    }

    /// <summary>
    /// The persistence decision, A/B'd on the SAME window so nothing but the anchor can explain the
    /// difference. An anchored pass returns its findings in full and writes none of them; the identical
    /// window unanchored returns the same findings and writes them.
    ///
    /// <para>Run against the engine rather than the tool because the rule lives in the engine on purpose:
    /// there is no legitimate caller for "anchored AND persist", so there is no way to ask for it, and a
    /// future caller that never read the argument cannot get it wrong.</para>
    /// </summary>
    [Fact]
    public async Task AnAnchoredPass_ReturnsItsFindings_AndWritesNoneOfThem()
    {
        using var seeder = new TestDataSeeder(_duckDb);
        await seeder.SeedMemoryStarvedServerAsync();

        var service = CreateTestService();

        var anchored = TestDataSeeder.CreateTestContext();
        anchored.AsOfUtc = anchored.TimeRangeEnd;

        var exploratory = await service.AnalyzeAsync(anchored);
        Assert.NotEmpty(exploratory);
        Assert.Empty(await service.GetRecentFindingsAsync(TestDataSeeder.TestServerId));

        /* The control, and the reason the assertion above is not vacuous: the same window with no anchor
           produces findings AND rows. Without this, "nothing was written" would also be the reading of a
           fixture that had nothing to write. */
        var persisting = await service.AnalyzeAsync(TestDataSeeder.CreateTestContext());
        Assert.NotEmpty(persisting);
        Assert.NotEmpty(await service.GetRecentFindingsAsync(TestDataSeeder.TestServerId));
    }

    /// <summary>
    /// And the tool says so. A payload that quietly omitted the difference would leave an agent telling
    /// someone to "check the persisted findings" for a run that never wrote any.
    /// </summary>
    [Fact]
    public async Task TheAnalyzeTool_DisclosesThatAnAnchoredRunWasNotPersisted()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), 900_000L);
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-29), 800_000L);

        var service = CreateTestService();

        var anchored = await McpAnalysisTools.AnalyzeServer(
            service, _serverManager, null, 4, DateTime.UtcNow.AddHours(-29).ToString("o"));
        var (anchoredPersisted, anchoredNote) = PersistenceOf(anchored);
        Assert.False(anchoredPersisted);
        Assert.Contains("NOT written to the store", anchoredNote!, StringComparison.Ordinal);

        Assert.Equal(0, await CountFindingsAsync());

        /* Unanchored, the same tool reports the ordinary answer and says nothing extra. */
        var (defaultPersisted, defaultNote) = PersistenceOf(await McpAnalysisTools.AnalyzeServer(service, _serverManager));
        Assert.True(defaultPersisted);
        Assert.Null(defaultNote);
    }

    /// <summary>
    /// A bad anchor is REFUSED on the persisting tool too. Falling back to "now" here would not merely
    /// answer a different question — it would run a real, persisting analysis the caller did not ask for.
    /// </summary>
    [Fact]
    public async Task TheAnalyzeTool_RefusesAnUnusableAnchor_RatherThanAnalysingThePresent()
    {
        var service = CreateTestService();

        Assert.StartsWith(
            "Invalid as_of",
            await McpAnalysisTools.AnalyzeServer(service, _serverManager, null, 4, "last tuesday"),
            StringComparison.Ordinal);

        Assert.Contains(
            "future",
            await McpAnalysisTools.AnalyzeServer(service, _serverManager, null, 4, DateTime.UtcNow.AddDays(1).ToString("o")),
            StringComparison.Ordinal);

        Assert.Equal(0, await CountFindingsAsync());
    }

    /// <summary>
    /// <c>compare_analysis</c> moves BOTH windows, because <c>baseline_hours_back</c> has always been
    /// measured from the comparison window's end. Moving only the comparison end would silently change
    /// what the two windows are relative to each other.
    /// </summary>
    [Fact]
    public async Task CompareAnalysis_HangsBothWindowsOffTheAnchor()
    {
        var service = CreateTestService();
        var anchor = DateTime.UtcNow.AddHours(-30);
        await PlantWaitAsync(anchor.AddMinutes(-30), 900_000L);
        await PlantWaitAsync(anchor.AddHours(-28), 500_000L);

        var compared = await McpAnalysisTools.CompareAnalysis(
            service, _serverManager, null, 4, 28, anchor.ToString("o"));

        Assert.Contains(anchor.ToString("o"), compared, StringComparison.Ordinal);
        Assert.Contains(anchor.AddHours(-28).ToString("o"), compared, StringComparison.Ordinal);

        /* The control: unanchored, both windows hang off now and neither instant appears. */
        var comparedNow = await McpAnalysisTools.CompareAnalysis(service, _serverManager, null, 4, 28);
        Assert.DoesNotContain(anchor.ToString("o"), comparedNow, StringComparison.Ordinal);
    }

    // ── helpers ──

    private AnalysisService CreateTestService() => new(_duckDb) { MinimumDataHours = 0 };

    private static string StatusOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.GetProperty("status").GetString()!;
    }

    /// <summary>
    /// Reads <c>persisted</c> / <c>persistence_note</c> from either shape <c>analyze_server</c> can
    /// return. A data-bearing result carries them at the root; a miss carries them under <c>hints</c>,
    /// beside the <c>analysis_time</c> that already lived there — so the test asserts the CLAIM rather
    /// than a path, and does not quietly start passing if the run happens to find nothing.
    /// </summary>
    private static (bool Persisted, string? Note) PersistenceOf(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        var carrier = root.TryGetProperty("persisted", out _) ? root : root.GetProperty("hints");

        var note = carrier.GetProperty("persistence_note");
        return (carrier.GetProperty("persisted").GetBoolean(),
                note.ValueKind == JsonValueKind.Null ? null : note.GetString());
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    private async Task<int> CountFindingsAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = "SELECT COUNT(*) FROM analysis_findings WHERE server_id = $1";
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        return Convert.ToInt32(await cmd.ExecuteScalarAsync() ?? 0);
    }

    private async Task PlantWaitAsync(DateTime at, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestServer', $4, 5000, $5, 0, 5000, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(PastWait);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// One finding row as a scheduled pass would have left it. Planted rather than produced, because
    /// producing it would stamp <c>analysis_time</c> with NOW — the very thing an anchored run refuses.
    /// </summary>
    private async Task PlantFindingAsync(DateTime analysisTime, string storyPathHash)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO analysis_findings
    (finding_id, analysis_time, server_id, server_name, database_name,
     time_range_start, time_range_end, severity, confidence, category,
     story_path, story_path_hash, story_text,
     root_fact_key, root_fact_value, leaf_fact_key, leaf_fact_value, fact_count, incident_id,
     remediation_action_json, drill_down_json)
VALUES ($1, $2, $3, 'TestServer', NULL, $4, $5, 0.9, 0.8, 'waits',
        'AN2506_LITE', $6, 'planted for the #2506 anchored findings read',
        'AN2506_LITE', 1, NULL, NULL, 1, NULL, NULL, NULL)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(analysisTime);
        P(_serverId);
        P(analysisTime.AddHours(-4));
        P(analysisTime);
        P(storyPathHash);
        await cmd.ExecuteNonQueryAsync();
    }
}
