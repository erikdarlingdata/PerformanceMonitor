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
/// #3538 A2, through the real tools: a window the collector only partly observed says so in every
/// analysis payload, a window it never observed is "unavailable" even when point-in-time facts exist,
/// and a fully collected window says nothing extra. The collector-level arithmetic (fractions per
/// observed time, the COLLECTION_GAP fact's numbers) is pinned in <c>FactCollectorTests</c>; this class
/// pins what a CALLER sees, because the defect was a caller reading confident numbers about a window
/// three quarters of which nobody had measured.
///
/// <para>Rows are planted relative to the clock the tools use (a 4h window ending now), as a realistic
/// series: a baseline reading at the window start and one collection every 15 minutes carrying the
/// delta since the previous one, for as many minutes of the window as the scenario's collector was
/// up. <see cref="WindowCoverage"/> explains why the first reading has to exist.</para>
/// </summary>
public sealed class AnalysisCoverageTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string HeavyWait = "CV3538_HEAVY_WAIT";
    private const string Wait = "CXPACKET";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -3_538_000;
    private DuckDBConnection? _seedConn;

    public AnalysisCoverageTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AnalysisCoverageTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

        /* Windows auth so AddServer never touches the credential store — no DPAPI side effects. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);

        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /* ── analyze_server ── */

    /// <summary>
    /// The failure scenario from the review, end to end: a CXPACKET storm at a true 25% of observed
    /// time with the collector down for three of the four hours. Divided by the nominal window that
    /// is 6% — under the parallelism bar — and the pass used to be a near-clean bill with no caveat.
    /// Now the fraction is the true one, the finding fires, and the payload says how much of the
    /// window it speaks for.
    /// </summary>
    [Fact]
    public async Task AnalyzeServer_PartialWindow_FindsTheStorm_AndCarriesTheCoverageCaveat()
    {
        /* 900,000 ms of CXPACKET over the ONE observed hour: 0.25 of observed time, 0.0625 of nominal. */
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 60);

        var service = CreateService();
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal("findings", root.GetProperty("status").GetString());

        var caveat = root.GetProperty("caveat").GetString();
        Assert.NotNull(caveat);
        Assert.Contains("PARTIAL COVERAGE", caveat, StringComparison.Ordinal);
        Assert.Contains("25%", caveat, StringComparison.Ordinal);
        Assert.Contains("get_collection_health", caveat, StringComparison.Ordinal);

        var coverage = root.GetProperty("coverage");
        Assert.True(coverage.GetProperty("partial").GetBoolean());
        Assert.False(coverage.GetProperty("unobserved").GetBoolean());
        Assert.InRange(coverage.GetProperty("observed_fraction").GetDouble(), 0.24, 0.26);
        Assert.InRange(coverage.GetProperty("observed_hours").GetDouble(), 0.98, 1.0);
        Assert.InRange(coverage.GetProperty("largest_gap_hours").GetDouble(), 2.99, 3.02);

        /* The storm is a finding at its TRUE fraction — the number the nominal window would have read
           as 0.0625 and left under every bar. */
        var cx = FindRootFact(root, "CXPACKET");
        Assert.InRange(cx.GetProperty("value").GetDouble(), 0.24, 0.26);

        Assert.NotNull(service.LastWindowCoverage);
        Assert.True(service.LastWindowCoverage!.IsPartial);
    }

    /// <summary>
    /// The partial all-clear is scoped to the time that was seen. Same <c>empty</c> status, because facts
    /// were scored and nothing fired, but the prose no longer claims the whole window.
    /// </summary>
    [Fact]
    public async Task AnalyzeServer_PartialWindowWithNoFindings_IsAScopedAllClear_NotAFullOne()
    {
        await PlantSeriesAsync(Wait, totalDeltaMs: 4_000, coveredMinutes: 60);

        var service = CreateService();
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());

        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("PARTIAL reading, not a full all-clear", message, StringComparison.Ordinal);
        Assert.Contains("25%", message, StringComparison.Ordinal);
        Assert.DoesNotContain("All metrics are within normal ranges", message, StringComparison.Ordinal);

        Assert.True(root.GetProperty("hints").GetProperty("coverage").GetProperty("partial").GetBoolean());
        Assert.Null(service.WindowEmptyMessage);
    }

    /// <summary>
    /// Full coverage is the ordinary case and says nothing extra: no caveat, the classic all-clear, and
    /// a coverage block that reads 100% so a caller can branch on it uniformly.
    /// </summary>
    [Fact]
    public async Task AnalyzeServer_FullWindow_HasNoCaveat_AndReportsFullCoverage()
    {
        await PlantSeriesAsync(Wait, totalDeltaMs: 4_000, coveredMinutes: 240);

        var service = CreateService();
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal("empty", root.GetProperty("status").GetString());
        Assert.Contains("All metrics are within normal ranges", result, StringComparison.Ordinal);
        Assert.DoesNotContain("PARTIAL", result, StringComparison.Ordinal);

        var coverage = root.GetProperty("hints").GetProperty("coverage");
        Assert.False(coverage.GetProperty("partial").GetBoolean());
        Assert.InRange(coverage.GetProperty("observed_fraction").GetDouble(), 0.99, 1.0);
    }

    /// <summary>
    /// Zero coverage composes with the wave-1 (#3524) envelope even when facts exist. The point-in-time
    /// configuration facts read from the latest row regardless of window, so a dead collector with a
    /// config table used to produce a real pass — config-only findings or an all-clear — that read as
    /// "analyzed this window". It is the same unavailable answer, and it says which kind of nothing.
    /// </summary>
    [Fact]
    public async Task AnalyzeServer_UnobservedWindowWithPointInTimeFacts_IsUnavailable()
    {
        /* One reading with no predecessor: the delta is unknowable, so is the time it stands for. */
        await PlantWaitAsync(DateTime.UtcNow.AddMinutes(-30), HeavyWait, 9_000_000L);
        await PlantServerConfigAsync();

        var service = CreateService();
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("NOT an all-clear", result, StringComparison.Ordinal);
        Assert.Contains("point-in-time", result, StringComparison.Ordinal);
        Assert.Contains("get_collection_health", result, StringComparison.Ordinal);
        Assert.DoesNotContain("within normal ranges", result, StringComparison.Ordinal);

        Assert.NotNull(service.WindowEmptyMessage);
        Assert.Contains("observed none of the analysis window", service.WindowEmptyMessage, StringComparison.Ordinal);
        Assert.NotNull(service.LastWindowCoverage);
        Assert.False(service.LastWindowCoverage!.IsObserved);
    }

    /* ── get_analysis_facts ── */

    [Fact]
    public async Task GetAnalysisFacts_PartialWindow_ShowsTheGapFact_AndTheCaveat()
    {
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 60);

        var result = await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Contains("PARTIAL COVERAGE", root.GetProperty("caveat").GetString()!, StringComparison.Ordinal);
        Assert.True(root.GetProperty("coverage").GetProperty("partial").GetBoolean());

        JsonElement? gap = null, cx = null;
        foreach (var fact in root.GetProperty("facts").EnumerateArray())
        {
            var key = fact.GetProperty("key").GetString();
            if (key == WindowCoverage.FactKey) gap = fact;
            if (key == "CXPACKET") cx = fact;
        }

        Assert.NotNull(gap);
        Assert.Equal(WindowCoverage.FactSource, gap!.Value.GetProperty("source").GetString());
        Assert.InRange(gap.Value.GetProperty("value").GetDouble(), 0.24, 0.26);
        Assert.Equal(0, gap.Value.GetProperty("severity").GetDouble());

        Assert.NotNull(cx);
        Assert.InRange(cx!.Value.GetProperty("value").GetDouble(), 0.24, 0.26);
        Assert.InRange(cx.Value.GetProperty("metadata").GetProperty("coverage_fraction").GetDouble(), 0.24, 0.26);
    }

    [Fact]
    public async Task GetAnalysisFacts_UnobservedWindowWithPointInTimeFacts_IsUnavailable()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddMinutes(-30), HeavyWait, 9_000_000L);
        await PlantServerConfigAsync();

        var result = await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var message = root.GetProperty("message").GetString()!;
        Assert.Contains("observed none of the requested window", message, StringComparison.Ordinal);
        Assert.Contains("audit_config", message, StringComparison.Ordinal);
        Assert.True(root.GetProperty("hints").GetProperty("coverage").GetProperty("unobserved").GetBoolean());
    }

    [Fact]
    public async Task GetAnalysisFacts_FullWindow_HasNoCaveat_AndNoGapFact()
    {
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 240);

        var result = await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal(JsonValueKind.Null, root.GetProperty("caveat").ValueKind);
        Assert.False(root.GetProperty("coverage").GetProperty("partial").GetBoolean());
        Assert.DoesNotContain(WindowCoverage.FactKey, result, StringComparison.Ordinal);
    }

    /* ── compare_analysis ── */

    /// <summary>
    /// The case the empty-window caveats never reached: BOTH windows have facts, one of them from a
    /// collector that was up for a quarter of its window. The rates on that side are per observed
    /// time, so the deltas are honest — and the caveat says which side speaks for an hour.
    /// </summary>
    [Fact]
    public async Task CompareAnalysis_PartialComparisonWindow_CarriesTheCoverageCaveat_OnThatSide()
    {
        var now = DateTime.UtcNow;

        /* Baseline window (28h..24h back): fully collected. Comparison window (4h..now): one hour. */
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 240, windowEnd: now.AddHours(-24));
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 60, windowEnd: now);

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;

        var caveat = root.GetProperty("caveat").GetString();
        Assert.NotNull(caveat);
        Assert.Contains("The COMPARISON window was only partly collected", caveat, StringComparison.Ordinal);
        Assert.DoesNotContain("The BASELINE window was only partly collected", caveat, StringComparison.Ordinal);
        Assert.DoesNotContain("produced no facts at all", caveat, StringComparison.Ordinal);
        Assert.Contains("is not a wait that resolved", caveat, StringComparison.Ordinal);

        Assert.False(root.GetProperty("baseline").GetProperty("coverage").GetProperty("partial").GetBoolean());
        Assert.True(root.GetProperty("comparison").GetProperty("coverage").GetProperty("partial").GetBoolean());

        /* The same 900,000 ms on each side: 0.0625 of a fully observed window, 0.25 of a quarter-observed
           one. Same storm intensity per observed hour reads as four times worse in the comparison
           window because the collector saw a quarter as much time — and THAT is what the caveat is for. */
        JsonElement? cx = null;
        foreach (var fact in root.GetProperty("facts").EnumerateArray())
            if (fact.GetProperty("key").GetString() == "CXPACKET") cx = fact;
        Assert.NotNull(cx);
        Assert.InRange(cx!.Value.GetProperty("baseline_value").GetDouble(), 0.06, 0.065);
        Assert.InRange(cx.Value.GetProperty("comparison_value").GetDouble(), 0.24, 0.26);

        /* #3538 A3 composes with this: the fourfold value move IS banded worse (75% of the larger side, and
           the key saturates its ladder), and the row carries coverage_caveat: true so that verdict cannot
           be read without the sentence above — it is the collector's quarter, not the server's storm. */
        Assert.Equal("worse", cx.Value.GetProperty("status").GetString());
        Assert.True(cx.Value.GetProperty("coverage_caveat").GetBoolean());
        Assert.True(root.GetProperty("summary").GetProperty("coverage_caveat").GetBoolean());
        Assert.All(root.GetProperty("families").EnumerateArray(), f => Assert.True(f.GetProperty("coverage_caveat").GetBoolean()));

        /* The COLLECTION_GAP fact is reported through the coverage blocks, not as a compared key. */
        Assert.DoesNotContain(WindowCoverage.FactKey, result, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CompareAnalysis_BothWindowsFullyCollected_HasNoCaveat()
    {
        var now = DateTime.UtcNow;
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 240, windowEnd: now.AddHours(-24));
        await PlantSeriesAsync(Wait, totalDeltaMs: 900_000, coveredMinutes: 240, windowEnd: now);

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.Equal(JsonValueKind.Null, root.GetProperty("caveat").ValueKind);
        Assert.InRange(root.GetProperty("baseline").GetProperty("coverage").GetProperty("observed_fraction").GetDouble(), 0.99, 1.0);
        Assert.InRange(root.GetProperty("comparison").GetProperty("coverage").GetProperty("observed_fraction").GetDouble(), 0.99, 1.0);
        Assert.False(root.GetProperty("summary").GetProperty("coverage_caveat").GetBoolean());
        Assert.All(root.GetProperty("facts").EnumerateArray(), f => Assert.False(f.GetProperty("coverage_caveat").GetBoolean()));
    }

    /* ── WindowCoverage arithmetic, no store ── */

    [Fact]
    public void WindowCoverage_Fraction_IsBounded_AndPartialBelowTheBar()
    {
        var quarter = new WindowCoverage { NominalMs = 14_400_000, ObservedMs = 3_600_000, SampleCount = 5, LargestGapMs = 10_800_000 };
        Assert.Equal(0.25, quarter.Fraction, precision: 10);
        Assert.True(quarter.IsObserved);
        Assert.True(quarter.IsPartial);

        var atTheBar = new WindowCoverage { NominalMs = 100, ObservedMs = 70 };
        Assert.False(atTheBar.IsPartial);
        var justUnder = new WindowCoverage { NominalMs = 100, ObservedMs = 69.9 };
        Assert.True(justUnder.IsPartial);

        /* Clipping keeps observed <= nominal by construction; the guard is only against rounding. */
        var overByRounding = new WindowCoverage { NominalMs = 100, ObservedMs = 100.0001 };
        Assert.Equal(1.0, overByRounding.Fraction);

        var unobserved = WindowCoverage.Unobserved(14_400_000);
        Assert.False(unobserved.IsObserved);
        Assert.False(unobserved.IsPartial);
        Assert.Equal(0, unobserved.Fraction);
        Assert.Equal(14_400_000, unobserved.LargestGapMs);

        var reversed = WindowCoverage.Unobserved(-5);
        Assert.Equal(0, reversed.NominalMs);
        Assert.Equal(0, reversed.Fraction);
    }

    [Fact]
    public void WindowCoverage_GapFact_CarriesTheNumbers_AndDescribeSaysThem()
    {
        var quarter = new WindowCoverage { NominalMs = 14_400_000, ObservedMs = 3_600_000, SampleCount = 5, LargestGapMs = 10_800_000 };

        var fact = quarter.ToGapFact(serverId: 7);
        Assert.Equal(WindowCoverage.FactSource, fact.Source);
        Assert.Equal(WindowCoverage.FactKey, fact.Key);
        Assert.Equal(7, fact.ServerId);
        Assert.Equal(0.25, fact.Value, precision: 10);
        Assert.Equal(3_600_000, fact.Metadata["observed_ms"]);
        Assert.Equal(14_400_000, fact.Metadata["nominal_ms"]);
        Assert.Equal(10_800_000, fact.Metadata["unobserved_ms"]);
        Assert.Equal(10_800_000, fact.Metadata["largest_gap_ms"]);
        Assert.Equal(5, fact.Metadata["sample_count"]);

        var prose = quarter.Describe();
        Assert.Contains("25%", prose, StringComparison.Ordinal);
        Assert.Contains("4h window", prose, StringComparison.Ordinal);
        Assert.Contains("1h of collected time", prose, StringComparison.Ordinal);
        Assert.Contains("largest unobserved stretch 3h", prose, StringComparison.Ordinal);

        Assert.Contains("observed NONE", WindowCoverage.Unobserved(14_400_000).Describe(), StringComparison.Ordinal);
    }

    /* ── helpers ── */

    private AnalysisService CreateService() => new(_duckDb) { MinimumDataHours = 0 };

    private static JsonElement FindRootFact(JsonElement root, string key)
    {
        foreach (var finding in root.GetProperty("findings").EnumerateArray())
        {
            var rootFact = finding.GetProperty("root_fact");
            if (rootFact.GetProperty("key").GetString() == key)
                return rootFact;
        }

        throw new Xunit.Sdk.XunitException($"no finding rooted on {key} in: {root}");
    }

    /// <summary>
    /// A realistic delta series for a 4h window ending at <paramref name="windowEnd"/> (default now):
    /// a baseline reading at the window start with no knowable delta, then a collection every 15
    /// minutes for the first <paramref name="coveredMinutes"/>, the deltas summing to
    /// <paramref name="totalDeltaMs"/>. Nothing after that — the collector was down.
    /// </summary>
    private async Task PlantSeriesAsync(string waitType, long totalDeltaMs, int coveredMinutes, DateTime? windowEnd = null)
    {
        var end = windowEnd ?? DateTime.UtcNow;
        var start = end.AddHours(-4);
        var points = coveredMinutes / 15;
        var perPoint = totalDeltaMs / points;

        await PlantWaitAsync(start, waitType, 0L);
        for (var i = 1; i <= points; i++)
            await PlantWaitAsync(start.AddMinutes(15 * i), waitType, perPoint);
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

    private async Task PlantWaitAsync(DateTime at, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, 'TestServer', $4, 50, 1000000, 0, 4, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>A point-in-time fact source: the latest server_config row, read regardless of window.</summary>
    private async Task PlantServerConfigAsync()
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        foreach (var (name, value) in new[] { ("cost threshold for parallelism", 50), ("max degree of parallelism", 8) })
        {
            using var cmd = conn.CreateCommand();
            cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, 'TestServer', $4, $5, $5, true, true)";
            void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
            P(_nextId--);
            P(DateTime.UtcNow.AddMinutes(-30));
            P(_serverId);
            P(name);
            P(value);
            await cmd.ExecuteNonQueryAsync();
        }
    }
}
