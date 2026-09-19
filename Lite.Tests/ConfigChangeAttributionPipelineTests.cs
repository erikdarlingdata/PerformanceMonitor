/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A10 (Q2): the <c>CONFIG_CHANGED</c> finding through the REAL Lite pipeline against a DuckDB store —
/// the step-2.5 wiring in <see cref="AnalysisService"/> that the pure pins in
/// <see cref="ConfigChangeAttributionTests"/> cannot see. Three arms, one store shape each:
/// a <c>server_config</c> snapshot pair whose newer capture sits INSIDE the 4 h window yields exactly one
/// finding rooted at the key, at the Information severity, with the setting and its values frozen into
/// its StoryText; the same pair with the newer capture OUTSIDE the window yields none (the window rule is
/// the diff's, not a "latest snapshot" read); and a change one hour old discloses that its after half is
/// a partial. Every arm plants a benign wait series through the window so the pass's coverage witness
/// finds the window observed — otherwise the dead-collector envelope returns before step 2.5 runs.
///
/// <para>The Darling twin's wiring is byte-for-byte the same statement over <c>server_config</c> instead of
/// <c>v_server_config</c> and the same call sequence; it has no in-process store to run against here, and
/// its end-to-end test runs against the dev PostgreSQL in CI's Darling PostgreSQL job.</para>
/// </summary>
public sealed class ConfigChangeAttributionPipelineTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string StaleWait = "CC3653_STALE_WAIT";
    private const string BenignWait = "CC3653_BENIGN_WAIT";
    private const string Maxdop = "max degree of parallelism";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly int _serverId;
    private long _nextId = -3_653_000;
    private DuckDBConnection? _seedConn;

    public ConfigChangeAttributionPipelineTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "ConfigChangeAttributionPipelineTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);

        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
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
    /// MAXDOP 0 captured 30 h ago, MAXDOP 8 captured 3 h ago: the diff's change time is inside the 4 h
    /// window, so the pass emits the finding — one, rooted at the key, at 0.25, category <c>config</c>,
    /// with the frozen prose naming the setting and both values. The compare ran (both halves are inside
    /// the planted series) and its verdict counts are on the finding's metadata.
    /// </summary>
    [Fact]
    public async Task AChangeObservedInsideTheWindow_YieldsOneInformationFinding_NamingTheSetting()
    {
        var now = DateTime.UtcNow;
        await PlantWaitSeriesAsync(now);
        await PlantServerConfigAsync(now.AddHours(-30), Maxdop, 0);
        await PlantServerConfigAsync(now.AddHours(-3), Maxdop, 8);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        Assert.Null(service.WindowEmptyMessage);
        Assert.Null(service.InsufficientDataMessage);

        var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
        Assert.Equal(ConfigChangeAttribution.InformationSeverity, finding.Severity, precision: 9);
        Assert.Equal("config", finding.Category);
        Assert.Equal(ConfigChangeAttribution.FactKey, finding.StoryPath);

        var advice = FactAdvice.TryReadStoryText(finding.StoryText);
        Assert.NotNull(advice);
        Assert.Contains($"`{Maxdop}` 0 → 8", advice!.Headline, StringComparison.Ordinal);
        Assert.Contains("first observed by the configuration snapshot at", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("27 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("not an accusation", advice.Remediation, StringComparison.Ordinal);

        /* The compare ran over observed halves: the verdict counts are on the row, and it is not "unavailable". */
        Assert.NotNull(finding.RootFactMetadata);
        Assert.Equal(0, finding.RootFactMetadata![ConfigChangeAttribution.MetaCompareUnavailable]);
        Assert.True(finding.RootFactMetadata.ContainsKey(ConfigChangeAttribution.MetaComparedKeys));
        Assert.Equal(1, finding.RootFactMetadata[ConfigChangeAttribution.MetaChangedSettings]);
        Assert.Equal(8, finding.RootFactMetadata[ConfigChangeAttribution.NewInUseKey(Maxdop)]);
        Assert.Equal(3.0, finding.RootFactMetadata[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
        Assert.Equal(1, finding.RootFactMetadata[ConfigChangeAttribution.MetaAfterWindowClamped]);
    }

    /// <summary>
    /// The control: the SAME two snapshots with the newer one 10 h old. The latest snapshot still says
    /// MAXDOP 8 (the CONFIG_MAXDOP fact reads it), but the change was first observed before the window
    /// started, so there is nothing to attribute this pass. No CONFIG_CHANGED finding; the pass otherwise
    /// runs (no envelope message).
    /// </summary>
    [Fact]
    public async Task AChangeObservedBeforeTheWindow_YieldsNoFinding()
    {
        var now = DateTime.UtcNow;
        await PlantWaitSeriesAsync(now);
        await PlantServerConfigAsync(now.AddHours(-30), Maxdop, 0);
        await PlantServerConfigAsync(now.AddHours(-10), Maxdop, 8);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        Assert.Null(service.WindowEmptyMessage);
        Assert.DoesNotContain(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
    }

    /// <summary>
    /// A single snapshot is not a change (the diff needs two), so a fresh server's first capture inside the
    /// window emits nothing — the case the COALESCE bound in the snapshot read exists for.
    /// </summary>
    [Fact]
    public async Task ASingleSnapshotInsideTheWindow_IsNotAChange()
    {
        var now = DateTime.UtcNow;
        await PlantWaitSeriesAsync(now);
        await PlantServerConfigAsync(now.AddHours(-1), Maxdop, 8);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        Assert.DoesNotContain(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
    }

    /// <summary>
    /// The after-window disclosure: a change first observed one hour ago has one hour of "after", and the
    /// frozen prose says the after half is still filling in rather than reporting a four-hour compare it
    /// did not have. The metadata carries the clamp and the observed hours.
    /// </summary>
    [Fact]
    public async Task AChangeOneHourOld_DisclosesThePartialAfterHalf()
    {
        var now = DateTime.UtcNow;
        await PlantWaitSeriesAsync(now);
        await PlantServerConfigAsync(now.AddHours(-30), Maxdop, 0);
        await PlantServerConfigAsync(now.AddHours(-1), Maxdop, 8);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
        var advice = FactAdvice.TryReadStoryText(finding.StoryText)!;
        Assert.Contains("the 1 h after it that exist so far", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("still filling in, and later passes complete it", advice.Investigation, StringComparison.Ordinal);
        Assert.Equal(1, finding.RootFactMetadata![ConfigChangeAttribution.MetaAfterWindowClamped]);
        Assert.Equal(1.0, finding.RootFactMetadata[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
    }

    /* ── plants ── */

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }
        return _seedConn;
    }

    /// <summary>
    /// 30 h of history so the real 24 h data-span gate passes, then a benign reading every fifteen minutes
    /// from 8 h ago to now, so the pass window AND both halves of a compare around a change up to 4 h old
    /// are observed by the coverage witness.
    /// </summary>
    private async Task PlantWaitSeriesAsync(DateTime now)
    {
        await PlantWaitAsync(now.AddHours(-30), StaleWait, 60_000L);
        for (var minutesAgo = 480; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 100L);
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
VALUES ($1, $2, $3, 'TestServer', $4, 50, $5, 0, 50, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>One sys.configurations capture row: the collector's shape, value_configured == value_in_use (dynamic).</summary>
    private async Task PlantServerConfigAsync(DateTime captureTime, string name, long value)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_config
    (config_id, capture_time, server_id, server_name, configuration_name,
     value_configured, value_in_use, is_dynamic, is_advanced)
VALUES ($1, $2, $3, 'TestServer', $4, $5, $5, true, true)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(captureTime);
        P(_serverId);
        P(name);
        P(value);
        await cmd.ExecuteNonQueryAsync();
    }
}
