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
///
/// <para>#3740 adds the trace-anchor arms: a stored msg 15457 line for the same option in the span between
/// the two captures anchors the compare on the trace's time ("changed at", <c>anchor_source</c> = default
/// trace, the after half complete where the observation anchor's would have been clamped); a line for a
/// DIFFERENT option leaves the observation anchor untouched. Both plant the server at <b>UTC+10</b> and store
/// the line's <c>event_time</c> in the server's local frame, as the collector does, so a read that forgot to
/// de-skew would put the line ten hours in the future — outside the span — and the positive arm would fail on
/// selection, not merely on the rendered hour.</para>
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

        /* No trace line in the store: the observation anchor, and the fact says so (#3740). */
        Assert.NotNull(finding.RootFactMetadata);
        Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, finding.RootFactMetadata![ConfigChangeAttribution.MetaAnchorSource]);
        Assert.Equal(finding.RootFactMetadata[ConfigChangeAttribution.MetaChangeTimeUnix], finding.RootFactMetadata[ConfigChangeAttribution.MetaObservedAtUnix]);

        /* The compare ran over observed halves: the verdict counts are on the row, and it is not "unavailable". */
        Assert.Equal(0, finding.RootFactMetadata[ConfigChangeAttribution.MetaCompareUnavailable]);
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

    /* ── the trace anchor (#3740) ── */

    /// <summary>The planted server's offset: UTC+10, the direction that would SUPPRESS the anchor if the
    /// read forgot to de-skew (a local stamp read as UTC sits ten hours in the future, past the capture).</summary>
    private const int ServerOffsetMinutes = 600;

    /// <summary>
    /// MAXDOP 0 captured 30 h ago, MAXDOP 8 captured 3 h ago, and a stored msg 15457 line for MAXDOP 0 → 8
    /// stamped 5 h ago (UTC; stored at the server's local wall clock, +10 h): the finding anchors on the
    /// trace's time. The prose says "changed at" that UTC hour and names the trace; the metadata says
    /// <c>anchor_source</c> = default trace with the trace time as the change time and the capture as the
    /// observation; and the after half is COMPLETE (4 h, not clamped) because 5 h ago + 4 h is in the past —
    /// where the observation anchor, 3 h ago, would have reported 3 h clamped. That last pair is what the
    /// anchor is FOR: the compare answers the question about the change, not about the snapshot.
    /// </summary>
    [Fact]
    public async Task AMatchingTraceLineInTheSpan_AnchorsTheCompareOnTheTraceTime_AndSaysChangedAt()
    {
        var now = DateTime.UtcNow;
        var changedAtUtc = now.AddHours(-5);
        await PlantWaitSeriesAsync(now);
        await PlantServerOffsetAsync(now);
        await PlantServerConfigAsync(now.AddHours(-30), Maxdop, 0);
        await PlantServerConfigAsync(now.AddHours(-3), Maxdop, 8);
        await PlantReconfigureLineAsync(changedAtUtc, Maxdop, 0, 8);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
        var advice = FactAdvice.TryReadStoryText(finding.StoryText)!;
        Assert.Contains($"changed at {changedAtUtc:yyyy-MM-dd HH:mm} UTC (default trace: the sp_configure line, msg 15457)", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("first observed it 2 h later", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("compared the 4 h before the change with the 4 h after it", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("first observed by the configuration snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("still filling in", advice.Investigation, StringComparison.Ordinal);

        var m = finding.RootFactMetadata!;
        Assert.Equal(ConfigChangeAttribution.AnchorSourceDefaultTrace, m[ConfigChangeAttribution.MetaAnchorSource]);
        Assert.Equal(new DateTimeOffset(DateTime.SpecifyKind(changedAtUtc, DateTimeKind.Utc)).ToUnixTimeSeconds(), m[ConfigChangeAttribution.MetaChangeTimeUnix], precision: 0);
        Assert.Equal(2.0, m[ConfigChangeAttribution.MetaObservedLagHours], precision: 1);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaObservationGapHours]);
        Assert.True(m.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
        Assert.Equal(4.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaAfterWindowClamped]);
        Assert.Equal(0, m[ConfigChangeAttribution.MetaCompareUnavailable]);
    }

    /// <summary>
    /// The control: the SAME store shape, but the stored 15457 line names a DIFFERENT option (cost threshold
    /// for parallelism). The MAXDOP change is not dated by it, so the finding keeps the observation anchor
    /// exactly as #3720 shipped it — "first observed", the 27 h span, the clamped 3 h after half — and says
    /// so in <c>anchor_source</c>.
    /// </summary>
    [Fact]
    public async Task ATraceLineForADifferentOption_LeavesTheObservationAnchor()
    {
        var now = DateTime.UtcNow;
        await PlantWaitSeriesAsync(now);
        await PlantServerOffsetAsync(now);
        await PlantServerConfigAsync(now.AddHours(-30), Maxdop, 0);
        await PlantServerConfigAsync(now.AddHours(-3), Maxdop, 8);
        await PlantReconfigureLineAsync(now.AddHours(-5), "cost threshold for parallelism", 5, 50);

        var service = new AnalysisService(_duckDb);
        var findings = await service.AnalyzeAsync(_serverId, "TestServer");

        var finding = Assert.Single(findings, f => f.RootFactKey == ConfigChangeAttribution.FactKey);
        var advice = FactAdvice.TryReadStoryText(finding.StoryText)!;
        Assert.Contains("first observed by the configuration snapshot at", advice.Investigation, StringComparison.Ordinal);
        Assert.Contains("27 h since the previous snapshot", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("default trace", advice.Investigation, StringComparison.Ordinal);

        var m = finding.RootFactMetadata!;
        Assert.Equal(ConfigChangeAttribution.AnchorSourceObservation, m[ConfigChangeAttribution.MetaAnchorSource]);
        Assert.False(m.ContainsKey(ConfigChangeAttribution.TraceChangeTimeUnixKey(Maxdop)));
        Assert.Equal(27.0, m[ConfigChangeAttribution.MetaObservationGapHours], precision: 1);
        Assert.Equal(3.0, m[ConfigChangeAttribution.MetaAfterHoursObserved], precision: 1);
        Assert.Equal(1, m[ConfigChangeAttribution.MetaAfterWindowClamped]);
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

    /// <summary>The server's collected offset (<c>server_properties.utc_offset_minutes</c>, v42), which the
    /// trace-anchor read uses to shift its span into the server's frame and de-skew the rows back.</summary>
    private async Task PlantServerOffsetAsync(DateTime now)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO server_properties
    (collection_id, collection_time, server_id, server_name,
     edition, product_version, product_level, engine_edition,
     cpu_count, hyperthread_ratio, physical_memory_mb, utc_offset_minutes)
VALUES ($1, $2, $3, 'TestServer', 'Developer Edition', '16.0.4265.3', 'RTM', 3, 8, 1, 16384, $4)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(now.AddHours(-30));
        P(_serverId);
        P(ServerOffsetMinutes);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// One stored msg 15457 line as the collector writes it: an ErrorLog event (class 22), <c>error_number</c>
    /// 15457, severity 10, the TextData the raw error-log line (timestamp, spid, then the message — measured on
    /// SQL Server 2022), and <c>event_time</c> in the SERVER's local wall clock (<c>fn_trace_gettable</c>'s
    /// StartTime, stored raw): the UTC instant plus the planted offset.
    /// </summary>
    private async Task PlantReconfigureLineAsync(DateTime changedAtUtc, string option, long oldValue, long newValue)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO default_trace_events
    (default_trace_event_id, collection_time, server_id, server_name,
     event_time, event_name, event_class, spid, database_id, database_name, error_number, severity, text_data)
VALUES ($1, $2, $3, 'TestServer', $4, 'ErrorLog', 22, 95, 1, 'master', 15457, 10, $5)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(changedAtUtc.AddMinutes(1));
        P(_serverId);
        P(changedAtUtc.AddMinutes(ServerOffsetMinutes));
        P($"{changedAtUtc.AddMinutes(ServerOffsetMinutes):yyyy-MM-dd HH:mm:ss.ff} spid95      Configuration option '{option}' changed from {oldValue} to {newValue}. Run the RECONFIGURE statement to install.");
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
