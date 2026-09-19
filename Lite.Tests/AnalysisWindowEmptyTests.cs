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
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3524: <c>analyze_server</c> must not answer "all metrics are within normal ranges" when the
/// analysis window collected NOTHING. The pipeline's data-span gate measures LIFETIME history, so a
/// server whose collection died (broken credential, unreachable target) still passes it — and the
/// zero-facts pass used to return a bare <c>[]</c> that the tool rendered as a true-negative
/// all-clear. Both sides of the distinction are asserted, on the REAL 24h gate rather than a zeroed
/// one, because the bug lives precisely in the gap between "enough history" and "an empty window".
///
/// <para>#3653 adds the third kind of nothing, which the first fix got wrong once every collector stamped
/// coverage (#3592): a window the collector WAS up for, over which nothing rose to a fact. That is a
/// measurement, and it must not wear the dead-collector envelope. The unobserved-window-with-point-in-time-
/// facts arm lives in <see cref="AnalysisCoverageTests"/>.</para>
/// </summary>
public sealed class AnalysisWindowEmptyTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string StaleWait = "WE3524_STALE_WAIT";
    private const string BenignWait = "WE3524_BENIGN_WAIT";

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -3_524_000;
    private DuckDBConnection? _seedConn;

    public AnalysisWindowEmptyTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AnalysisWindowEmptyTests_" + Guid.NewGuid().ToString("N")[..8]);
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

    /// <summary>
    /// The dead-collector shape: 25 hours of history (the real 24h gate PASSES) whose newest row is
    /// five hours old, so the tool's default 4h window holds nothing. That must come back as
    /// <c>unavailable</c> with the pointer at collection health — never as the all-clear prose an
    /// operator (or their agent) would read as "this server is fine".
    /// </summary>
    [Fact]
    public async Task ADeadCollectorWindow_IsUnavailable_NotAnAllClear()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), StaleWait, 60_000L);
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-5), StaleWait, 60_000L);

        var service = new AnalysisService(_duckDb);
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("get_collection_health", result, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", result, StringComparison.Ordinal);
        Assert.DoesNotContain("within normal ranges", result, StringComparison.Ordinal);

        /* The service says WHICH kind of nothing this was: the window, not the lifetime span. */
        Assert.NotNull(service.WindowEmptyMessage);
        Assert.Null(service.InsufficientDataMessage);

        /* The #2506 persistence disclosure survives the new envelope — an anchored empty-window run
           still owes the caller that context, and this unanchored one reports the ordinary answer. */
        Assert.True(doc.RootElement.GetProperty("hints").GetProperty("persisted").GetBoolean());
    }

    /// <summary>
    /// The control, and the reason the fix is a distinction rather than a rewording: the SAME server
    /// with a benign wait collected THROUGHOUT the window has facts to score, finds nothing wrong, and
    /// keeps the genuine true-negative all-clear.
    ///
    /// <para>The series is a realistic one — a reading every fifteen minutes from the window's start
    /// to now — rather than the single row the control originally planted, because since #3538 A2 the
    /// engine divides by the time the collector actually observed, and a lone reading with nothing
    /// before it observes no time at all (its delta was the calculator's first sighting). A single
    /// in-window row is now, correctly, the dead-collector shape; the all-clear is earned by a window
    /// the collector was up for.</para>
    /// </summary>
    [Fact]
    public async Task AWindowWithFactsButNoFindings_KeepsTheAllClear()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), StaleWait, 60_000L);
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-5), StaleWait, 60_000L);

        var now = DateTime.UtcNow;
        for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 100L);

        var service = new AnalysisService(_duckDb);
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.Contains("All metrics are within normal ranges", result, StringComparison.Ordinal);
        Assert.DoesNotContain("PARTIAL", result, StringComparison.Ordinal);
        Assert.Null(service.WindowEmptyMessage);

        /* #3538 A2: the all-clear now says how much of the window it speaks for. */
        Assert.NotNull(service.LastWindowCoverage);
        Assert.False(service.LastWindowCoverage!.IsPartial);
        Assert.InRange(
            doc.RootElement.GetProperty("hints").GetProperty("coverage").GetProperty("observed_fraction").GetDouble(),
            0.99, 1.0);
    }

    /// <summary>
    /// #3653: the collector was UP for the whole window — a reading every fifteen minutes, so the coverage
    /// witness finds intervals inside it — but every reading carries ZERO accrued wait time, so no wait
    /// fact is emitted (<c>delta_wait_time_ms &gt; 0</c> is the wait read's filter) and no other family has
    /// a row for this server to read: an observed window that produced no fact at all. Until #3653 the
    /// service's rule was <c>facts.Count == 0 || ObservedDurationMs &lt;= 0</c>, and the first half made
    /// this window wear the dead-collector envelope above — "collection appears to have stopped or broken
    /// … NOT an all-clear", rendered <c>unavailable</c> with a pointer at collection health — for a
    /// collector the witness proves was running. It is <c>empty</c>: an all-clear that rests on the
    /// coverage the payload states, with none of the dead-collector prose.
    ///
    /// <para>The fact count is asserted directly through <c>CollectAndScoreFactsAsync</c> so this arm is
    /// known to exercise the zero-facts path and not merely "facts but no findings", which
    /// <see cref="AWindowWithFactsButNoFindings_KeepsTheAllClear"/> already covers.</para>
    /// </summary>
    [Fact]
    public async Task AnObservedWindowWithNoFacts_IsAnAllClearAtItsCoverage_NotADeadCollector()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), StaleWait, 60_000L);

        var now = DateTime.UtcNow;
        for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 0L);

        var service = new AnalysisService(_duckDb);

        /* The precondition this arm exists for: observed time, zero facts. */
        var (facts, coverage, _) = await service.CollectAndScoreFactsAsync(_serverId, "TestServer");
        Assert.Empty(facts);
        Assert.NotNull(coverage);
        Assert.True(coverage!.IsObserved);
        Assert.False(coverage.IsPartial);

        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
        Assert.DoesNotContain("NOT an all-clear", result, StringComparison.Ordinal);
        Assert.DoesNotContain("appears to have stopped", result, StringComparison.Ordinal);
        Assert.DoesNotContain("get_collection_health", result, StringComparison.Ordinal);
        Assert.DoesNotContain("PARTIAL", result, StringComparison.Ordinal);

        /* The all-clear says which coverage it rests on. */
        var payloadCoverage = doc.RootElement.GetProperty("hints").GetProperty("coverage");
        Assert.False(payloadCoverage.GetProperty("unobserved").GetBoolean());
        Assert.False(payloadCoverage.GetProperty("partial").GetBoolean());
        Assert.InRange(payloadCoverage.GetProperty("observed_fraction").GetDouble(), 0.99, 1.0);

        /* Neither miss message is set: this pass ran, and the worker-side marker would clear. */
        Assert.Null(service.WindowEmptyMessage);
        Assert.Null(service.InsufficientDataMessage);
        Assert.NotNull(service.LastWindowCoverage);
        Assert.True(service.LastWindowCoverage!.IsObserved);
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
VALUES ($1, $2, $3, 'TestServer', $4, 50, $5, 0, 50, $5, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }
}
