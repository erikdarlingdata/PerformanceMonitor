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
using System.Threading;
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
    /// #6132: collection that came back seconds ago, after more than a week offline, leaves ONE row in the window
    /// (the first collection after the outage has no earlier one to difference against). The window is unobserved
    /// all the same, but collection is running: the answer must say it was missing until that row and has resumed,
    /// not that it "stopped or broken".
    /// </summary>
    [Fact]
    public async Task ACollectorThatResumedSecondsAgo_IsToldAsResumed_NotAsStoppedOrBroken()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddDays(-9), StaleWait, 60_000L);
        await PlantWaitAsync(DateTime.UtcNow.AddSeconds(-50), StaleWait, 60_000L);

        var service = new AnalysisService(_duckDb);
        var result = await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        using var doc = JsonDocument.Parse(result);
        Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
        var message = service.WindowEmptyMessage;
        Assert.NotNull(message);
        Assert.Contains("Collection was missing until about", message, StringComparison.Ordinal);
        Assert.Contains("has resumed", message, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("stopped or broken", result, StringComparison.Ordinal);
        Assert.DoesNotContain("why they stopped", result, StringComparison.Ordinal);
    }

    /// <summary>The other half: when no collection landed in the window at all, nothing in it shows a resumption,
    /// so the answer says that and keeps the all-clear warning without claiming either outcome.</summary>
    [Fact]
    public async Task AWindowWithNoCollectionAtAll_SaysNothingLandedAndDoesNotClaimResumption()
    {
        await PlantWaitAsync(DateTime.UtcNow.AddDays(-9), StaleWait, 60_000L);
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), StaleWait, 60_000L);

        var service = new AnalysisService(_duckDb);
        await McpAnalysisTools.AnalyzeServer(service, _serverManager);

        var message = service.WindowEmptyMessage;
        Assert.NotNull(message);
        Assert.Contains("No collection landed inside it", message, StringComparison.Ordinal);
        Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
        Assert.DoesNotContain("has resumed.", message, StringComparison.Ordinal);
        Assert.DoesNotContain("appears to have stopped", message, StringComparison.Ordinal);
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

    /// <summary>
    /// #8910: a window older than the history Lite keeps (the archive's three months) is answered as removed by retention, not
    /// as a collector that "may have stopped or broken". The store has the shape a real archive purge leaves: the month files
    /// before the cutoff are gone, so the oldest row is in the archive, in the first month the purge kept
    /// (<see cref="RetentionService.OldestRetainedInstant"/>), up to a month AFTER the raw cutoff. A recent hot row keeps the
    /// span far over the history gate; <c>as_of</c> puts the window ten days before the oldest row. Both tools say so, and
    /// neither points at collection health. A window INSIDE the span with no rows, and with over a day of history before its
    /// end, keeps the collection wording: collection may well have missed it.
    /// </summary>
    [Fact]
    public async Task AWindowOlderThanRetention_SaysRetentionRemovedIt_AndOtherEmptyWindowsKeepTheCollectionText()
    {
        var now = DateTime.UtcNow;
        string AsOf(double daysAgo) => now.AddDays(-daysAgo).ToString("o", System.Globalization.CultureInfo.InvariantCulture);
        var oldestKept = DateTime.SpecifyKind(RetentionService.OldestRetainedInstant(now), DateTimeKind.Utc).AddHours(3);
        var oldestDaysAgo = (now - oldestKept).TotalDays;

        /* Two archived rows two days apart: the history gate measures to a window's end, so the inside window below has a
           day of history before it. */
        await PlantWaitAsync(oldestKept.AddDays(2), StaleWait, 60_000L);
        await ArchiveWaitAsync(oldestKept);
        try
        {
            await PlantWaitAsync(now.AddHours(-5), StaleWait, 60_000L);

            var service = new AnalysisService(_duckDb);
            var removed = await McpAnalysisTools.AnalyzeServer(service, _serverManager, hours_back: 48, as_of: AsOf(oldestDaysAgo + 10));
            using (var doc = JsonDocument.Parse(removed))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains("3 months of retention for wait_stats", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.Contains("NOT an all-clear", message, StringComparison.Ordinal);
                Assert.DoesNotContain("stopped or broken", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("total collected history", message, StringComparison.Ordinal);
            }
            Assert.True(service.WindowEmptyRemovedByRetention);

            var removedFacts = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, hours_back: 48, as_of: AsOf(oldestDaysAgo + 10));
            using (var doc = JsonDocument.Parse(removedFacts))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("may not have run yet", message, StringComparison.Ordinal);
            }

            /* A window INSIDE the retained span that holds no rows: today's text, today's pointer. */
            var inside = await McpAnalysisTools.AnalyzeServer(service, _serverManager, hours_back: 4, as_of: AsOf(oldestDaysAgo / 2.0));
            using (var doc = JsonDocument.Parse(inside))
            {
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("Collection may have stopped or broken", message, StringComparison.Ordinal);
                Assert.Contains("get_collection_health", message, StringComparison.Ordinal);
                Assert.DoesNotContain("Retention removed it", message, StringComparison.Ordinal);
            }
            Assert.False(service.WindowEmptyRemovedByRetention);

            var insideFacts = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, hours_back: 4, as_of: AsOf(oldestDaysAgo / 2.0));
            Assert.DoesNotContain("Retention removed it", insideFacts, StringComparison.Ordinal);
        }
        finally
        {
            await RemoveArchiveAsync();
        }
    }

    /// <summary>
    /// #9218: a replay whose whole window ends before the oldest row the store keeps is answered from one lookup of that row, and
    /// the fact collector never runs (no family is read, no coverage is measured), in <c>analyze_server</c> and in
    /// <c>get_analysis_facts</c>. A window that reaches the oldest row, and one inside retention, still run it, and the facts the
    /// tool returns are the ones the service scores, in severity order.
    /// </summary>
    [Fact]
    public async Task AWindowWhollyBeforeRetention_ReadsNoFacts_AndAWindowThatReachesTheFloorOrSitsInsideStillDoes()
    {
        var now = DateTime.UtcNow;
        string Iso(DateTime at) => at.ToString("o", System.Globalization.CultureInfo.InvariantCulture);
        var oldestKept = DateTime.SpecifyKind(RetentionService.OldestRetainedInstant(now), DateTimeKind.Utc).AddHours(3);

        await ArchiveWaitAsync(oldestKept);
        try
        {
            for (var hours = 1; hours <= 30; hours++)
                await PlantWaitAsync(oldestKept.AddHours(hours), BenignWait, 100L);
            for (var minutesAgo = 600; minutesAgo >= 0; minutesAgo -= 15)
                await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 100L);

            var service = new AnalysisService(_duckDb);

            var farBefore = Iso(oldestKept.AddDays(-10));
            var removed = await McpAnalysisTools.AnalyzeServer(service, _serverManager, hours_back: 4, as_of: farBefore);
            Assert.Equal(0, service.LastCollectionFamilyCount);
            Assert.Null(service.LastWindowCoverage);
            Assert.True(service.WindowEmptyRemovedByRetention);
            var removedFacts = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, hours_back: 4, as_of: farBefore);
            foreach (var answer in new[] { removed, removedFacts })
            {
                using var doc = JsonDocument.Parse(answer);
                Assert.Equal("unavailable", doc.RootElement.GetProperty("status").GetString());
                var message = doc.RootElement.GetProperty("message").GetString()!;
                Assert.Contains("This window ends before the oldest row this store keeps (", message, StringComparison.Ordinal);
                Assert.Contains("3 months of retention for wait_stats", message, StringComparison.Ordinal);
                Assert.Contains("Retention removed it, and collection did not break.", message, StringComparison.Ordinal);
                Assert.DoesNotContain("get_collection_health", message, StringComparison.Ordinal);
            }

            /* get_analysis_facts answers from its own early return, which says no fact was read; the fall-through arms count facts instead. */
            using (var factsDoc = JsonDocument.Parse(removedFacts))
                Assert.Contains("no fact was read for it", factsDoc.RootElement.GetProperty("message").GetString()!, StringComparison.Ordinal);

            /* A window that starts before the oldest row and ends a day after it keeps its path: the collector runs. */
            var straddleEnd = Iso(oldestKept.AddHours(28));
            var straddle = await McpAnalysisTools.AnalyzeServer(service, _serverManager, hours_back: 48, as_of: straddleEnd);
            Assert.True(service.LastCollectionFamilyCount > 0);
            Assert.NotNull(service.LastWindowCoverage);
            Assert.False(service.WindowEmptyRemovedByRetention);
            using (var doc = JsonDocument.Parse(straddle))
                Assert.NotEqual("unavailable", doc.RootElement.GetProperty("status").GetString());

            /* A window inside retention returns the facts the service scores, in severity order. */
            var insideEnd = now.AddMinutes(-30);
            var insideJson = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager, hours_back: 4, as_of: Iso(insideEnd));
            var (scored, _, _) = await service.CollectAndScoreFactsAsync(
                _serverId, "TestServer", 4, DateTime.SpecifyKind(insideEnd, DateTimeKind.Utc), CancellationToken.None);
            Assert.NotEmpty(scored);
            using (var doc = JsonDocument.Parse(insideJson))
            {
                Assert.True(doc.RootElement.TryGetProperty("facts", out _));
                var returned = doc.RootElement.GetProperty("facts").EnumerateArray()
                    .Select(f => f.GetProperty("key").GetString()!).ToList();
                Assert.Equal(scored.OrderByDescending(f => f.Severity).Select(f => f.Key).ToList(), returned);
            }
        }
        finally
        {
            await RemoveArchiveAsync();
        }
    }

    /// <summary>
    /// #8910: a store whose purge has stalled keeps rows far older than the horizon, so a window before the server was first
    /// monitored ends before the oldest row too. The store never held that window, and the answer must not say retention
    /// removed it.
    /// </summary>
    [Fact]
    public async Task AWindowBeforeAStalledPurgeStoresFirstRow_KeepsTheCollectionText_NotARetentionClaim()
    {
        var now = DateTime.UtcNow;
        var horizon = now.AddMonths(-RetentionService.ArchiveRetentionMonths);
        await PlantWaitAsync(horizon.AddDays(-10), StaleWait, 60_000L);
        await PlantWaitAsync(now.AddHours(-5), StaleWait, 60_000L);

        var service = new AnalysisService(_duckDb);
        var result = await McpAnalysisTools.AnalyzeServer(
            service, _serverManager, hours_back: 48,
            as_of: horizon.AddDays(-20).ToString("o", System.Globalization.CultureInfo.InvariantCulture));

        Assert.DoesNotContain("Retention removed it", result, StringComparison.Ordinal);
        Assert.Contains("Collection may have stopped or broken", result, StringComparison.Ordinal);
        Assert.False(service.WindowEmptyRemovedByRetention);
    }

    /// <summary>
    /// #8910: a young store's first row is five days old, far past the retention horizon, so a window ten days back was never
    /// held. Retention did not remove it, and the message must not say it did.
    /// </summary>
    [Fact]
    public async Task AWindowBeforeAYoungStoresFirstRow_KeepsTheCollectionText_NotARetentionClaim()
    {
        var now = DateTime.UtcNow;
        await PlantWaitAsync(now.AddDays(-5), StaleWait, 60_000L);
        await PlantWaitAsync(now.AddHours(-5), StaleWait, 60_000L);

        var service = new AnalysisService(_duckDb);
        var result = await McpAnalysisTools.AnalyzeServer(
            service, _serverManager, hours_back: 48,
            as_of: now.AddDays(-10).ToString("o", System.Globalization.CultureInfo.InvariantCulture));

        Assert.DoesNotContain("Retention removed it", result, StringComparison.Ordinal);
        Assert.Contains("Collection may have stopped or broken", result, StringComparison.Ordinal);
        Assert.False(service.WindowEmptyRemovedByRetention);
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

    private DateTime? _archivedMonth;

    private string ArchiveFile(DateTime month) =>
        Path.Combine(_duckDb.ArchivePath, month.ToString("yyyyMM", System.Globalization.CultureInfo.InvariantCulture) + "_wait_stats.parquet").Replace("\\", "/");

    /// <summary>
    /// Leaves the one wait_stats row at <paramref name="at"/> in the archive only, as a monthly archive file named for its month,
    /// and none in the hot table: the shape the archive purge leaves behind when it deletes the older months.
    /// </summary>
    private async Task ArchiveWaitAsync(DateTime at)
    {
        await PlantWaitAsync(at, StaleWait, 60_000L);
        Directory.CreateDirectory(_duckDb.ArchivePath);
        _archivedMonth = at;
        using (var readLock = _duckDb.AcquireReadLock())
        {
            var conn = await SeedConnectionAsync();
            using var copy = conn.CreateCommand();
            copy.CommandText = $"COPY (SELECT * FROM wait_stats) TO '{ArchiveFile(at)}' (FORMAT PARQUET)";
            await copy.ExecuteNonQueryAsync();
            using var delete = conn.CreateCommand();
            delete.CommandText = "DELETE FROM wait_stats";
            await delete.ExecuteNonQueryAsync();
        }
        await _duckDb.CreateArchiveViewsAsync();
    }

    private async Task RemoveArchiveAsync()
    {
        if (_archivedMonth is { } month)
        {
            try { File.Delete(ArchiveFile(month)); }
            catch { /* best-effort cleanup */ }
            await _duckDb.CreateArchiveViewsAsync();
            _archivedMonth = null;
        }
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
