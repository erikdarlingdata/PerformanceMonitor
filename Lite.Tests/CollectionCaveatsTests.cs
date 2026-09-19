/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
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
/// #3691: a pass whose fact families silently failed says so. Every family read in the DuckDB collector degrades
/// to "no facts" and LOGS (#2826); until now the pass learned nothing from it, so a family that could not be
/// read looked exactly like a family that read and found nothing — and an all-clear over it was a clean card
/// that was actually blind. The failure is now RECORDED on the <see cref="AnalysisContext"/> beside the log
/// line, carried out of the pass, and rendered by <c>analyze_server</c> as <c>collection_caveats</c> plus a
/// sentence in the status prose — ONLY when a family failed. A clean pass's payload is the same object
/// through the same serializer call, so it carries no such field and its bytes are what they were.
///
/// <para>The fault planted here is a real one on a real store: the trace-flag family's <c>v_trace_flags</c>
/// view is dropped, so <c>CollectTraceFlagFactsAsync</c>'s read fails while every other family runs. A
/// private <see cref="DuckDbInitializer"/> on a temp path (the <c>AnalysisDataSpanTests</c> shape) rather than
/// the shared fixture, because the drop must not reach any other test's store.</para>
/// </summary>
public sealed class CollectionCaveatsTests : IDisposable
{
    private const string StaleWait = "CC3691_STALE_WAIT";
    private const string BenignWait = "CC3691_BENIGN_WAIT";

    private readonly string _tempDir;
    private readonly string _dbPath;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -3_691_000;

    public CollectionCaveatsTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "CollectionCaveatsTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _dbPath = Path.Combine(_tempDir, "test.duckdb");

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
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch { /* best-effort cleanup */ }
    }

    /// <summary>
    /// The collector level: one family's read fails, the context records exactly that family (labelled off the
    /// method name, <c>CollectTraceFlagFactsAsync</c> → <c>trace_flag</c>) as an <c>error</c> — Lite has no
    /// timeout that can arrive at the reporter — with the family total stamped from the type, and every other
    /// family still runs (the pass is not lost to one missing view).
    /// </summary>
    [Fact]
    public async Task AFamilyWhoseReadFails_IsRecordedOnTheContext_AndTheOthersStillRun()
    {
        var duckDb = await InitializeAsync();
        await ExecuteAsync("DROP VIEW v_trace_flags");
        var now = DateTime.UtcNow;
        for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 100L);

        var collector = new DuckDbFactCollector(duckDb);
        var context = new AnalysisContext
        {
            ServerId = _serverId,
            ServerName = "TestServer",
            TimeRangeStart = now.AddHours(-4),
            TimeRangeEnd = now,
        };

        var facts = await collector.CollectFactsAsync(context);

        Assert.Equal(32, context.CollectionFamilyCount);
        var failure = Assert.Single(context.CollectionFailures);
        Assert.Equal("trace_flag", failure.Family);
        Assert.Equal("CollectTraceFlagFactsAsync", failure.Read);
        Assert.Equal(CollectionFailureOutcome.Error, failure.Outcome);
        Assert.Contains("v_trace_flags", failure.Message, StringComparison.OrdinalIgnoreCase);

        /* The rest of the pass happened: the wait family read its benign wait over an observed window. */
        Assert.NotNull(context.Coverage);
        Assert.True(context.Coverage!.IsObserved);
        Assert.Contains(facts, f => f.Key == BenignWait);
    }

    /// <summary>
    /// The tool level, both arms. The same planted store — 25 hours of history so the real 24h gate passes, a
    /// benign wait every fifteen minutes so the window is observed and nothing fires — is analysed twice:
    /// first CLEAN, where the <c>empty</c> all-clear carries no <c>collection_caveats</c> and states its two
    /// fact counts; then with <c>v_trace_flags</c> dropped, where the same all-clear carries the block naming
    /// <c>trace_flag</c>, <c>1 of 32</c>, and the caveat sentence in its message.
    /// </summary>
    [Fact]
    public async Task AnAllClearOverAFailedFamily_SaysSo_AndACleanOneDoesNot()
    {
        var duckDb = await InitializeAsync();
        await PlantWaitAsync(DateTime.UtcNow.AddHours(-30), StaleWait, 60_000L);
        var now = DateTime.UtcNow;
        for (var minutesAgo = 240; minutesAgo >= 0; minutesAgo -= 15)
            await PlantWaitAsync(now.AddMinutes(-minutesAgo), BenignWait, 100L);

        var service = new AnalysisService(duckDb);

        /* ── clean: the all-clear, no caveat field, the two counts present. */
        var clean = await McpAnalysisTools.AnalyzeServer(service, _serverManager);
        using (var doc = JsonDocument.Parse(clean))
        {
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            var hints = doc.RootElement.GetProperty("hints");
            Assert.False(hints.TryGetProperty("collection_caveats", out _), "a clean pass must not carry collection_caveats — not even as null");
            Assert.True(hints.GetProperty("fact_count").GetInt32() > 0, "the benign wait is a fact the scorer saw");
            Assert.True(hints.TryGetProperty("facts_scored", out var scored) && scored.ValueKind == JsonValueKind.Number);
        }
        Assert.DoesNotContain("COLLECTION CAVEAT", clean, StringComparison.Ordinal);
        Assert.Contains("All metrics are within normal ranges", clean, StringComparison.Ordinal);
        Assert.Empty(service.LastCollectionFailures);
        Assert.Equal(32, service.LastCollectionFamilyCount);

        /* ── one family blind: same status, the block and the sentence. */
        await ExecuteAsync("DROP VIEW v_trace_flags");
        var blind = await McpAnalysisTools.AnalyzeServer(service, _serverManager);
        using (var doc = JsonDocument.Parse(blind))
        {
            Assert.Equal("empty", doc.RootElement.GetProperty("status").GetString());
            var message = doc.RootElement.GetProperty("message").GetString()!;
            Assert.Contains("All metrics are within normal ranges. COLLECTION CAVEAT: 1 of 32 fact families could not be read (trace_flag (error)) — the absence of findings is not evidence", message, StringComparison.Ordinal);

            var caveats = doc.RootElement.GetProperty("hints").GetProperty("collection_caveats");
            Assert.Equal(1, caveats.GetProperty("families_failed").GetInt32());
            Assert.Equal(32, caveats.GetProperty("families_total").GetInt32());
            var entry = Assert.Single(caveats.GetProperty("entries").EnumerateArray());
            Assert.Equal("trace_flag", entry.GetProperty("family").GetString());
            Assert.Equal("CollectTraceFlagFactsAsync", entry.GetProperty("read").GetString());
            Assert.Equal("error", entry.GetProperty("outcome").GetString());
            Assert.Contains("v_trace_flags", entry.GetProperty("message").GetString(), StringComparison.OrdinalIgnoreCase);

            /* The block is appended LAST, after every property the clean payload had. */
            var names = doc.RootElement.GetProperty("hints").EnumerateObject().Select(p => p.Name).ToArray();
            Assert.Equal("collection_caveats", names[^1]);
            Assert.Contains("coverage", names);
            Assert.Contains("fact_count", names);
        }
        var failure = Assert.Single(service.LastCollectionFailures);
        Assert.Equal("trace_flag", failure.Family);

        /* get_analysis_facts, same store: the data result carries the block and the caveat string. */
        var facts = await McpAnalysisTools.GetAnalysisFacts(service, _serverManager);
        using (var doc = JsonDocument.Parse(facts))
        {
            Assert.True(doc.RootElement.TryGetProperty("total_facts", out _), "the facts read returned data");
            Assert.Contains("1 of 32 fact families could not be read (trace_flag (error))", doc.RootElement.GetProperty("caveat").GetString(), StringComparison.Ordinal);
            var caveats = doc.RootElement.GetProperty("collection_caveats");
            Assert.Equal("trace_flag", Assert.Single(caveats.GetProperty("entries").EnumerateArray()).GetProperty("family").GetString());
            Assert.Equal("collection_caveats", doc.RootElement.EnumerateObject().Last().Name);
        }
    }

    private DuckDbInitializer? _duckDb;
    private DuckDBConnection? _seedConn;

    private async Task<DuckDbInitializer> InitializeAsync()
    {
        _duckDb = new DuckDbInitializer(_dbPath);
        await _duckDb.InitializeAsync();
        return _duckDb;
    }

    private async Task<DuckDBConnection> SeedConnectionAsync()
    {
        if (_seedConn is null)
        {
            _seedConn = _duckDb!.CreateConnection();
            await _seedConn.OpenAsync(TestContext.Current.CancellationToken);
        }
        return _seedConn;
    }

    private async Task ExecuteAsync(string sql)
    {
        using var readLock = _duckDb!.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task PlantWaitAsync(DateTime at, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb!.AcquireReadLock();
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
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }
}
