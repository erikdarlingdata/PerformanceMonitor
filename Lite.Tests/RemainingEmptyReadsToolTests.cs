/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Notifications;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The last four reads on #2485's list, each of which needs a different shape of answer — which is why they
/// are here together rather than given one boilerplate envelope.
///
/// <list type="bullet">
/// <item><c>get_wait_types</c> is windowed, so a LIMIT 1 probe against the same source separates a quiet
/// window from a server nothing was ever stored for.</item>
/// <item><c>get_memory_clerks</c> is NOT windowed — it returns every clerk at MAX(collection_time) — so zero
/// rows is logically the same statement as zero rows in the table and a probe would agree with the read by
/// construction. What it owes the caller instead is that an empty clerk list is never a quiet period.</item>
/// <item><c>get_mute_rules</c> is config, not collection: both empties are true negatives, but "no rule was
/// ever written" and "five rules that all lapsed" are different states and the second is a mute somebody
/// intended.</item>
/// <item><c>compare_analysis</c> needs no probe at all — its own fact counts already hold the answer, and
/// the defect was that all-zero counters read as "nothing changed" rather than "nothing to compare".</item>
/// </list>
///
/// <para>Every message here is Darling's word for word; <c>McpMissMessageParityPinTests</c> holds that down
/// against both source trees.</para>
/// </summary>
public sealed class RemainingEmptyReadsToolTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "RemainingEmptySrv";

    private readonly DuckDbInitializer _duckDb;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 850000;

    public RemainingEmptyReadsToolTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-remaining-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);

        var server = new ServerConnection
        {
            Id = Guid.NewGuid().ToString(),
            ServerName = ServerName,
            IsEnabled = true,
        };
        _serverManager.AddServer(server);

        /* Derived, not stored -- seeding under a hardcoded id would write rows the tool looks past. */
        _serverId = RemoteCollectorService.GetDeterministicHashCode(
            RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    [Fact]
    public async Task WaitTypes_NeverCollected_IsNotAWindowToWiden()
    {
        var service = new LocalDataService(_duckDb);

        var root = JsonDocument.Parse(
            await McpWaitTools.GetWaitTypes(service, _serverManager, ServerName, 4)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("EVER", text, StringComparison.Ordinal);
        Assert.Contains("not an empty window", text, StringComparison.Ordinal);

        /* The second-cycle nuance is what makes this branch self-clearing on a freshly added server. */
        Assert.Contains("SECOND collection cycle", text, StringComparison.Ordinal);
        Assert.DoesNotContain("widen hours_back", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WaitTypes_CollectedButOutsideTheWindow_IsAWindowToWiden()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddHours(-48), "CXPACKET", 5000L);

        var root = JsonDocument.Parse(
            await McpWaitTools.GetWaitTypes(service, _serverManager, ServerName, 1)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("widen hours_back", text, StringComparison.Ordinal);
        Assert.DoesNotContain("EVER", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task WaitTypes_InsideTheWindow_StillReturnsTheList()
    {
        var service = new LocalDataService(_duckDb);
        await SeedWaitAsync(DateTime.UtcNow.AddMinutes(-10), "PAGEIOLATCH_SH", 900L);

        var root = JsonDocument.Parse(
            await McpWaitTools.GetWaitTypes(service, _serverManager, ServerName, 4)).RootElement;

        Assert.False(root.TryGetProperty("status", out _));
        Assert.Equal(1, root.GetProperty("wait_types").GetArrayLength());
    }

    /// <summary>
    /// One branch, on purpose. What is pinned is what the sentence REFUSES to imply: an empty clerk list is
    /// not a quiet period and not a window that wants widening, because a live SQL Server always has clerks.
    /// </summary>
    [Fact]
    public async Task MemoryClerks_EmptySnapshot_IsNeverDescribedAsAQuietPeriod()
    {
        var service = new LocalDataService(_duckDb);

        var root = JsonDocument.Parse(
            await McpMemoryTools.GetMemoryClerks(service, _serverManager, ServerName)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("never a quiet period", text, StringComparison.Ordinal);
        Assert.Contains("LATEST snapshot", text, StringComparison.Ordinal);

        /* No window, so no window to widen -- offering that would be advice that cannot work. */
        Assert.DoesNotContain("widen hours_back", text, StringComparison.Ordinal);
    }

    [Fact]
    public async Task MuteRules_NoneConfigured_SaysNothingIsSuppressedAnywhere()
    {
        var service = new MuteRuleService(new InMemoryMuteRuleStore(), new AppLoggerAdapter<MuteRuleService>());

        var root = JsonDocument.Parse(await McpAlertTools.GetMuteRules(service)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("No mute rules are configured", text, StringComparison.Ordinal);
        Assert.Equal(0, root.GetProperty("hints").GetProperty("configured_count").GetInt32());
    }

    /// <summary>
    /// The state the bare array could never express: rules exist, somebody meant them, and every one has
    /// lapsed. Nothing is being suppressed either way — which is why both branches are "empty" — but a
    /// caller auditing why a server looks quiet needs to know the rules are there.
    /// </summary>
    [Fact]
    public async Task MuteRules_AllLapsed_IsADifferentAnswerFromNoneConfigured()
    {
        var store = new InMemoryMuteRuleStore();
        var service = new MuteRuleService(store, new AppLoggerAdapter<MuteRuleService>());

        await service.AddRuleAsync(new MuteRule
        {
            Id = "lapsed-1",
            Enabled = true,
            CreatedAtUtc = DateTime.UtcNow.AddDays(-7),
            ExpiresAtUtc = DateTime.UtcNow.AddDays(-1),
            ServerName = ServerName,
            MetricName = "High CPU",
        });

        var root = JsonDocument.Parse(await McpAlertTools.GetMuteRules(service)).RootElement;

        Assert.Equal("empty", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("disabled or expired", text, StringComparison.Ordinal);
        Assert.Contains("enabled_only=false", text, StringComparison.Ordinal);

        /* And it must NOT reach for the none-configured sentence, which would be a flat lie here. */
        Assert.DoesNotContain("No mute rules are configured", text, StringComparison.Ordinal);

        var hints = root.GetProperty("hints");
        Assert.Equal(1, hints.GetProperty("configured_count").GetInt32());
        Assert.Equal(1, hints.GetProperty("excluded_by_filter").GetInt32());

        /* enabled_only=false is the escape hatch the message names, so it has to actually work. */
        var all = JsonDocument.Parse(await McpAlertTools.GetMuteRules(service, enabled_only: false)).RootElement;
        Assert.False(all.TryGetProperty("status", out _));
        Assert.Equal(1, all.GetProperty("total_count").GetInt32());
    }

    /// <summary>
    /// All-zero counters and <c>facts: []</c> read as "nothing changed" when the truth is "there was nothing
    /// to compare" — opposite conclusions about the same server. No probe is needed: the comparison list is
    /// the union of both windows' keys, so zero entries IS both fact sets being empty.
    /// </summary>
    [Fact]
    public async Task CompareAnalysis_WithNoFactsInEitherWindow_SaysThereWasNothingToCompare()
    {
        var analysis = new AnalysisService(_duckDb) { MinimumDataHours = 0 };

        var root = JsonDocument.Parse(
            await McpAnalysisTools.CompareAnalysis(analysis, _serverManager, ServerName, 4, 28)).RootElement;

        Assert.Equal("unavailable", root.GetProperty("status").GetString());
        var text = root.GetProperty("message").GetString()!;
        Assert.Contains("EITHER window", text, StringComparison.Ordinal);
        Assert.Contains("NOT a report that nothing changed", text, StringComparison.Ordinal);

        /* The windows it actually looked at, so the caller can check collection covered them. */
        var hints = root.GetProperty("hints");
        Assert.False(string.IsNullOrEmpty(hints.GetProperty("baseline_start").GetString()));
        Assert.False(string.IsNullOrEmpty(hints.GetProperty("comparison_end").GetString()));
    }

    /// <summary>In-memory store so the mute-rule branches are decided by the test, not by whatever rules a
    /// shared store happens to hold.</summary>
    private sealed class InMemoryMuteRuleStore : IMuteRuleStore
    {
        private readonly List<MuteRule> _rules = new();

        public Task<IReadOnlyList<MuteRule>> LoadAllAsync() => Task.FromResult<IReadOnlyList<MuteRule>>(_rules.ToList());
        public Task InsertAsync(MuteRule rule) { _rules.Add(rule); return Task.CompletedTask; }
        public Task UpdateAsync(MuteRule rule) => Task.CompletedTask;
        public Task SetEnabledAsync(string ruleId, bool enabled) => Task.CompletedTask;
        public Task DeleteAsync(string ruleId) { _rules.RemoveAll(r => r.Id == ruleId); return Task.CompletedTask; }
        public Task DeleteExpiredAsync(IReadOnlyList<string> expiredIds) => Task.CompletedTask;
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

    private async Task SeedWaitAsync(DateTime collectionTimeUtc, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var connection = await SeedConnectionAsync();
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"
INSERT INTO wait_stats
    (collection_id, collection_time, server_id, server_name, wait_type,
     waiting_tasks_count, wait_time_ms, signal_wait_time_ms,
     delta_waiting_tasks, delta_wait_time_ms, delta_signal_wait_time_ms)
VALUES ($1, $2, $3, $4, $5, 0, 0, 0, $6, $7, 0)";
        cmd.Parameters.Add(new DuckDBParameter { Value = _nextId++ });
        cmd.Parameters.Add(new DuckDBParameter { Value = DateTime.SpecifyKind(collectionTimeUtc, DateTimeKind.Unspecified) });
        cmd.Parameters.Add(new DuckDBParameter { Value = _serverId });
        cmd.Parameters.Add(new DuckDBParameter { Value = ServerName });
        cmd.Parameters.Add(new DuckDBParameter { Value = waitType });
        cmd.Parameters.Add(new DuckDBParameter { Value = 10L });
        cmd.Parameters.Add(new DuckDBParameter { Value = deltaWaitMs });
        await cmd.ExecuteNonQueryAsync();
    }
}
