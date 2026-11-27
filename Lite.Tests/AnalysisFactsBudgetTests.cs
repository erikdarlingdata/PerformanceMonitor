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
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Analysis;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9195: get_analysis_facts and compare_analysis answer within the 32,768-byte response budget every other Lite read honours (#9140),
/// and say so when they cut. A wide window on a busy server scored 100+ facts and answered 33 KB with no key saying the answer was over
/// the budget. The store here holds 160 distinct wait types, so the whole fact list is far past the budget.
/// </summary>
public sealed class AnalysisFactsBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const int WaitTypeCount = 160;

    /* compare_analysis also carries a family row per fact outside the cut, so fewer facts already pass the budget. */
    private const int CompareWaitTypeCount = 70;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -9_195_000;
    private DuckDBConnection? _seedConn;

    public AnalysisFactsBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;

        _tempDir = Path.Combine(Path.GetTempPath(), "AnalysisFactsBudgetTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);

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

    [Fact]
    public async Task GetAnalysisFacts_OverTheBudget_IsCutWorstFirst_AndCarriesTheBudgetKeys()
    {
        await PlantWindowAsync(DateTime.UtcNow);

        var result = await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager);

        Assert.True(Encoding.UTF8.GetByteCount(result) <= McpResponseBudget.DefaultBytes, "the reply stays inside the response budget");
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        var returned = root.GetProperty("facts").GetArrayLength();
        var scored = root.GetProperty("total_facts").GetInt32();
        Assert.True(scored >= WaitTypeCount, "every wait type scored a fact");
        Assert.True(returned < scored, "the budget cut something");
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Contains("min_severity", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
        Assert.Equal(scored, root.GetProperty("shown").GetInt32());

        /* What was kept is the worst end of the list: severity never rises down the page. */
        var severities = root.GetProperty("facts").EnumerateArray().Select(f => f.GetProperty("severity").GetDouble()).ToList();
        Assert.Equal(severities.OrderByDescending(s => s), severities);
    }

    [Fact]
    public async Task GetAnalysisFacts_UnderTheBudget_IsWhole_AndSaysNothingWasCut()
    {
        await PlantWindowAsync(DateTime.UtcNow);

        var result = await McpAnalysisTools.GetAnalysisFacts(CreateService(), _serverManager, null, 4, "config");
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
        Assert.Equal(root.GetProperty("shown").GetInt32(), root.GetProperty("facts").GetArrayLength());
    }

    [Fact]
    public async Task CompareAnalysis_OverTheBudget_IsCut_AndCarriesTheBudgetKeys()
    {
        var now = DateTime.UtcNow;
        await PlantWindowAsync(now.AddHours(-24), CompareWaitTypeCount);
        await PlantWindowAsync(now, CompareWaitTypeCount);

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        Assert.True(Encoding.UTF8.GetByteCount(result) <= McpResponseBudget.DefaultBytes, "the reply stays inside the response budget");
        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.True(root.GetProperty("facts").GetArrayLength() < root.GetProperty("facts_total").GetInt32(), "the budget cut something");
        Assert.True(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Contains("hours_back", root.GetProperty("budget_note").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task CompareAnalysis_UnderTheBudget_SaysNothingWasCut()
    {
        var now = DateTime.UtcNow;
        await PlantSeriesAsync("CXPACKET", now.AddHours(-24));
        await PlantSeriesAsync("CXPACKET", now);

        var result = await McpAnalysisTools.CompareAnalysis(CreateService(), _serverManager, null, 4, 28);

        using var doc = JsonDocument.Parse(result);
        var root = doc.RootElement;
        Assert.False(root.GetProperty("budget_truncated").GetBoolean());
        Assert.Equal(JsonValueKind.Null, root.GetProperty("budget_note").ValueKind);
    }

    private AnalysisService CreateService() => new(_duckDb) { MinimumDataHours = 0 };

    /// <summary>One fully collected series for the first wait type, and one delta row inside the window for each of the others.</summary>
    private async Task PlantWindowAsync(DateTime windowEnd, int waitTypeCount = WaitTypeCount)
    {
        await PlantSeriesAsync("CXPACKET", windowEnd);
        for (var i = 0; i < waitTypeCount - 1; i++)
            await PlantWaitAsync(windowEnd.AddMinutes(-60), $"B9195_WAIT_{i:000}", 20_000 + (i * 1000L));
    }

    private async Task PlantSeriesAsync(string waitType, DateTime windowEnd)
    {
        var start = windowEnd.AddHours(-4);
        await PlantWaitAsync(start, waitType, 0L);
        for (var i = 1; i <= 16; i++)
            await PlantWaitAsync(start.AddMinutes(15 * i), waitType, 56_250L);
    }

    private async Task PlantWaitAsync(DateTime at, string waitType, long deltaWaitMs)
    {
        using var readLock = _duckDb.AcquireReadLock();
        _seedConn ??= _duckDb.CreateConnection();
        if (_seedConn.State != System.Data.ConnectionState.Open)
            await _seedConn.OpenAsync();
        using var cmd = _seedConn.CreateCommand();
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
}
