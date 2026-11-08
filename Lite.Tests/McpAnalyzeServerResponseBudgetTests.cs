/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8081, Lite's twin of the Darling <c>analyze_server</c> budget tests: a window with more findings than
/// <see cref="McpResponseBudget.DefaultBytes"/> holds answers under the budget with its most severe findings,
/// says it was cut (<c>truncated</c>, <c>findings_returned</c> beside <c>finding_count</c>), and the
/// <c>truncation_note</c> sends the reader to the store only when the run persisted — an anchored run saved
/// nothing, so its note says the cut findings were not persisted and names <c>hours_back</c> instead.
/// </summary>
public sealed class McpAnalyzeServerResponseBudgetTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private long _nextId = -8_081_000;
    private DuckDBConnection? _seedConn;

    /// <summary>The wait types <c>FactScorer</c> grades, each one its own fact and its own story root.</summary>
    private static readonly string[] ScoredWaitTypes =
    [
        "CXPACKET", "SOS_SCHEDULER_YIELD", "THREADPOOL", "PAGEIOLATCH_SH", "PAGEIOLATCH_EX",
        "RESOURCE_SEMAPHORE", "RESOURCE_SEMAPHORE_QUERY_COMPILE", "WRITELOG", "PAGELATCH_UP",
    ];

    public McpAnalyzeServerResponseBudgetTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "McpAnalyzeBudgetTests_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        /* Windows auth so AddServer never touches the credential store. */
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = "TestServer", DisplayName = "TestServer" };
        _serverManager.AddServer(server);
        /* The id the tool resolves to, so the rows below are the rows it reads. */
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
    public async Task AWindowOfManyFindings_FitsTheBudget_MostSevereFirst_AndSaysItWasCut()
    {
        await SeedManyFindingsAsync();

        var json = await McpAnalysisTools.AnalyzeServer(CreateTestService(), _serverManager);
        Assert.StartsWith("{", json, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("findings", root.GetProperty("status").GetString());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        var findingCount = root.GetProperty("finding_count").GetInt32();
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut reply says truncated: true");
        Assert.True(root.TryGetProperty("findings_returned", out var returned), "findings_returned rides beside finding_count");
        var findings = root.GetProperty("findings");
        Assert.Equal(returned.GetInt32(), findings.GetArrayLength());
        Assert.InRange(returned.GetInt32(), 1, findingCount - 1);
        Assert.True(root.GetProperty("persisted").GetBoolean());
        Assert.Contains("get_analysis_findings", root.GetProperty("truncation_note").GetString()!, StringComparison.Ordinal);

        /* The most severe are kept: the returned list is non-increasing in severity. */
        var severities = findings.EnumerateArray().Select(f => f.GetProperty("severity").GetDouble()).ToList();
        for (var i = 1; i < severities.Count; i++)
            Assert.True(severities[i] <= severities[i - 1], $"finding {i} ({severities[i]}) is more severe than finding {i - 1} ({severities[i - 1]}); the cut must keep the most severe");
    }

    [Fact]
    public async Task AnAnchoredWindowOfManyFindings_SaysTheCutFindingsWereNotSaved()
    {
        var windowEnd = await SeedManyFindingsAsync();

        var json = await McpAnalysisTools.AnalyzeServer(CreateTestService(), _serverManager, null, 4, windowEnd.ToString("o"));
        Assert.StartsWith("{", json, StringComparison.Ordinal);
        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;

        Assert.Equal("findings", root.GetProperty("status").GetString());
        Assert.False(root.GetProperty("persisted").GetBoolean());
        Assert.True(Encoding.UTF8.GetByteCount(json) <= McpResponseBudget.DefaultBytes,
            $"the reply is {Encoding.UTF8.GetByteCount(json):#,0} bytes, over the {McpResponseBudget.DefaultBytes:#,0}-byte budget");
        Assert.True(root.TryGetProperty("truncated", out var truncated) && truncated.GetBoolean(), "a cut reply says truncated: true");
        var note = root.GetProperty("truncation_note").GetString()!;
        Assert.DoesNotContain("get_analysis_findings", note, StringComparison.Ordinal);
        Assert.Contains("not persisted", note, StringComparison.Ordinal);
        Assert.Contains("narrow hours_back", note, StringComparison.Ordinal);
    }

    private AnalysisService CreateTestService() => new(_duckDb) { MinimumDataHours = 0 };

    /// <summary>Every scored wait type saturated for the four hours ending a minute ago, and six queries whose
    /// plan regressed inside that window (the PLAN_REGRESSION finding carries five force-plan targets, the
    /// finding's cap). Returns the window's end, in UTC.</summary>
    private async Task<DateTime> SeedManyFindingsAsync()
    {
        var now = DateTime.UtcNow;
        var windowEnd = new DateTime(now.Ticks - (now.Ticks % TimeSpan.TicksPerMinute), DateTimeKind.Utc).AddMinutes(-1);
        var windowStart = windowEnd.AddHours(-4);
        foreach (var waitType in ScoredWaitTypes)
        {
            await PlantWaitAsync(windowStart, waitType, 0L);
            for (var i = 1; i <= 16; i++)
                await PlantWaitAsync(windowStart.AddMinutes(15 * i), waitType, 900_000L);
        }
        for (long q = 1; q <= 6; q++)
        {
            await PlantQueryStorePlanAsync(windowEnd, q, planId: (q * 10) + 1, "0xCHEAP" + q, cpuUs: 100_000,
                firstExec: windowEnd.AddDays(-6), lastExec: windowEnd.AddDays(-5));
            await PlantQueryStorePlanAsync(windowEnd, q, planId: (q * 10) + 2, "0xCOSTLY" + q, cpuUs: (long)(100_000 * (2 + (q / 2.0))),
                firstExec: windowEnd.AddDays(-1), lastExec: windowEnd);
        }

        return windowEnd;
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
VALUES ($1, $2, $3, 'TestServer', $4, $5, $6, 0, $5, $6, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(at);
        P(_serverId);
        P(waitType);
        P(deltaWaitMs > 0 ? 10L : 0L);
        P(deltaWaitMs);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task PlantQueryStorePlanAsync(DateTime collectedAt, long queryId, long planId, string planHash, long cpuUs, DateTime firstExec, DateTime lastExec)
    {
        using var readLock = _duckDb.AcquireReadLock();
        var conn = await SeedConnectionAsync();
        using var cmd = conn.CreateCommand();
        cmd.CommandText = @"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     query_plan_hash, is_forced_plan, force_failure_count)
VALUES ($1, $2, $3, 'TestServer', 'AppDb', $4, $5, 'Regular', $6, $7, $8, $9, 100, $10, $11, $12, false, 0)";
        void P(object v) => cmd.Parameters.Add(new DuckDBParameter { Value = v });
        P(_nextId--);
        P(collectedAt);
        P(_serverId);
        P(queryId);
        P(planId);
        P(firstExec);
        P(lastExec);
        P("SELECT * FROM dbo.Orders WHERE CustomerId = @id" + queryId.ToString(CultureInfo.InvariantCulture));
        P("0xQH" + queryId.ToString(CultureInfo.InvariantCulture));
        P(cpuUs);
        P(cpuUs + 20_000);
        P(planHash);
        await cmd.ExecuteNonQueryAsync();
    }
}
