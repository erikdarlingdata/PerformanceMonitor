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
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Analysis.ObjectReferences;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9885: Lite's <c>get_object_usage</c> says which read answered the table window (the persisted chain or the full fold, with the fold's
/// reason), in the reply's coverage and in the slow-call row, and the row carries the call's phases. The statement parse is kept per text
/// between calls, so a second first call for another window parses nothing and answers the same as a call that parsed everything.
/// </summary>
public sealed class ObjectUsageAnsweredFromTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectUsageAnsweredSrv";
    private const string Database = "Sales";
    private static readonly DateTime Now = ObjectUsageChainHistory.Now;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private long _nextId = -50_000_000;

    public ObjectUsageAnsweredFromTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-answeredfrom-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_configDir);
        _serverManager = new ServerManager(_configDir);
        var server = new ServerConnection { Id = Guid.NewGuid().ToString(), ServerName = ServerName, IsEnabled = true };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        try { Directory.Delete(_configDir, recursive: true); } catch (IOException) { /* temp dir */ }
    }

    private async Task SeedChainAsync()
    {
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate());
        var built = await new ObjectUsageChainBuilder(_duckDb, () => Now).BuildPairAsync(_serverId, Database, int.MaxValue, CancellationToken.None);
        Assert.True(built > 0);
    }

    private async Task ExecAsync(string sql)
    {
        using var writeLock = _duckDb.AcquireWriteLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = sql;
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    private async Task InsertStatementAsync(DateTime collected, long queryId, long intervalId, long executions, DateTime intervalStart, string text)
    {
        using var readLock = _duckDb.AcquireReadLock();
        using var connection = _duckDb.CreateConnection();
        await connection.OpenAsync(TestContext.Current.CancellationToken);
        using var cmd = connection.CreateCommand();
        cmd.CommandText = @"INSERT INTO query_store_stats
            (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, module_name,
             execution_count, last_execution_time, runtime_stats_interval_id, interval_start_time_utc, first_execution_time, query_text)
            VALUES ($1,$2,$3,$4,$5,$6,1,'Regular','Adhoc',$7,$8,$9,$10,$10,$11)";
        void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
        P(_nextId--); P(collected); P(_serverId); P(ServerName); P(Database); P(queryId); P(executions); P(collected); P(intervalId); P(intervalStart); P(text);
        await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
    }

    /// <summary>One call inside the call timing scope the host's filter opens, with every call counted slow, so its row is written.</summary>
    private async Task<(JsonElement Reply, McpSlowCall Row)> CallWithRowAsync(int daysBack)
    {
        var recorder = new McpCallTimingRecorder(_dataService) { SlowThreshold = TimeSpan.Zero };
        string text;
        using (var call = recorder.Begin("get_object_usage", null))
        {
            text = await McpObjectUsageTools.GetObjectUsage(_dataService, _serverManager, ServerName, Database, daysBack, 500, 0, Now);
            call.Finish(McpCallTimingRecorder.OutcomeOk, new CallToolResult { Content = [new TextContentBlock { Text = text }] });
        }

        await recorder.WhenWritesDoneAsync();
        var rows = await _dataService.GetMcpSlowCallsAsync(DateTime.UtcNow.AddMinutes(-10), 20, "get_object_usage");
        return (JsonDocument.Parse(text).RootElement, rows.First());
    }

    private static string? AnsweredFrom(JsonElement reply) =>
        reply.GetProperty("coverage")[0].GetProperty("tables").GetProperty("answered_from").GetString();

    private static void AssertPhasesAreKept(McpSlowCall row)
    {
        Assert.NotNull(row.HotMs);
        Assert.NotNull(row.ArchiveMs);
        var phases = Assert.IsType<string>(row.PhaseSplit).Split(", ");
        Assert.True(phases.Length >= 4, $"phase_split was '{row.PhaseSplit}'");
        Assert.All(phases, p => Assert.EndsWith(" ms", p, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(7)]
    [InlineData(90)]
    public async Task AChainCoveringTheWindow_SaysChain_InTheReplyAndTheSlowCallRow(int daysBack)
    {
        await SeedChainAsync();

        var (reply, row) = await CallWithRowAsync(daysBack);

        Assert.Equal("chain", AnsweredFrom(reply));
        Assert.Equal("chain", row.AnsweredFrom);
        Assert.Equal("get_object_usage", row.Tool);
        AssertPhasesAreKept(row);
    }

    [Fact]
    public async Task ARemovedChain_SaysFoldWithItsReason_AndAnswersTheSameObjects()
    {
        await SeedChainAsync();
        var (chainReply, _) = await CallWithRowAsync(90);

        await ExecAsync("DELETE FROM object_usage_deltas");
        await ExecAsync("DELETE FROM object_usage_runs");
        await ExecAsync("DELETE FROM object_usage_snapshots");
        var (foldReply, row) = await CallWithRowAsync(90);

        var answered = AnsweredFrom(foldReply);
        Assert.StartsWith("fold: ", answered, StringComparison.Ordinal);
        Assert.True(answered!.Length > "fold: ".Length, "the fold names no reason");
        Assert.Equal(answered, row.AnsweredFrom);
        AssertPhasesAreKept(row);
        Assert.Equal(chainReply.GetProperty("objects").GetRawText(), foldReply.GetProperty("objects").GetRawText());
    }

    [Fact]
    public async Task ASecondFirstCallForAnotherWindow_ParsesNoStatementAgain_AndAnswersTheSameAsACallThatParsedThemAll()
    {
        await ObjectUsageChainHistory.SeedAsync(_duckDb, _serverId, ServerName, Database, ObjectUsageChainHistory.Generate());
        var tag = Guid.NewGuid().ToString("N");
        var interval = DateTime.SpecifyKind(Now.AddDays(-3), DateTimeKind.Unspecified);
        for (var i = 0; i < 3; i++)
        {
            await InsertStatementAsync(interval.AddMinutes(10 * (i + 1)), 700 + i, 801, 4 + i, interval, $"SELECT * FROM dbo.vKept{i}_{tag} WHERE a = 1");
        }

        var kept = new QueryTextScanCache();
        async Task<JsonElement> CallAsync(int daysBack, QueryTextScanCache scans) =>
            JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(
                _dataService, _serverManager, ServerName, Database, daysBack, 500, 0, Now, cache: null, scanCache: scans)).RootElement;

        var first = await CallAsync(60, kept);
        Assert.Equal(3, kept.Scans);

        var second = await CallAsync(30, kept);
        Assert.Equal(3, kept.Scans);

        var fresh = new QueryTextScanCache();
        var withoutTheKeep = await CallAsync(30, fresh);
        Assert.Equal(3, fresh.Scans);

        Assert.Equal(withoutTheKeep.GetProperty("objects").GetRawText(), second.GetProperty("objects").GetRawText());
        Assert.Equal(
            withoutTheKeep.GetProperty("coverage")[0].GetProperty("views").GetRawText(),
            second.GetProperty("coverage")[0].GetProperty("views").GetRawText());
        Assert.Equal(3, first.GetProperty("objects").EnumerateArray().Count(o => o.GetProperty("object_name").GetString()!.StartsWith("vKept", StringComparison.Ordinal)));
    }
}
