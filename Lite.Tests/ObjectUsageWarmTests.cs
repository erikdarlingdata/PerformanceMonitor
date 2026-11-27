/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Analysis.ObjectReferences;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9885: the object usage parse cache is warmed once per start, on the housekeeping loop after the chain's first pass and only while the MCP server is
/// on, so the first <c>get_object_usage</c> call after a start parses no statement text. The warm stops at its time budget and at shutdown and leaves
/// the cache usable. The warm is driven through the loop's own steps, in the loop's order, with a parse cache the test hands in and counts.
/// </summary>
public sealed class ObjectUsageWarmTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "ObjectUsageWarmSrv";
    private const string Database = "Sales";
    private const int Statements = 3;

    private readonly int _serverId;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly string _configDir;
    private readonly ServerManager _serverManager;
    private readonly DateTime _interval = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-3), DateTimeKind.Unspecified);
    private readonly string _tag = Guid.NewGuid().ToString("N");
    private long _nextId = -60_000_000;

    public ObjectUsageWarmTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _dataService = new LocalDataService(_duckDb);
        _configDir = Path.Combine(Path.GetTempPath(), "pmlite-usagewarm-" + Guid.NewGuid().ToString("N"));
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

    private async Task InsertStatementsAsync(int count, int firstQueryId)
    {
        for (var i = 0; i < count; i++)
        {
            var collected = _interval.AddMinutes(10 * (i + 1));
            using var readLock = _duckDb.AcquireReadLock();
            using var connection = _duckDb.CreateConnection();
            await connection.OpenAsync(TestContext.Current.CancellationToken);
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"INSERT INTO query_store_stats
                (collection_id, collection_time, server_id, server_name, database_name, query_id, plan_id, execution_type_desc, module_name,
                 execution_count, last_execution_time, runtime_stats_interval_id, interval_start_time_utc, first_execution_time, query_text)
                VALUES ($1,$2,$3,$4,$5,$6,1,'Regular','Adhoc',$7,$8,$9,$10,$10,$11)";
            void P(object? v) => cmd.Parameters.Add(new DuckDBParameter { Value = v ?? DBNull.Value });
            P(_nextId--); P(collected); P(_serverId); P(ServerName); P(Database); P(firstQueryId + i); P(4 + i); P(collected); P(801); P(_interval);
            P($"SELECT * FROM dbo.vWarm{firstQueryId + i}_{_tag} WHERE a = 1");
            await cmd.ExecuteNonQueryAsync(TestContext.Current.CancellationToken);
        }
    }

    private async Task<JsonElement> CallAsync(QueryTextScanCache scans) =>
        JsonDocument.Parse(await McpObjectUsageTools.GetObjectUsage(
            _dataService, _serverManager, ServerName, Database, 90, 500, 0, DateTime.UtcNow, cache: null, scanCache: scans)).RootElement;

    private static List<string> WarmViews(JsonElement reply) =>
        reply.GetProperty("objects").EnumerateArray()
            .Select(o => o.GetProperty("object_name").GetString()!)
            .Where(n => n.StartsWith("vWarm", StringComparison.Ordinal))
            .ToList();

    private (CollectionBackgroundService Service, CapturedLog Log) NewService(QueryTextScanCache cache, bool mcpOn)
    {
        var log = new CapturedLog();
        var service = new CollectionBackgroundService(null!, _duckDb, serverManager: _serverManager, logger: log)
        {
            ObjectUsageWarmCache = cache,
            ObjectUsageWarmEnabled = () => mcpOn,
        };
        return (service, log);
    }

    private static (CollectionBackgroundService.HousekeepingStep Chain, CollectionBackgroundService.HousekeepingStep Warm) ChainLoopSteps(CollectionBackgroundService service)
    {
        var loop = service.BuildHousekeepingLoops().Single(l => l.Name == "object_usage_chain");
        return (loop.Steps[0], loop.Steps[1]);
    }

    [Fact]
    public async Task AfterTheLoopsFirstTick_TheFirstCall_ParsesNoNewText_AndAnswersTheSameAsAColdCall()
    {
        await InsertStatementsAsync(Statements, 700);
        var kept = new QueryTextScanCache();
        var (service, log) = NewService(kept, mcpOn: true);
        var (chain, warm) = ChainLoopSteps(service);

        await chain.Run(TestContext.Current.CancellationToken);
        await warm.Run(TestContext.Current.CancellationToken);
        Assert.Equal(Statements, kept.Scans);
        Assert.Equal(1, log.Count(LogLevel.Information));

        var first = await CallAsync(kept);
        Assert.Equal(Statements, kept.Scans);

        var cold = new QueryTextScanCache();
        var withoutTheWarm = await CallAsync(cold);
        Assert.Equal(Statements, cold.Scans);
        Assert.Equal(Statements, WarmViews(first).Count);
        Assert.Equal(withoutTheWarm.GetProperty("objects").GetRawText(), first.GetProperty("objects").GetRawText());
    }

    [Fact]
    public async Task TheWarm_WaitsForTheChainsFirstPass_RunsOnlyWithTheMcpServerOn_AndOnlyOncePerStart()
    {
        await InsertStatementsAsync(Statements, 700);
        var kept = new QueryTextScanCache();

        var (beforeChain, beforeChainLog) = NewService(kept, mcpOn: true);
        await ChainLoopSteps(beforeChain).Warm.Run(TestContext.Current.CancellationToken);
        Assert.Equal(0, kept.Scans);
        Assert.Equal(0, beforeChainLog.Count(LogLevel.Information));

        var mcpOn = false;
        var (service, log) = NewService(kept, mcpOn: false);
        service.ObjectUsageWarmEnabled = () => mcpOn;
        var (chain, warm) = ChainLoopSteps(service);
        await chain.Run(TestContext.Current.CancellationToken);
        await warm.Run(TestContext.Current.CancellationToken);
        Assert.Equal(0, kept.Scans);
        Assert.Equal(0, log.Count(LogLevel.Information));

        mcpOn = true;
        await warm.Run(TestContext.Current.CancellationToken);
        Assert.Equal(Statements, kept.Scans);
        Assert.Equal(1, log.Count(LogLevel.Information));

        await InsertStatementsAsync(1, 900);
        await warm.Run(TestContext.Current.CancellationToken);
        Assert.Equal(Statements, kept.Scans);
        Assert.Equal(1, log.Count(LogLevel.Information));
    }

    [Fact]
    public async Task AWarmThatHitsItsBudget_StopsParsingEarly_AndLeavesTheCacheForTheFirstReader()
    {
        await InsertStatementsAsync(Statements, 700);
        var kept = new QueryTextScanCache();

        var warm = await _dataService.WarmObjectUsageParseCacheAsync(
            _serverId, DateTime.UtcNow.AddDays(-McpObjectUsageTools.MaxDaysBack), TimeSpan.Zero, kept, TestContext.Current.CancellationToken);

        Assert.True(warm.StoppedByBudget);
        Assert.Equal(Statements, warm.Texts);
        Assert.Equal(0, warm.Parsed);
        Assert.Equal(0, kept.Scans);

        var reply = await CallAsync(kept);
        Assert.Equal(Statements, kept.Scans);
        Assert.Equal(Statements, WarmViews(reply).Count);
    }

    [Fact]
    public async Task AWarmCancelledAtShutdown_Throws_AndParsesNothing()
    {
        await InsertStatementsAsync(Statements, 700);
        var kept = new QueryTextScanCache();
        using var stop = new CancellationTokenSource();
        await stop.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => _dataService.WarmObjectUsageParseCacheAsync(
            _serverId, DateTime.UtcNow.AddDays(-McpObjectUsageTools.MaxDaysBack), TimeSpan.FromMinutes(5), kept, stop.Token));

        Assert.Equal(0, kept.Scans);
    }

    /// <summary>The service's log lines, kept by level.</summary>
    private sealed class CapturedLog : ILogger<CollectionBackgroundService>
    {
        private readonly List<LogLevel> _levels = [];

        public int Count(LogLevel level)
        {
            lock (_levels)
            {
                return _levels.Count(l => l == level);
            }
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            lock (_levels)
            {
                _levels.Add(logLevel);
            }
        }
    }
}
