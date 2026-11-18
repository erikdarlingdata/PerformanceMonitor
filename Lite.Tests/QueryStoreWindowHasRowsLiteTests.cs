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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #8570, Lite half: <see cref="LocalDataService.QueryStoreWindowHasRowsAsync"/> answers whether the window holds a group the
/// Query Store top read would show, and get_query_store_top asks it before calling an empty filtered answer a measured zero. The
/// bounded fill of the top read ranks at most three rounds (6, 24 and 96 candidates at top 1), so a window whose duration ranking
/// puts a hundred WAITFOR groups above its only ordinary one gives an empty top-1 page; the probe still finds that group, answers
/// false when every group is a WAITFOR shell or the window has none, and keeps the database filter.
/// </summary>
public sealed class QueryStoreWindowHasRowsLiteTests : IClassFixture<SharedDuckDbFixture>, IDisposable
{
    private const string ServerName = "QsProbeServer";
    private const int Shells = 100;
    private static readonly DateTime Collected = DateTime.SpecifyKind(DateTime.UtcNow.AddMinutes(-90), DateTimeKind.Unspecified);

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _dataService;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;
    private DuckDBConnection? _seedConn;
    private long _nextId = 1;

    public QueryStoreWindowHasRowsLiteTests(SharedDuckDbFixture fixture)
    {
        fixture.ResetData();
        _duckDb = fixture.DuckDb;
        _tempDir = Path.Combine(Path.GetTempPath(), "QsProbe_" + Guid.NewGuid().ToString("N")[..8]);
        var configDir = Path.Combine(_tempDir, "config");
        Directory.CreateDirectory(configDir);
        _dataService = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(configDir);
        var server = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(server);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(server));
    }

    public void Dispose()
    {
        _seedConn?.Dispose();
        try { if (Directory.Exists(_tempDir)) Directory.Delete(_tempDir, recursive: true); }
        catch (IOException) { /* best-effort cleanup */ }
        catch (UnauthorizedAccessException) { /* best-effort cleanup */ }
    }

    [Fact]
    public async Task AHundredShellsAboveTheOnlyOrdinaryGroup_LeaveTheTopOnePageEmpty_ButTheProbeFindsTheGroup()
    {
        await SeedShellsAboveAsync("TestDb", ordinary: true);

        Assert.Empty(await _dataService.GetQueryStoreTopQueriesAsync(_serverId, hoursBack: 24, top: 1));
        Assert.True(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));
    }

    [Fact]
    public async Task OneOrdinaryGroup_TheProbeAnswersTrue()
    {
        await SeedStoreKeyAsync("TestDb", 1, 1000, "SELECT 1 FROM dbo.t1", Collected);

        Assert.True(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));
    }

    [Fact]
    public async Task OnlyShellsInTheWindow_TheProbeAnswersFalse()
    {
        await SeedShellsAboveAsync("TestDb", ordinary: false);

        Assert.False(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));
    }

    [Fact]
    public async Task AWindowWithNoRows_TheProbeAnswersFalse_AndAWiderOneReachesTheRow()
    {
        Assert.False(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));

        var twoDaysAgo = DateTime.SpecifyKind(DateTime.UtcNow.AddHours(-48), DateTimeKind.Unspecified);
        await SeedStoreKeyAsync("TestDb", 1, 1000, "SELECT 1 FROM dbo.t1", twoDaysAgo);

        Assert.False(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));
        Assert.True(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 72));
    }

    [Fact]
    public async Task TheDatabaseFilter_ScopesTheProbe()
    {
        await SeedShellsAboveAsync("ShowDb", ordinary: true);
        await SeedShellsAboveAsync("ShellDb", ordinary: false);

        Assert.True(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24, new[] { "ShowDb" }));
        Assert.False(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24, new[] { "ShellDb" }));
        Assert.True(await _dataService.QueryStoreWindowHasRowsAsync(_serverId, 24));
    }

    [Fact]
    public async Task AnExecutionTypeThatMatchesNothing_UnderAHundredShells_IsAMeasuredZero()
    {
        await SeedShellsAboveAsync("TestDb", ordinary: true);

        var answer = JsonDocument.Parse(await McpQueryTools.GetQueryStoreTop(_dataService, _serverManager, execution_type: "Aborted")).RootElement;

        Assert.Equal("empty", answer.GetProperty("status").GetString());
        var message = answer.GetProperty("message").GetString()!;
        Assert.Contains("No Aborted executions", message, StringComparison.Ordinal);
        Assert.Contains("measured zero", message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task RunUntilExhausted_GrowsPastTheRoundBound_AndStopsWhenTheCandidatesRunOut()
    {
        /* 1000 ranked candidates, the only showable one at rank 500: the bounded fill stops after its three rounds with an empty
           page, the unbounded one grows until a round ranks past it. */
        var bounded = new List<int>();
        Assert.Empty(await TopFill.RunAsync(1, Round(available: 1000, showableAt: 500, bounded)));
        Assert.Equal(new[] { 6, 24, 96 }, bounded);

        var exhausted = new List<int>();
        Assert.Equal(new[] { 500 }, await TopFill.RunUntilExhaustedAsync(1, Round(available: 1000, showableAt: 500, exhausted)));
        Assert.Equal(new[] { 6, 24, 96, 384, 1536 }, exhausted);

        /* No showable candidate: the round that ranks fewer candidates than its limit has seen them all. */
        var none = new List<int>();
        Assert.Empty(await TopFill.RunUntilExhaustedAsync(1, Round(available: 1000, showableAt: null, none)));
        Assert.Equal(new[] { 6, 24, 96, 384, 1536 }, none);

        /* A showable candidate in round one costs one round. */
        var first = new List<int>();
        Assert.Equal(new[] { 0 }, await TopFill.RunUntilExhaustedAsync(1, Round(available: 1000, showableAt: 0, first)));
        Assert.Equal(new[] { 6 }, first);
    }

    /// <summary>
    /// A round over <paramref name="available"/> ranked candidates where only the one at <paramref name="showableAt"/> survives the
    /// trim; it records each candidate limit it is asked for.
    /// </summary>
    private static Func<int, Task<(List<int> Rows, int CandidateCount)>> Round(int available, int? showableAt, List<int> limits)
        => candidates =>
        {
            limits.Add(candidates);
            var ranked = Math.Min(candidates, available);
            var rows = showableAt is int rank && rank < ranked ? new List<int> { rank } : new List<int>();
            return Task.FromResult((rows, ranked));
        };

    /// <summary>
    /// <see cref="Shells"/> WAITFOR groups in <paramref name="database"/>, ranked by duration above one ordinary group when
    /// <paramref name="ordinary"/> is set.
    /// </summary>
    private async Task SeedShellsAboveAsync(string database, bool ordinary)
    {
        var total = Shells + 1;
        for (var i = 0; i < Shells; i++)
        {
            await SeedStoreKeyAsync(database, 1000 + i, (total - i) * 1000L, $"WAITFOR DELAY '00:00:{i % 60:00}'", Collected);
        }

        if (ordinary)
        {
            await SeedStoreKeyAsync(database, 5000, 1000L, "SELECT 1 FROM dbo.t1", Collected);
        }
    }

    /// <summary>One query_store_stats group whose duration (the ranking) is <paramref name="durationUs"/>.</summary>
    private Task SeedStoreKeyAsync(string database, long queryId, long durationUs, string text, DateTime collected) => ExecAsync(@"
INSERT INTO query_store_stats
    (collection_id, collection_time, server_id, server_name, database_name,
     query_id, plan_id, execution_type_desc, first_execution_time, last_execution_time,
     query_text, query_hash, execution_count, avg_cpu_time_us, avg_duration_us,
     avg_logical_io_reads, avg_logical_io_writes, avg_physical_io_reads,
     runtime_stats_interval_id, interval_start_time_utc)
VALUES ($1, $2, $3, $4, $5, $6, $6, 'Regular', $2, $2, $7, $8, 10, 1000, $9, 100, 0, 0, 1, $2)",
        _nextId++, collected, _serverId, ServerName, database, queryId, text, "0xQ" + queryId, durationUs);

    private async Task ExecAsync(string sql, params object?[] values)
    {
        using var readLock = _duckDb.AcquireReadLock();
        if (_seedConn is null)
        {
            _seedConn = _duckDb.CreateConnection();
            await _seedConn.OpenAsync();
        }

        using var cmd = _seedConn.CreateCommand();
        cmd.CommandText = sql;
        foreach (var value in values)
        {
            cmd.Parameters.Add(new DuckDBParameter { Value = value });
        }

        await cmd.ExecuteNonQueryAsync();
    }
}
