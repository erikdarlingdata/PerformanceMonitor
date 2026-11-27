/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Models;
using PerformanceMonitorLite.Services;
using PerformanceMonitorLite.Tests;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #9195: a slow get_query_store_top call and a slow get_analysis_facts call each log one line that says how long the call waited
/// for the store's read lock and over how many connections. The lock is held by a writer until the call is seen waiting for it,
/// and the writer then lets go only after a lower bound of time has passed, so the wait the line reports does not depend on how fast
/// the machine is. The slow gate is a threshold the test sets, not a clock the test beats.
///
/// <para>Collection <c>app-logger-statics</c>: the assertions drain the process-wide log buffer, and the lock is one static for the
/// whole process, so nothing else may read the store while a writer holds it.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class ReadLockWaitLogTests : IDisposable
{
    private const string ServerName = "ReadLockWaitLogServer";

    /// <summary>The wait the writer guarantees: it lets go only once this much time has passed since the call was seen waiting.</summary>
    private static readonly TimeSpan HeldAtLeast = TimeSpan.FromMilliseconds(150);

    private readonly string _dir = Path.Combine(Path.GetTempPath(), "LiteTests_9195w_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;
    private readonly ServerManager _serverManager;
    private readonly int _serverId;

    public ReadLockWaitLogTests()
    {
        Directory.CreateDirectory(_dir);
        _duckDb = new DuckDbInitializer(Path.Combine(_dir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
        _serverManager = new ServerManager(_dir);
        var connection = new ServerConnection { ServerName = ServerName, DisplayName = ServerName };
        _serverManager.AddServer(connection);
        _serverId = RemoteCollectorService.GetDeterministicHashCode(RemoteCollectorService.GetServerNameForStorage(connection));
    }

    public void Dispose()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.FromSeconds(2);
        _duckDb.Dispose();
        try { Directory.Delete(_dir, recursive: true); } catch { /* best effort */ }
    }

    private Task<string> QueryStoreTop() => McpQueryTools.GetQueryStoreTop(_service, _serverManager, ServerName);

    private Task FactsRead() =>
        new AnalysisService(_duckDb) { MinimumDataHours = 0 }.CollectAndScoreFactsAsync(_serverId, ServerName);

    /// <summary>
    /// Runs <paramref name="call"/> while a writer holds the store's lock, and lets go once the call is seen waiting and
    /// <see cref="HeldAtLeast"/> has passed since.
    /// </summary>
    private async Task RunBehindAWriterAsync(Func<Task> call)
    {
        var writerHasIt = new ManualResetEventSlim(false);
        var releaseWriter = new ManualResetEventSlim(false);
        var writer = Task.Run(() =>
        {
            using var write = _duckDb.AcquireWriteLock(TimeSpan.FromSeconds(60));
            writerHasIt.Set();
            releaseWriter.Wait(TimeSpan.FromSeconds(60));
        });

        try
        {
            Assert.True(writerHasIt.Wait(TimeSpan.FromSeconds(60)), "the writer never took the lock");
            var running = Task.Run(call);
            Assert.True(
                SpinWait.SpinUntil(() => DuckDbInitializer.WaitingReadCountForTests >= 1, TimeSpan.FromSeconds(60)),
                "the call never waited for the lock");
            var seenWaiting = Stopwatch.StartNew();
            SpinWait.SpinUntil(() => seenWaiting.Elapsed >= HeldAtLeast);
            releaseWriter.Set();
            await running.WaitAsync(TimeSpan.FromSeconds(120));
        }
        finally
        {
            releaseWriter.Set();
            await writer.WaitAsync(TimeSpan.FromSeconds(60));
        }
    }

    private static List<string> Lines(string startsWith) =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains(startsWith, StringComparison.Ordinal) && l.Contains(ServerName, StringComparison.Ordinal))
            .ToList();

    /// <summary>The connections one uncontended run of <paramref name="call"/> takes the read lock for, after a first run that fills the caches.</summary>
    private static async Task<int> ConnectionsAsync(Func<Task> call)
    {
        await call();
        using var tally = ReadLockWaitTally.Begin();
        await call();
        return tally.Connections;
    }

    private static (long WaitMs, int Connections) LockWait(string line)
    {
        var m = Regex.Match(line, @"read lock wait (\d+) ms over (\d+) connections");
        Assert.True(m.Success, line);
        return (long.Parse(m.Groups[1].Value), int.Parse(m.Groups[2].Value));
    }

    [Fact]
    public async Task SlowQueryStoreTopRead_BehindAWriter_LogsTheLockWaitAndTheConnectionCount()
    {
        var expected = await ConnectionsAsync(QueryStoreTop);
        Assert.True(expected >= 1);
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.Zero;
        AppLogger.DrainBufferedLines();

        await RunBehindAWriterAsync(QueryStoreTop);

        var line = Assert.Single(Lines("Query Store top read took"));
        var (waitMs, connections) = LockWait(line);
        Assert.True(waitMs >= HeldAtLeast.TotalMilliseconds, line);
        Assert.Equal(expected, connections);
        Assert.Matches(@"tag scope \d+ ms, top read \d+ ms, window floor \d+ ms, other \d+ ms", line);
    }

    [Fact]
    public async Task FastQueryStoreTopRead_LogsNothing()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.MaxValue;
        AppLogger.DrainBufferedLines();

        await QueryStoreTop();

        Assert.Empty(Lines("Query Store top read took"));
    }

    [Fact]
    public async Task SlowFactsRead_BehindAWriter_LogsTheLockWaitAndTheConnectionCount()
    {
        var expected = await ConnectionsAsync(FactsRead);
        Assert.True(expected >= 1);
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.Zero;
        AppLogger.DrainBufferedLines();

        await RunBehindAWriterAsync(FactsRead);

        var line = Assert.Single(Lines("Analysis facts read took"));
        var (waitMs, connections) = LockWait(line);
        Assert.True(waitMs >= HeldAtLeast.TotalMilliseconds, line);
        Assert.Equal(expected, connections);
    }

    [Fact]
    public async Task FastFactsRead_LogsNothing()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.MaxValue;
        AppLogger.DrainBufferedLines();

        await FactsRead();

        Assert.Empty(Lines("Analysis facts read took"));
    }
}
