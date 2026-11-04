/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Helpers;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7256: the slow-read line separates the lock wait from the query time. The test turns the process-wide
/// <see cref="QueryLogger"/> on with a threshold of 0, so every timed read in the process writes into its log directory
/// for as long as it runs, and it takes the first window-floor line in that file as its own read's. A window-floor probe
/// from any other test class running at the same time writes an earlier line with a lock wait of 0 ms, and the test then
/// fails on a line that is not its read's. So it runs in a collection of its own, which xunit runs alone after the
/// parallel classes have finished.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class QueryLoggerStateCollection
{
    public const string Name = "query-logger-state";
}

[Collection(QueryLoggerStateCollection.Name)]
public sealed class SlowReadLogLineTests : IDisposable
{
    private const int ServerId = 7256;
    private static readonly DateTime NowUtc = DateTime.UtcNow;

    private readonly string _tempDir;
    private readonly DuckDbInitializer _duckDb;
    private readonly LocalDataService _service;

    public SlowReadLogLineTests()
    {
        _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(_tempDir);
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
        _service = new LocalDataService(_duckDb);
    }

    public void Dispose()
    {
        _duckDb.Dispose();
        try
        {
            if (Directory.Exists(_tempDir))
                Directory.Delete(_tempDir, recursive: true);
        }
        catch
        {
            /* Best-effort cleanup */
        }
    }

    [Fact]
    public async Task ASlowReadsLogLine_SeparatesTheLockWaitFromTheQueryTime()
    {
        var logDirectory = Path.Combine(_tempDir, "logs");
        var oldThreshold = QueryLogger.ThresholdSeconds;
        var oldEnabled = QueryLogger.IsEnabled;
        QueryLogger.Initialize(logDirectory);
        QueryLogger.SetEnabled(true);
        QueryLogger.SetThreshold(0);
        try
        {
            /* A writer holds the lock for 400 ms with the read queued behind it. The read's own work is a few milliseconds. */
            var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            using var release = new ManualResetEventSlim();
            var holder = Task.Factory.StartNew(() =>
            {
                using var writeLock = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
                signal.SetResult();
                release.Wait(TimeSpan.FromMinutes(2));
            }, TaskCreationOptions.LongRunning);
            await signal.Task.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);

            var read = Task.Run(() => _service.GetQueryWindowFloorAsync(
                QueryWindowRelation.QueryStoreStats, ServerId, NowUtc.AddHours(-1), NowUtc), TestContext.Current.CancellationToken);
            await Task.Delay(400, TestContext.Current.CancellationToken);
            Assert.False(read.IsCompleted, "the read waits behind the writer");
            release.Set();
            await holder;
            await read.WaitAsync(TimeSpan.FromSeconds(30), TestContext.Current.CancellationToken);
        }
        finally
        {
            QueryLogger.SetThreshold(oldThreshold);
            QueryLogger.SetEnabled(oldEnabled);
        }

        var log = File.ReadAllText(QueryLogger.GetCurrentLogFile());
        var match = Regex.Match(log, @"Context:\s+GetQueryWindowFloorAsync \(lock wait (\d+) ms, open (\d+) ms, query and row read (\d+) ms\)");
        Assert.True(match.Success, "the slow-read line carries the three phases: " + log[..Math.Min(log.Length, 600)]);
        var lockWait = int.Parse(match.Groups[1].Value);
        var queryAndRows = int.Parse(match.Groups[3].Value);
        Assert.True(lockWait >= 300, $"the queued read's wait is reported as lock wait ({lockWait} ms)");
        Assert.True(queryAndRows < lockWait, $"the wait is not counted as query time ({queryAndRows} ms query, {lockWait} ms lock wait)");
    }
}
