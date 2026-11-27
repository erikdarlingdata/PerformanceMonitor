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
using System.Threading.Tasks;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis;
using PerformanceMonitorLite.Database;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9353, #9195: the plan regression fact pass reads two statements, and it takes the store's read lock once for each. A
/// housekeeping writer that asks for the lock while the pass runs gets in between the two statements instead of waiting out both.
/// The pass also logs one line, only when it runs slow, that says where its time went.
///
/// <para>The hold test pauses the pass through a seam after its first statement and takes the write lock right there, on the
/// pass's own thread (the lock is thread-affine). It needs no timing: either the lock is free at that point or it is not.</para>
///
/// <para>Collection <c>app-logger-statics</c>: the line tests drain the process-wide log buffer and set the process-wide slow
/// threshold.</para>
/// </summary>
[Collection("app-logger-statics")]
public sealed class PlanRegressionReadLockTests : IDisposable
{
    private const string ServerName = "PlanRegressionLockServer";

    private readonly string _tempDir = Path.Combine(Path.GetTempPath(), "LiteTests_9353_" + Guid.NewGuid().ToString("N")[..8]);
    private readonly DuckDbInitializer _duckDb;

    public PlanRegressionReadLockTests()
    {
        Directory.CreateDirectory(Path.Combine(_tempDir, "archive"));
        _duckDb = new DuckDbInitializer(Path.Combine(_tempDir, "test.duckdb"));
        _duckDb.InitializeFromTemplateAsync().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.FromSeconds(2);
        _duckDb.Dispose();
        try { Directory.Delete(_tempDir, recursive: true); } catch { /* best effort */ }
    }

    private static AnalysisContext Context()
    {
        var now = DateTime.UtcNow;
        return new AnalysisContext
        {
            ServerId = 9353,
            ServerName = ServerName,
            TimeRangeStart = now.AddHours(-4),
            TimeRangeEnd = now,
        };
    }

    private Task RunPassAsync(DuckDbFactCollector collector) =>
        collector.CollectPlanRegressionFactsAsync(Context(), new List<Fact>());

    private static List<string> PassLines() =>
        AppLogger.DrainBufferedLines()
            .Where(l => l.Contains("Plan regression read took", StringComparison.Ordinal) && l.Contains(ServerName, StringComparison.Ordinal))
            .ToList();

    [Fact]
    public async Task AWriterThatAsksBetweenTheTwoStatements_TakesTheLock()
    {
        var collector = new DuckDbFactCollector(_duckDb);
        var wroteBetween = false;
        Exception? refused = null;
        collector.AfterPlanRegressionFailuresReadForTests = () =>
        {
            try
            {
                /* A long wait, as ArchiveSpanReadTests uses (#9564, #9810): the write lock is one lock for the whole test process, so another
                   class's InitializeAsync on a busy runner can hold it for more than 5 s. A read lock kept across batches still fails
                   this test: the same thread throws LockRecursionException, and another thread waits out the whole timeout. */
                using var write = _duckDb.AcquireWriteLock(TimeSpan.FromMinutes(2));
                wroteBetween = true;
            }
            catch (Exception ex)
            {
                refused = ex;
            }
        };

        await RunPassAsync(collector);

        Assert.True(wroteBetween, "the pass still held the read lock between its two statements: " + refused?.Message);
    }

    [Fact]
    public async Task ASlowPass_LogsWhereItsTimeWent()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.Zero;
        AppLogger.DrainBufferedLines();

        await RunPassAsync(new DuckDbFactCollector(_duckDb));

        var line = Assert.Single(PassLines());
        Assert.Matches(
            @"took \d+ ms for PlanRegressionLockServer \(server 9353\): archive read plan \d+ ms, " +
            @"failures read \d+ ms for \d+ rows \(read lock wait \d+ ms\), " +
            @"cost read \d+ ms for \d+ candidates \(read lock wait \d+ ms\), " +
            @"longest single read lock hold \d+ ms, plan check \d+ ms with \d+ plan fetches",
            line);
    }

    [Fact]
    public async Task AFastPass_LogsNothing()
    {
        ReadLockWaitTally.SlowReadThreshold = TimeSpan.MaxValue;
        AppLogger.DrainBufferedLines();

        await RunPassAsync(new DuckDbFactCollector(_duckDb));

        Assert.Empty(PassLines());
    }
}
