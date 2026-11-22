/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9303: after a service start the trigram indexes' pending-list clean must not wait for the build's start delay. The delay keeps a
/// whole-table build off the post-restart IO burst; the clean only works on an index that exists and is what keeps a collection insert
/// from merging the pending list inside its own statement.
/// </summary>
public sealed class TrigramCleanStartTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    /// <summary>A wait that returns at once for a zero span and otherwise records the span and holds until shutdown.</summary>
    private sealed class RecordingDelay
    {
        private readonly object _gate = new();
        private readonly List<TimeSpan> _waits = [];

        public IReadOnlyList<TimeSpan> Waits
        {
            get
            {
                lock (_gate)
                {
                    return [.. _waits];
                }
            }
        }

        public Task DelayAsync(TimeSpan wait, CancellationToken cancellationToken)
        {
            if (wait == TimeSpan.Zero)
            {
                return Task.CompletedTask;
            }

            lock (_gate)
            {
                _waits.Add(wait);
            }

            return Task.Delay(Timeout.Infinite, cancellationToken);
        }
    }

    [Fact]
    public async Task TheCleanRunsItsFirstPass_WhileTheBuildStillWaitsItsStartDelay()
    {
        var ct = TestContext.Current.CancellationToken;
        var delay = new RecordingDelay();
        var passes = 0;
        var firstClean = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var run = QueryTextTrigramIndex.RunLoopsAsync(
            () => true, new QueryTextTrigramIndexTracker(), NullLogger.Instance, "Trigram index",
            QueryTextTrigramIndex.StartDelay, QueryTextTrigramIndex.PassInterval, QueryTextTrigramIndex.CleanInterval,
            (_, _) =>
            {
                Interlocked.Increment(ref passes);
                return Task.FromResult(true);
            },
            _ =>
            {
                firstClean.TrySetResult();
                return Task.CompletedTask;
            },
            delay.DelayAsync, stop.Token);

        await firstClean.Task.WaitAsync(Guard, ct);

        Assert.Equal(0, Volatile.Read(ref passes));
        Assert.Contains(QueryTextTrigramIndex.StartDelay, delay.Waits);

        await stop.CancelAsync();
        await run.WaitAsync(Guard, ct);
        Assert.True(run.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task TheCleanWaitsItsOwnInterval_AfterTheFirstPass()
    {
        var ct = TestContext.Current.CancellationToken;
        var delay = new RecordingDelay();
        var firstClean = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var run = QueryTextTrigramIndex.RunLoopsAsync(
            () => true, new QueryTextTrigramIndexTracker(), NullLogger.Instance, "Trigram index",
            QueryTextTrigramIndex.StartDelay, QueryTextTrigramIndex.PassInterval, TimeSpan.FromMinutes(2),
            (_, _) => Task.FromResult(true),
            _ =>
            {
                firstClean.TrySetResult();
                return Task.CompletedTask;
            },
            delay.DelayAsync, stop.Token);

        await firstClean.Task.WaitAsync(Guard, ct);
        var deadline = DateTime.UtcNow + Guard;
        while (!delay.Waits.Contains(TimeSpan.FromMinutes(2)) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(20, ct);
        }

        Assert.Contains(TimeSpan.FromMinutes(2), delay.Waits);

        await stop.CancelAsync();
        await run.WaitAsync(Guard, ct);
    }

    /// <summary>The real <c>RunAsync</c> of each index, with the build's own start delay and a store that refuses the connection: the first
    /// clean pass still reaches its connection at once and reports the refusal, long before the delay ends.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RunAsync_StartsTheCleanAtOnce_WithTheBuildsStartDelay(bool queryStore)
    {
        var ct = TestContext.Current.CancellationToken;
        await using var postgres = NpgsqlDataSource.Create("Host=127.0.0.1;Port=1;Username=x;Password=x;Database=x;Timeout=5;Pooling=false");
        var logger = new CapturingTestLogger();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tracker = new QueryTextTrigramIndexTracker();

        var run = queryStore
            ? QueryStoreTextTrigramIndex.RunAsync(
                postgres, () => true, tracker, logger, QueryStoreTextTrigramIndex.StartDelay, QueryStoreTextTrigramIndex.PassInterval, stop.Token)
            : QueryTextTrigramIndex.RunAsync(
                postgres, () => true, tracker, logger, QueryTextTrigramIndex.StartDelay, QueryTextTrigramIndex.PassInterval, stop.Token);

        var deadline = DateTime.UtcNow + Guard;
        while (logger.CountAtLevel(LogLevel.Warning) == 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, ct);
        }

        var warnings = logger.CountAtLevel(LogLevel.Warning);
        await stop.CancelAsync();
        await run.WaitAsync(Guard, ct);

        Assert.True(warnings > 0, "The first clean did not reach the store before the build's start delay ended. " + logger.Joined);
    }
}
