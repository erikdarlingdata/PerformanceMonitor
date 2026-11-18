/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8978: the two trigram text index options are tri-state on the store's service row. NULL defers to darling.json, a store value
/// wins, the index loop reads the resolved value at every pass, and a host that loads the file after the store was applied cannot put
/// the file value back on the shared tracker.
/// </summary>
public sealed class TrigramIndexStoreOptionTests
{
    /// <summary>Bounds every wait on the stepped loop, so a loop that stops stepping fails the test instead of hanging it.</summary>
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(30);

    [Theory]
    [InlineData(null, false, false)]
    [InlineData(null, true, true)]
    [InlineData(true, false, true)]
    [InlineData(false, true, false)]
    [InlineData(true, true, true)]
    [InlineData(false, false, false)]
    public void Resolve_TheStoreValueWins_AndNullFallsBackToTheFile(bool? store, bool file, bool expected)
    {
        Assert.Equal(expected, DarlingConfig.ResolveTrigramIndexOption(store, file));
        Assert.Equal(store is null ? QueryTextTrigramIndexTracker.SourceFile : QueryTextTrigramIndexTracker.SourceStore,
            DarlingConfig.TrigramIndexOptionSource(store));
    }

    [Fact]
    public void ApplyToConfig_CarriesTheStoreValues_AndLeavesTheFileValuesAlone()
    {
        var config = new DarlingConfig { QueryTextTrigramIndexEnabled = true, QueryStoreTextTrigramIndexEnabled = false };

        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView
        {
            QueryTextTrigramIndexStoreValue = false,
            QueryStoreTextTrigramIndexStoreValue = true,
        });
        Assert.False(config.QueryTextTrigramIndexResolved);
        Assert.True(config.QueryStoreTextTrigramIndexResolved);
        Assert.True(config.QueryTextTrigramIndexEnabled);
        Assert.False(config.QueryStoreTextTrigramIndexEnabled);

        /* A store column cleared back to NULL hands the option back to the file at the next reload. */
        StoreConfigProvider.ApplyToConfig(config, new StoreConfigView());
        Assert.Null(config.QueryTextTrigramIndexStoreValue);
        Assert.Null(config.QueryStoreTextTrigramIndexStoreValue);
        Assert.True(config.QueryTextTrigramIndexResolved);
        Assert.False(config.QueryStoreTextTrigramIndexResolved);
    }

    [Fact]
    public void ConfigureFromFile_SetsTheFileValue_UntilTheResolvedValueIsSet_ThenIsIgnored()
    {
        var tracker = new QueryTextTrigramIndexTracker();
        Assert.False(tracker.Read().Configured);

        tracker.ConfigureFromFile(true);
        var fromFile = tracker.Read();
        Assert.True(fromFile.Configured);
        Assert.True(fromFile.Enabled);
        Assert.Equal(QueryTextTrigramIndexTracker.SourceFile, fromFile.OptionSource);

        tracker.Configure(false, QueryTextTrigramIndexTracker.SourceStore);
        /* The MCP host started later by the store's mcp_enabled loads darling.json again; the store's value stays. */
        tracker.ConfigureFromFile(true);
        var afterLateLoad = tracker.Read();
        Assert.False(afterLateLoad.Enabled);
        Assert.Equal(QueryTextTrigramIndexTracker.SourceStore, afterLateLoad.OptionSource);

        /* The pass configures with the value alone and keeps the source the worker recorded. */
        tracker.Configure(true);
        Assert.True(tracker.Read().Enabled);
        Assert.Equal(QueryTextTrigramIndexTracker.SourceStore, tracker.Read().OptionSource);
    }

    /// <summary>A delay the test steps by hand: each call reports its wait on <see cref="Waits"/> and returns only when released.</summary>
    private sealed class SteppedDelay
    {
        private readonly SemaphoreSlim _release = new(0);

        public Channel<TimeSpan> Waits { get; } = Channel.CreateUnbounded<TimeSpan>();

        public async Task DelayAsync(TimeSpan wait, CancellationToken cancellationToken)
        {
            await Waits.Writer.WriteAsync(wait, cancellationToken);
            await _release.WaitAsync(cancellationToken);
        }

        public void Release() => _release.Release();

        public Task<TimeSpan> NextWaitAsync(CancellationToken cancellationToken) =>
            Waits.Reader.ReadAsync(cancellationToken).AsTask().WaitAsync(Guard, cancellationToken);
    }

    [Fact]
    public async Task TheLoop_ReadsTheOptionAtEveryPass_TheTrackerFollows_AndASettledPassDoesNotEndIt()
    {
        var ct = TestContext.Current.CancellationToken;
        var tracker = new QueryTextTrigramIndexTracker();
        var option = false;
        var passes = new List<bool>();
        var delay = new SteppedDelay();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var loop = QueryTextTrigramIndex.RunLoopAsync(
            () => option, tracker, NullLogger.Instance, "Trigram index", TimeSpan.FromMinutes(20), TimeSpan.FromMinutes(30),
            (on, _) =>
            {
                passes.Add(on);
                return Task.FromResult(true); // "settled": an off option with no index left
            },
            delay.DelayAsync, stop.Token);

        /* The tracker holds the option before the first delay, so get_store_metrics never reads it unconfigured. */
        Assert.Equal(TimeSpan.FromMinutes(20), await delay.NextWaitAsync(ct));
        Assert.True(tracker.Read().Configured);
        Assert.False(tracker.Read().Enabled);
        Assert.Empty(passes);

        delay.Release();
        Assert.Equal(TimeSpan.FromMinutes(30), await delay.NextWaitAsync(ct));
        Assert.Equal([false], passes);

        option = true;
        delay.Release();
        Assert.Equal(TimeSpan.FromMinutes(30), await delay.NextWaitAsync(ct));
        Assert.Equal([false, true], passes);
        Assert.True(tracker.Read().Enabled);

        option = false;
        delay.Release();
        Assert.Equal(TimeSpan.FromMinutes(30), await delay.NextWaitAsync(ct));
        Assert.Equal([false, true, false], passes);
        Assert.False(tracker.Read().Enabled);

        await stop.CancelAsync();
        await loop.WaitAsync(Guard, ct);
        Assert.True(loop.IsCompletedSuccessfully);
    }

    [Fact]
    public async Task TheLoop_KeepsGoingAfterAPassThrows()
    {
        var ct = TestContext.Current.CancellationToken;
        var tracker = new QueryTextTrigramIndexTracker();
        var calls = 0;
        var delay = new SteppedDelay();
        using var stop = CancellationTokenSource.CreateLinkedTokenSource(ct);

        var loop = QueryTextTrigramIndex.RunLoopAsync(
            () => true, tracker, NullLogger.Instance, "Query Store text trigram index", TimeSpan.Zero, TimeSpan.FromMinutes(30),
            (_, _) => ++calls == 1 ? throw new InvalidOperationException("catalog read failed") : Task.FromResult(false),
            delay.DelayAsync, stop.Token);

        Assert.Equal(TimeSpan.Zero, await delay.NextWaitAsync(ct));
        delay.Release();
        Assert.Equal(TimeSpan.FromMinutes(30), await delay.NextWaitAsync(ct));
        delay.Release();
        Assert.Equal(TimeSpan.FromMinutes(30), await delay.NextWaitAsync(ct));
        Assert.Equal(2, calls);

        await stop.CancelAsync();
        await loop.WaitAsync(Guard, ct);
        Assert.True(loop.IsCompletedSuccessfully);
    }
}
