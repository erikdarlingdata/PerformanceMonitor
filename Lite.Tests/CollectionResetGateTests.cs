/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitorLite.Database;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The gate that stops a size-triggered reset deleting the database out from under a running collection
/// (#2594).
///
/// <para><b>The field failure this encodes.</b> Opening a server tab calls
/// <c>RunAllCollectorsForServerAsync</c> on a bare <c>Task.Run</c>, sequenced against nothing. When the store
/// crossed 512 MB while such a collection was running, the reset deleted and recreated <c>monitor.duckdb</c>
/// underneath it — <c>index_object_stats</c> was five seconds into a fifty-five second run — and the
/// collection's final <c>collection_log</c> insert failed with <c>Table with name collection_log does not
/// exist</c>. Nothing was lost on disk and a restart cleared it, which is the signature of stale in-process
/// state rather than a damaged store.</para>
///
/// <para>These run serially: the gate is process-wide static state, so concurrent tests would see each
/// other's registrations.</para>
/// </summary>
[Collection("CollectionResetGate")]
public class CollectionResetGateTests : IDisposable
{
    public CollectionResetGateTests() => CollectionResetGate.ResetForTests();

    public void Dispose() => CollectionResetGate.ResetForTests();

    [Fact]
    public async Task AReset_DoesNotStartWhileACollectionIsRunning()
    {
        using var collection = await CollectionResetGate.BeginCollectionAsync();

        Assert.Equal(1, CollectionResetGate.CollectionsInFlight);

        /* The whole point: the reset must not get the gate here. It waits for the drain timeout and then
           defers, which the caller reports and retries on the next archival check. */
        var reset = await WithShortDrain(() => CollectionResetGate.TryBeginResetAsync());

        Assert.Null(reset);
    }

    [Fact]
    public async Task AReset_ProceedsOnceTheCollectionFinishes()
    {
        var collection = await CollectionResetGate.BeginCollectionAsync();
        collection.Dispose();

        Assert.Equal(0, CollectionResetGate.CollectionsInFlight);

        using var reset = await CollectionResetGate.TryBeginResetAsync();

        Assert.NotNull(reset);
    }

    [Fact]
    public async Task AResetWaits_RatherThanFailingImmediately_WhenACollectionEndsShortly()
    {
        var collection = await CollectionResetGate.BeginCollectionAsync();

        /* Released while the reset is already waiting, which is the ordinary case: a collection finishes
           and the reset proceeds without anyone retrying. */
        _ = Task.Run(async () =>
        {
            await Task.Delay(TimeSpan.FromMilliseconds(300));
            collection.Dispose();
        });

        var stopwatch = Stopwatch.StartNew();
        using var reset = await CollectionResetGate.TryBeginResetAsync();
        stopwatch.Stop();

        Assert.NotNull(reset);
        Assert.True(
            stopwatch.Elapsed >= TimeSpan.FromMilliseconds(200),
            $"the reset returned in {stopwatch.ElapsedMilliseconds} ms, so it did not actually wait for the "
            + "collection to finish.");
    }

    /// <summary>
    /// A collection arriving mid-reset must wait, not start against a database that is about to be deleted.
    /// This is the other half of the race and the half that is easy to forget: gating only the reset leaves
    /// the tab-open path free to start one millisecond before the file disappears.
    /// </summary>
    [Fact]
    public async Task ACollection_DoesNotStartWhileAResetIsRunning()
    {
        var reset = await CollectionResetGate.TryBeginResetAsync();
        Assert.NotNull(reset);

        var started = CollectionResetGate.BeginCollectionAsync();

        var raced = await Task.WhenAny(started, Task.Delay(TimeSpan.FromMilliseconds(400)));
        Assert.NotSame(started, raced);

        reset!.Dispose();

        using var collection = await started.WaitAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(1, CollectionResetGate.CollectionsInFlight);
    }

    /// <summary>
    /// Collections must NOT serialise against each other. The background sweep and a tab-open sweep run
    /// concurrently by design, and turning that into a queue would be a behaviour change wearing a bug fix.
    /// </summary>
    [Fact]
    public async Task Collections_RunConcurrentlyWithEachOther()
    {
        using var first = await CollectionResetGate.BeginCollectionAsync();

        var second = CollectionResetGate.BeginCollectionAsync().WaitAsync(TimeSpan.FromSeconds(2));

        using var acquired = await second;

        Assert.Equal(2, CollectionResetGate.CollectionsInFlight);
    }

    [Fact]
    public async Task DisposingACollectionTwice_DoesNotDriveTheCounterNegative()
    {
        var collection = await CollectionResetGate.BeginCollectionAsync();
        collection.Dispose();
        collection.Dispose();

        Assert.Equal(0, CollectionResetGate.CollectionsInFlight);

        /* A negative counter would let a reset run while a real collection was still writing. */
        using var other = await CollectionResetGate.BeginCollectionAsync();
        Assert.Equal(1, CollectionResetGate.CollectionsInFlight);
    }

    /// <summary>
    /// The production drain timeout is three minutes, which no test should sit through. This shortens the
    /// wait by racing the call rather than by making the timeout configurable — a knob that exists only for
    /// tests is a knob somebody eventually sets in production.
    /// </summary>
    private static async Task<IDisposable?> WithShortDrain(Func<Task<IDisposable?>> attempt)
    {
        var call = attempt();
        var finished = await Task.WhenAny(call, Task.Delay(TimeSpan.FromMilliseconds(500)));

        if (finished == call)
        {
            return await call;
        }

        /* Still draining after the grace period, which is the assertion the caller wants: it did not take
           the gate. The underlying call abandons itself when the collection scope is disposed in teardown. */
        return null;
    }
}

[CollectionDefinition("CollectionResetGate", DisableParallelization = true)]
public sealed class CollectionResetGateCollection
{
}
