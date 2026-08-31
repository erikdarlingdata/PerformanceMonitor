/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the #2717 gate: the generic per-(server, collector) single-flight exclusion used by every collector
/// detached from Darling's sequential per-server body for a bimodal, data-driven cost tail — query_store first
/// (#2701), plan_correction second (#2717). Same shape as <see cref="QueryStoreServerGate"/> and the same two
/// properties matter: it EXCLUDES, and it never WAITS — a blocking acquire here would let one server's slow
/// detached run stall the caller that keyed the dictionary, recreating the #2148 wedge this whole family of
/// gates exists to avoid.
/// </summary>
public sealed class DetachedCollectorGateTests
{
    /// <summary>THE POINT: a second acquirer is refused while the first holds the gate.</summary>
    [Fact]
    public void ASecondAcquirerIsRefusedWhileTheFirstHoldsIt()
    {
        var gate = new DetachedCollectorGate();

        using var first = gate.TryAcquire();

        Assert.NotNull(first);
        Assert.Null(gate.TryAcquire());
        Assert.True(gate.IsHeld);
    }

    /// <summary>
    /// Releasing hands the gate to the next caller. A gate that could only be taken once would permanently stop
    /// the detached collector on that server — and it would look like "that collector has no data" rather than
    /// like a bug.
    /// </summary>
    [Fact]
    public void ReleasingLetsTheNextTickIn()
    {
        var gate = new DetachedCollectorGate();

        var first = gate.TryAcquire();
        Assert.NotNull(first);
        first!.Dispose();

        Assert.False(gate.IsHeld);
        using var second = gate.TryAcquire();
        Assert.NotNull(second);
    }

    /// <summary>
    /// It never blocks. Asserted as elapsed time against a HELD gate, because the failure this guards is a
    /// blocking acquire silently replacing the try-acquire — which would still pass an exclusion-only test
    /// while reintroducing a fleet stall (one server's still-running detached tick would freeze the whole
    /// per-server foreach that keyed the dictionary, which is the exact per-body sequential-await problem
    /// #2701/#2717 detach collectors to avoid in the first place).
    /// </summary>
    [Fact]
    public void ARefusedAcquireReturnsImmediatelyRatherThanWaiting()
    {
        var gate = new DetachedCollectorGate();
        using var held = gate.TryAcquire();

        var started = Environment.TickCount64;
        for (var i = 0; i < 1_000; i++)
        {
            Assert.Null(gate.TryAcquire());
        }

        Assert.True(Environment.TickCount64 - started < 1_000,
            "a thousand refused acquires must not block — a blocking acquire here stalls the caller's whole sweep");
    }

    /// <summary>
    /// Double-dispose must not release a gate a DIFFERENT tick has since taken. Without idempotence, "this
    /// tick's own detached run disposes twice, the next tick acquires in between" leaves two detached runs of
    /// the SAME collector against the SAME server in flight at once — the exact condition the gate exists to
    /// prevent, reached through a stray extra Dispose rather than through missing exclusion.
    /// </summary>
    [Fact]
    public void DisposingALeaseTwiceCannotReleaseSomebodyElsesHold()
    {
        var gate = new DetachedCollectorGate();

        var firstTick = gate.TryAcquire();
        Assert.NotNull(firstTick);
        firstTick!.Dispose();

        var secondTick = gate.TryAcquire();
        Assert.NotNull(secondTick);

        firstTick.Dispose();                 /* the stray second dispose */

        Assert.True(gate.IsHeld, "the second tick's hold must survive the first tick's double-dispose");
        Assert.Null(gate.TryAcquire());
        secondTick!.Dispose();
    }

    /// <summary>
    /// Under real contention exactly ONE holder exists at a time. Two overlapping detached runs of the same
    /// collector against the same server is precisely the field risk this gate exists to prevent (doubled
    /// transient load on a server already in its worst-case cost tail), so the concurrent-holder count is
    /// asserted directly rather than inferred.
    /// </summary>
    [Fact]
    public async Task UnderContentionOnlyOneHolderEverExistsAtOnce()
    {
        var gate = new DetachedCollectorGate();
        var concurrent = 0;
        var maxObserved = 0;
        var acquisitions = 0;

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 2_000; i++)
            {
                using var lease = gate.TryAcquire();
                if (lease is null)
                {
                    continue;
                }

                Interlocked.Increment(ref acquisitions);
                var now = Interlocked.Increment(ref concurrent);
                InterlockedMax(ref maxObserved, now);
                Interlocked.Decrement(ref concurrent);
            }
        })));

        Assert.True(acquisitions > 0, "the contention loop must have actually acquired the gate at least once");
        Assert.Equal(1, maxObserved);
    }

    private static void InterlockedMax(ref int target, int candidate)
    {
        int initial;
        do
        {
            initial = Volatile.Read(ref target);
            if (candidate <= initial)
            {
                return;
            }
        }
        while (Interlocked.CompareExchange(ref target, candidate, initial) != initial);
    }
}
