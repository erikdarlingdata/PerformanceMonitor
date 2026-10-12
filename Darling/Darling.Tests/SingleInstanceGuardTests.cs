/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins the #1581 single-instance guard (Fix B): the mutex-based "am I the only running instance" decision that
/// stops a stray second invocation (the incident's <c>Service.exe --version</c> that fell into a real startup)
/// from spawning a second service that fights the first over the bundled PostgreSQL and ports. The behavioral
/// cases drive real named mutexes across threads in-process (deterministic — a full cross-PROCESS test is not
/// worth the flakiness), using unique <c>Local\</c> names so they need no <c>SeCreateGlobalPrivilege</c>; they
/// are Windows-gated because named-mutex namespace semantics are Windows-specific. The production name and the
/// release-on-dispose (SCM stop→start) contract are pinned too.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class SingleInstanceGuardTests
{
    private static string UniqueName() => @"Local\Darling-Test-" + Guid.NewGuid().ToString("N");

    [Fact]
    public void ServiceMutexName_IsTheGlobalProductName()
    {
        /* Global\ so the lock spans terminal-services sessions: the service runs in session 0 and an interactive
           invocation runs in the user session — a Local\ mutex would not collide, which is the whole incident. */
        Assert.Equal(@"Global\PerformanceMonitorDarlingService", SingleInstanceGuard.ServiceMutexName);
    }

    [Fact]
    public void TryAcquire_FreshName_OwnsAndIsNotAbandoned()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Named-mutex namespace semantics are Windows-specific.");

        var name = UniqueName();
        using var guard = SingleInstanceGuard.TryAcquire(name);

        Assert.True(guard.Owns);
        Assert.False(guard.WasAbandoned);
    }

    [Fact]
    public void TryAcquire_AfterDispose_ReAcquiresCleanly_ScmStopStart()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Named-mutex namespace semantics are Windows-specific.");

        var name = UniqueName();

        /* First "instance" acquires then disposes (a graceful stop releases the mutex). */
        var first = SingleInstanceGuard.TryAcquire(name);
        Assert.True(first.Owns);
        first.Dispose();

        /* A subsequent "start" (fresh process, here a fresh guard) re-acquires cleanly, not as abandoned. */
        using var second = SingleInstanceGuard.TryAcquire(name);
        Assert.True(second.Owns);
        Assert.False(second.WasAbandoned);
    }

    [Fact]
    public void TryAcquire_WhenHeldByAnotherThread_DoesNotOwn()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Named-mutex namespace semantics are Windows-specific.");

        var name = UniqueName();

        /* A background thread acquires and HOLDS the named mutex; the main thread must then fail to own it. A
           named mutex is one kernel object across all handles in the process, but ownership is per-thread, so the
           main thread's WaitOne(0) returns false while the other thread holds it — exactly the second-instance
           case. keepAlive keeps a handle open so the object persists for the whole test. */
        using var keepAlive = new Mutex(false, name);
        using var acquired = new ManualResetEventSlim(false);
        using var release = new ManualResetEventSlim(false);

        var holder = new Thread(() =>
        {
            keepAlive.WaitOne();
            acquired.Set();
            release.Wait();
            keepAlive.ReleaseMutex();
        })
        { IsBackground = true };
        holder.Start();

        try
        {
            Assert.True(acquired.Wait(TimeSpan.FromSeconds(5)), "the holder thread did not acquire the mutex in time");

            using var guard = SingleInstanceGuard.TryAcquire(name);
            Assert.False(guard.Owns);
            Assert.False(guard.WasAbandoned);
        }
        finally
        {
            release.Set();
            holder.Join(TimeSpan.FromSeconds(5));
        }
    }

    [Fact]
    public void TryAcquire_AbandonedByCrashedInstance_OwnsWithAbandonedWarning()
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "Named-mutex namespace semantics are Windows-specific.");

        var name = UniqueName();

        /* A background thread acquires the mutex then EXITS without releasing it — exactly a crashed/force-killed
           previous instance. keepAlive (on this thread) keeps the kernel object alive after the holder exits, so
           it is genuinely ABANDONED rather than destroyed. The next acquire must then succeed WITH the abandoned
           flag set (a stale mutex must never wedge every future start). */
        using var keepAlive = new Mutex(false, name);
        using var acquired = new ManualResetEventSlim(false);

        var holder = new Thread(() =>
        {
            keepAlive.WaitOne();
            acquired.Set();
            /* Return without ReleaseMutex -> the mutex is abandoned when this thread exits. */
        })
        { IsBackground = true };
        holder.Start();

        Assert.True(acquired.Wait(TimeSpan.FromSeconds(5)), "the holder thread did not acquire the mutex in time");
        holder.Join(TimeSpan.FromSeconds(5));

        /* The kernel marks the mutex abandoned as the holder's OS thread is torn down, which completes shortly
           AFTER Thread.Join returns (Join synchronizes on the MANAGED thread; the native teardown that abandons
           the mutex can lag by a few milliseconds). The production TryAcquire is a non-blocking WaitOne(0), so a
           poll landing in that window sees the mutex still owned by the dying thread and returns false — a race
           that exists ONLY for this in-process crash SIMULATION, never in production, where the crashed PROCESS
           is fully dead (all handles closed by OS process teardown) before the SCM starts the replacement. Retry
           the non-blocking acquire until the abandonment is observable, then assert the real contract: an
           abandoned mutex is acquired WITH the abandoned warning. The guard itself must NOT retry — a false there
           means a LIVE second instance holds the lock and exiting is correct — so the retry lives here, not in
           the production code. */
        var guard = SingleInstanceGuard.TryAcquire(name);
        for (var attempt = 0; attempt < 200 && !guard.Owns; attempt++)
        {
            guard.Dispose();
            Thread.Sleep(10);
            guard = SingleInstanceGuard.TryAcquire(name);
        }

        using var acquiredGuard = guard;
        Assert.True(guard.Owns, "an abandoned mutex must become acquirable once the holder thread is fully torn down");
        Assert.True(guard.WasAbandoned);
    }
}
