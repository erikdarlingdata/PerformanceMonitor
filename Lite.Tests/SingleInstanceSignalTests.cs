/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using PerformanceMonitor.Ui;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// Coverage for <see cref="SingleInstanceSignal"/> — the cross-process "surface the window" channel
/// (#769, #1050). A second launch signals the named event and the owning instance restores its window
/// through WPF's Show() path instead of a raw Win32 ShowWindow (which leaves a tray-hidden window
/// blank). These exercise the handshake without WPF: that a signal wakes the owner's callback, that
/// each launch delivers exactly one restore, and that there is nothing to signal once the owner exits.
/// </summary>
public class SingleInstanceSignalTests
{
    private static readonly TimeSpan WaitTimeout = TimeSpan.FromSeconds(2);

    /* Unique per test so concurrent runs (and leftover machine-global kernel objects) can't collide. */
    private static string UniqueName() => "PMLite_Test_" + Guid.NewGuid().ToString("N");

    [Fact]
    public void TrySignal_NoOwner_ReturnsFalse()
    {
        Assert.False(SingleInstanceSignal.TrySignal(UniqueName()));
    }

    [Fact]
    public void TrySignal_WithOwner_InvokesCallback()
    {
        var name = UniqueName();
        using var fired = new AutoResetEvent(false);
        using var owner = new SingleInstanceSignal(name, () => fired.Set());

        Assert.True(SingleInstanceSignal.TrySignal(name));
        Assert.True(fired.WaitOne(WaitTimeout), "the owner's callback should fire after a signal");
    }

    [Fact]
    public void TrySignal_DeliversOneCallbackPerSignal()
    {
        var name = UniqueName();
        var count = 0;
        using var fired = new AutoResetEvent(false);
        using var owner = new SingleInstanceSignal(name, () =>
        {
            Interlocked.Increment(ref count);
            fired.Set();
        });

        /* Sequentially: send, wait for the callback to be consumed, send again. AutoReset means each
           launch maps to exactly one restore. */
        Assert.True(SingleInstanceSignal.TrySignal(name));
        Assert.True(fired.WaitOne(WaitTimeout));

        Assert.True(SingleInstanceSignal.TrySignal(name));
        Assert.True(fired.WaitOne(WaitTimeout));

        Assert.Equal(2, Volatile.Read(ref count));
    }

    [Fact]
    public void Dispose_ReleasesChannel_NothingLeftToSignal()
    {
        var name = UniqueName();
        var owner = new SingleInstanceSignal(name, () => { });
        owner.Dispose();

        /* The owner held the only handle on the named event; once disposed the kernel object is gone,
           so a later launch finds no one to surface. */
        Assert.False(SingleInstanceSignal.TrySignal(name));
    }

    [Fact]
    public void Constructor_NullCallback_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => new SingleInstanceSignal(UniqueName(), null!));
    }

    [Fact]
    public void Constructor_EmptyEventName_Throws()
    {
        Assert.Throws<ArgumentException>(() => new SingleInstanceSignal("", () => { }));
    }
}
