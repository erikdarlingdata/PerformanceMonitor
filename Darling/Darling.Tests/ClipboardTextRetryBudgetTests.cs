/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4624: pins <c>ClipboardText</c>'s retry bound honestly. Each WPF <c>Clipboard</c> call already runs its
/// own internal OLE retry loop before it throws - about 1.1 s per failed call, measured in the issue - so
/// the OLD 8-attempt/25-ms design's "~175 ms worst case" was wrong by close to two orders of magnitude; the
/// real worst case was closer to 9 s, all of it on the UI thread. This class drives
/// <c>ClipboardText</c>'s injectable seams (<c>ReadOnce</c>, <c>WriteOnce</c>, <c>WriteDataObjectOnce</c>) -
/// the same seams <c>TrySetText</c> / <c>TrySetDataObject</c> already used for #4582/#4594 - with a fake
/// that blocks and throws <see cref="ExternalException"/> the way a real OLE clipboard-open failure does,
/// so the bound is pinned without a real locked clipboard. Every test restores the seam it swaps in a
/// <c>finally</c>, and all mutation happens inside one test class so xunit's default sequential-within-class
/// execution keeps these from racing each other on the shared static state.
///
/// <para>What is pinned: the attempt count (<c>calls == 2</c>) and the backoff the product REQUESTS - exactly
/// one 25 ms delay between the two attempts, recorded through <c>SleepBackoff</c> / <c>DelayBackoff</c>.
/// Wall-clock time is deliberately NOT asserted: thread-pool starvation during the parallel suite makes any
/// elapsed-time bound noise (a healthy run takes ~25 ms and CI has measured seconds).</para>
/// </summary>
public sealed class ClipboardTextRetryBudgetTests
{
    // Stands in for the CLIPBRD_E_CANT_OPEN a real busy clipboard throws (System.Windows.Clipboard wraps it
    // in COMException, which ClipboardText's catch (ExternalException) - COMException's base type - already
    // covers; see ClipboardText.cs's own comment on that catch).
    private static ExternalException ClipboardBusyException() =>
        new("CLIPBRD_E_CANT_OPEN", unchecked((int)0x800401D0));

    [Fact]
    public void TryRead_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var delays = new List<int>();
        var originalDelay = ClipboardText.SleepBackoff;
        var original = ClipboardText.ReadOnce;

        try
        {
            ClipboardText.SleepBackoff = ms => delays.Add(ms);
            ClipboardText.ReadOnce = () =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            var ok = ClipboardText.TryRead(out var text);

            Assert.False(ok);
            Assert.Equal(string.Empty, text);
            Assert.Equal(2, calls);
            Assert.Equal(new[] { 25 }, delays);
        }
        finally
        {
            ClipboardText.SleepBackoff = originalDelay;
            ClipboardText.ReadOnce = original;
        }
    }

    [Fact]
    public async Task TryReadAsync_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var delays = new List<int>();
        var originalDelay = ClipboardText.DelayBackoff;
        var original = ClipboardText.ReadOnce;

        try
        {
            ClipboardText.DelayBackoff = ms =>
            {
                delays.Add(ms);
                return Task.CompletedTask;
            };
            ClipboardText.ReadOnce = () =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            var (ok, text) = await ClipboardText.TryReadAsync();

            Assert.False(ok);
            Assert.Equal(string.Empty, text);
            Assert.Equal(2, calls);
            Assert.Equal(new[] { 25 }, delays);
        }
        finally
        {
            ClipboardText.DelayBackoff = originalDelay;
            ClipboardText.ReadOnce = original;
        }
    }

    [Fact]
    public void TrySetText_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var delays = new List<int>();
        var originalDelay = ClipboardText.SleepBackoff;
        var original = ClipboardText.WriteOnce;

        try
        {
            ClipboardText.SleepBackoff = ms => delays.Add(ms);
            ClipboardText.WriteOnce = _ =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            var ok = ClipboardText.TrySetText("copied text");

            Assert.False(ok);
            Assert.Equal(2, calls);
            Assert.Equal(new[] { 25 }, delays);
        }
        finally
        {
            ClipboardText.SleepBackoff = originalDelay;
            ClipboardText.WriteOnce = original;
        }
    }

    [Fact]
    public void TrySetDataObject_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var delays = new List<int>();
        var originalDelay = ClipboardText.SleepBackoff;
        var original = ClipboardText.WriteDataObjectOnce;

        try
        {
            ClipboardText.SleepBackoff = ms => delays.Add(ms);
            ClipboardText.WriteDataObjectOnce = (_, _) =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            var ok = ClipboardText.TrySetDataObject("copied text");

            Assert.False(ok);
            Assert.Equal(2, calls);
            Assert.Equal(new[] { 25 }, delays);
        }
        finally
        {
            ClipboardText.SleepBackoff = originalDelay;
            ClipboardText.WriteDataObjectOnce = original;
        }
    }

    [Fact]
    public void TrySetText_SucceedsOnFirstAttempt_WithoutRetrying()
    {
        var calls = 0;
        var original = ClipboardText.WriteOnce;

        try
        {
            ClipboardText.WriteOnce = _ => calls++;

            var ok = ClipboardText.TrySetText("copied text");

            Assert.True(ok);
            Assert.Equal(1, calls);
        }
        finally
        {
            ClipboardText.WriteOnce = original;
        }
    }

    /// <summary>
    /// Pins the Stopwatch backstop itself, not just the attempt cap: a single attempt that blocks longer
    /// than the documented ~2.5 s budget must not be followed by a second one (one call, no backoff requested). Simulates the case the
    /// attempt count alone cannot bound - a WPF call slower than the ~1.1 s #4624 measured - by blocking
    /// past the budget on the FIRST attempt. Without the Stopwatch check, <c>MaxAttempts = 2</c> would still
    /// fire a second ~2.7 s attempt here, making calls 2 and requesting a backoff, which this test asserts against; that
    /// is the mutation this test is written to catch (see the PR body for the before/after run).
    /// </summary>
    [Fact]
    public void TrySetText_SkipsSecondAttempt_WhenFirstAttemptAloneSpendsTheBudget()
    {
        var calls = 0;
        var delays = new List<int>();
        var originalDelay = ClipboardText.SleepBackoff;
        var original = ClipboardText.WriteOnce;

        try
        {
            ClipboardText.SleepBackoff = ms => delays.Add(ms);
            ClipboardText.WriteOnce = _ =>
            {
                calls++;
                // Longer than the 2.5 s budget ClipboardText.cs documents, so attempt 1 alone already
                // exhausts it - the real-world case of one WPF call running slower than the ~1.1 s #4624
                // measured, not just the typical two-fast-failures case the other tests above cover.
                Thread.Sleep(TimeSpan.FromSeconds(2.7));
                throw ClipboardBusyException();
            };

            var ok = ClipboardText.TrySetText("copied text");

            Assert.False(ok);
            Assert.Equal(1, calls);
            Assert.Empty(delays);
        }
        finally
        {
            ClipboardText.SleepBackoff = originalDelay;
            ClipboardText.WriteOnce = original;
        }
    }
}
