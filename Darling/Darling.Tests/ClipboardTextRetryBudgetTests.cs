/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
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
/// </summary>
public sealed class ClipboardTextRetryBudgetTests
{
    // Stands in for the CLIPBRD_E_CANT_OPEN a real busy clipboard throws (System.Windows.Clipboard wraps it
    // in COMException, which ClipboardText's catch (ExternalException) - COMException's base type - already
    // covers; see ClipboardText.cs's own comment on that catch).
    // Mirrors ClipboardText.RetryBudget (private there).
    private static readonly TimeSpan RetryBudget = TimeSpan.FromSeconds(2.5);

    private static ExternalException ClipboardBusyException() =>
        new("CLIPBRD_E_CANT_OPEN", unchecked((int)0x800401D0));

    [Fact]
    public void TryRead_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var original = ClipboardText.ReadOnce;

        try
        {
            ClipboardText.ReadOnce = () =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            // Warm up: the first exception construction is not timed.
            _ = ClipboardBusyException();
            var stopwatch = Stopwatch.StartNew();
            var ok = ClipboardText.TryRead(out var text);
            stopwatch.Stop();

            Assert.False(ok);
            Assert.Equal(string.Empty, text);
            Assert.Equal(2, calls);
            Assert.True(
                // Wall-clock tolerant on busy runners; calls == 2 is the exact claim. This only catches a long sleep
                // or retrying for the whole budget.
                stopwatch.Elapsed < RetryBudget,
                $"expected a fast-failing clipboard to give up inside the {RetryBudget} retry budget, took {stopwatch.Elapsed}");
        }
        finally
        {
            ClipboardText.ReadOnce = original;
        }
    }

    [Fact]
    public async Task TryReadAsync_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var original = ClipboardText.ReadOnce;

        try
        {
            ClipboardText.ReadOnce = () =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            // Warm up: the first exception construction is not timed.
            _ = ClipboardBusyException();
            var stopwatch = Stopwatch.StartNew();
            var (ok, text) = await ClipboardText.TryReadAsync();
            stopwatch.Stop();

            Assert.False(ok);
            Assert.Equal(string.Empty, text);
            Assert.Equal(2, calls);
            Assert.True(
                // Wall-clock tolerant on busy runners; calls == 2 is the exact claim. This only catches a long sleep
                // or retrying for the whole budget.
                stopwatch.Elapsed < RetryBudget,
                $"expected a fast-failing clipboard to give up inside the {RetryBudget} retry budget, took {stopwatch.Elapsed}");
        }
        finally
        {
            ClipboardText.ReadOnce = original;
        }
    }

    [Fact]
    public void TrySetText_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var original = ClipboardText.WriteOnce;

        try
        {
            ClipboardText.WriteOnce = _ =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            // Warm up: the first exception construction is not timed.
            _ = ClipboardBusyException();
            var stopwatch = Stopwatch.StartNew();
            var ok = ClipboardText.TrySetText("copied text");
            stopwatch.Stop();

            Assert.False(ok);
            Assert.Equal(2, calls);
            Assert.True(
                // Wall-clock tolerant on busy runners; calls == 2 is the exact claim. This only catches a long sleep
                // or retrying for the whole budget.
                stopwatch.Elapsed < RetryBudget,
                $"expected a fast-failing clipboard to give up inside the {RetryBudget} retry budget, took {stopwatch.Elapsed}");
        }
        finally
        {
            ClipboardText.WriteOnce = original;
        }
    }

    [Fact]
    public void TrySetDataObject_MakesNoMoreThanTwoAttempts_WhenClipboardStaysBusy()
    {
        var calls = 0;
        var original = ClipboardText.WriteDataObjectOnce;

        try
        {
            ClipboardText.WriteDataObjectOnce = (_, _) =>
            {
                calls++;
                throw ClipboardBusyException();
            };

            // Warm up: the first exception construction is not timed.
            _ = ClipboardBusyException();
            var stopwatch = Stopwatch.StartNew();
            var ok = ClipboardText.TrySetDataObject("copied text");
            stopwatch.Stop();

            Assert.False(ok);
            Assert.Equal(2, calls);
            Assert.True(
                // Wall-clock tolerant on busy runners; calls == 2 is the exact claim. This only catches a long sleep
                // or retrying for the whole budget.
                stopwatch.Elapsed < RetryBudget,
                $"expected a fast-failing clipboard to give up inside the {RetryBudget} retry budget, took {stopwatch.Elapsed}");
        }
        finally
        {
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
    /// than the documented ~2.5 s budget must not be followed by a second one. Simulates the case the
    /// attempt count alone cannot bound - a WPF call slower than the ~1.1 s #4624 measured - by blocking
    /// past the budget on the FIRST attempt. Without the Stopwatch check, <c>MaxAttempts = 2</c> would still
    /// fire a second ~2.7 s attempt here, roughly doubling the elapsed time this test asserts against; that
    /// is the mutation this test is written to catch (see the PR body for the before/after run).
    /// </summary>
    [Fact]
    public void TrySetText_SkipsSecondAttempt_WhenFirstAttemptAloneSpendsTheBudget()
    {
        var calls = 0;
        var original = ClipboardText.WriteOnce;

        try
        {
            ClipboardText.WriteOnce = _ =>
            {
                calls++;
                // Longer than the 2.5 s budget ClipboardText.cs documents, so attempt 1 alone already
                // exhausts it - the real-world case of one WPF call running slower than the ~1.1 s #4624
                // measured, not just the typical two-fast-failures case the other tests above cover.
                Thread.Sleep(TimeSpan.FromSeconds(2.7));
                throw ClipboardBusyException();
            };

            // Warm up: the first exception construction is not timed.
            _ = ClipboardBusyException();
            var stopwatch = Stopwatch.StartNew();
            var ok = ClipboardText.TrySetText("copied text");
            stopwatch.Stop();

            Assert.False(ok);
            Assert.Equal(1, calls);
            Assert.True(
                stopwatch.Elapsed < TimeSpan.FromSeconds(4),
                "expected the budget to skip a second ~2.7 s attempt (which would push elapsed past 5 s), " +
                $"took {stopwatch.Elapsed}");
        }
        finally
        {
            ClipboardText.WriteOnce = original;
        }
    }
}
