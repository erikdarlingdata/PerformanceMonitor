/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #2235's minor finding: min/max CPU and elapsed on the top-queries/top-procedures reads are
/// lifetime extremes for the plan's cache residency, and a lifetime max can EXCEED the windowed
/// total — which reads as impossible unless labeled. These pin the conditional note: it fires only
/// on the provable case and names exactly the column(s) that prove it.
/// </summary>
public sealed class QueryStatExtremesTests
{
    /// <summary>The ordinary row: extremes inside the window's totals say nothing.</summary>
    [Fact]
    public void ExtremesWithinTotalsCarryNoNote()
    {
        Assert.Null(QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 1000, maxCpu: 400, totalElapsed: 2000, maxElapsed: 900));
    }

    /// <summary>
    /// Equality is NOT the provable case — a single-execution window has max == total by
    /// construction, and flagging it would put the note on every one-shot query.
    /// </summary>
    [Fact]
    public void ExactEqualityCarriesNoNote()
    {
        Assert.Null(QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 400, maxCpu: 400, totalElapsed: 900, maxElapsed: 900));
    }

    /// <summary>
    /// The field case that filed this: dbo.ClosingReportV6, window total 158,906 ms against a
    /// lifetime max of 322,066 ms. The note fires and names the CPU column.
    /// </summary>
    [Fact]
    public void CpuExtremeBeyondTheWindowTotalIsFlagged()
    {
        var note = QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 158906, maxCpu: 322066, totalElapsed: 500000, maxElapsed: 400000);

        Assert.NotNull(note);
        Assert.Contains("max_cpu_ms exceeds", note, System.StringComparison.Ordinal);
        Assert.DoesNotContain("max_elapsed_ms", note, System.StringComparison.Ordinal);
    }

    /// <summary>Elapsed alone proves it too, and the note names that column instead.</summary>
    [Fact]
    public void ElapsedExtremeBeyondTheWindowTotalIsFlagged()
    {
        var note = QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 500000, maxCpu: 400000, totalElapsed: 158906, maxElapsed: 322066);

        Assert.NotNull(note);
        Assert.Contains("max_elapsed_ms exceeds", note, System.StringComparison.Ordinal);
        Assert.DoesNotContain("max_cpu_ms", note, System.StringComparison.Ordinal);
    }

    /// <summary>Both provable, both named — a reader should not have to re-derive which half.</summary>
    [Fact]
    public void BothExtremesBeyondTotalsNameBoth()
    {
        var note = QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 100, maxCpu: 200, totalElapsed: 100, maxElapsed: 200);

        Assert.NotNull(note);
        Assert.Contains("max_cpu_ms and max_elapsed_ms exceed", note, System.StringComparison.Ordinal);
    }

    /// <summary>
    /// A zero-total window with a nonzero lifetime max is the purest form of the problem — the plan
    /// did nothing this window and the extreme is entirely inherited. It must flag.
    /// </summary>
    [Fact]
    public void InheritedExtremeOnAnIdleWindowIsFlagged()
    {
        Assert.NotNull(QueryStatExtremes.LifetimeExtremeNote(
            totalCpu: 0, maxCpu: 5, totalElapsed: 0, maxElapsed: 5));
    }
}
