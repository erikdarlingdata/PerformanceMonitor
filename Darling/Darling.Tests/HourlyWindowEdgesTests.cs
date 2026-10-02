/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>Pins <see cref="HourlyWindowEdges"/>: the disclosure of the hour-bucket edges of an hourly read.</summary>
public sealed class HourlyWindowEdgesTests
{
    private static readonly DateTime Aligned = new(2026, 1, 5, 10, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void UnalignedStart_NamesThePartialHour()
    {
        var note = HourlyWindowEdges.Note(Aligned.AddMinutes(37), Aligned.AddHours(1), Aligned.AddHours(4).AddMinutes(20), Aligned.AddHours(9));
        Assert.Contains("no bucket before " + Aligned.AddHours(1).ToString("o") + ": the data from", note, StringComparison.Ordinal);
        Assert.Contains(Aligned.AddMinutes(37).ToString("o"), note, StringComparison.Ordinal);
        Assert.Contains(Aligned.AddHours(1).ToString("o"), note, StringComparison.Ordinal);
    }

    [Fact]
    public void AlignedStart_HasNoLeadingSentence()
    {
        var note = HourlyWindowEdges.Note(Aligned, Aligned, Aligned.AddHours(4).AddMinutes(20), Aligned.AddHours(9));
        Assert.DoesNotContain("partial hour", note, StringComparison.Ordinal);
    }

    [Fact]
    public void UnalignedEnd_CountsTheWholeHourPastAsOf()
    {
        var end = Aligned.AddHours(4).AddMinutes(20);
        var note = HourlyWindowEdges.Note(Aligned, Aligned, end, Aligned.AddHours(9));
        Assert.Contains("up to " + Aligned.AddHours(5).ToString("o"), note, StringComparison.Ordinal);
    }

    /// <summary>The hourly reads stop BEFORE the window end (<c>bucket &lt; end</c>), so an end exactly on the
    /// hour no longer takes the hour that begins there: the served span ends at the window end and the note
    /// claims nothing past it. (This case used to be pinned as "counts the whole hour", when the bound was
    /// <c>bucket &lt;= end</c>.)</summary>
    [Fact]
    public void AlignedEnd_StopsAtTheEnd_AndCountsNothingPastAsOf()
    {
        var end = Aligned.AddHours(4);
        var note = HourlyWindowEdges.Note(Aligned, Aligned, end, Aligned.AddHours(9));
        Assert.DoesNotContain("included whole", note, StringComparison.Ordinal);
        Assert.DoesNotContain("counted past", note, StringComparison.Ordinal);

        var (_, servedEnd) = HourlyWindowEdges.ServedSpan(Aligned, Aligned, end, Aligned.AddHours(9));
        Assert.Equal(end, servedEnd);
    }

    [Fact]
    public void EndCutAtTheCeiling_SaysNothingAfterItWasRead()
    {
        var end = Aligned.AddHours(4).AddMinutes(20);
        var ceiling = Aligned.AddHours(3);
        var note = HourlyWindowEdges.Note(Aligned, Aligned, end, ceiling);
        Assert.Contains("the hourly rollup is materialized only to " + ceiling.ToString("o") + "; nothing after it was read", note, StringComparison.Ordinal);
        Assert.DoesNotContain("included whole", note, StringComparison.Ordinal);
    }

    [Fact]
    public void NullCeiling_SaysTheCeilingIsUnknown()
    {
        var note = HourlyWindowEdges.Note(Aligned, Aligned, Aligned.AddHours(4), null);
        Assert.Contains("materialization ceiling unknown; the end edge is not verified", note, StringComparison.Ordinal);
        Assert.DoesNotContain("nothing after it was read", note, StringComparison.Ordinal);
    }

    [Fact]
    public void CeilingPastAsOf_ServesTheWholeEndHourOnly()
    {
        var end = Aligned.AddHours(4).AddMinutes(20);
        var note = HourlyWindowEdges.Note(Aligned, Aligned, end, Aligned.AddHours(5).AddMinutes(30));
        Assert.Contains("up to " + Aligned.AddHours(5).ToString("o"), note, StringComparison.Ordinal);
        var low = HourlyWindowEdges.Note(Aligned, Aligned, end, Aligned.AddHours(5).AddMinutes(-30));
        Assert.Contains("up to " + Aligned.AddHours(5).AddMinutes(-30).ToString("o"), low, StringComparison.Ordinal);
    }

    [Fact]
    public void StartSnappedAndEndCut_NamesTheServedSpan()
    {
        var ceiling = Aligned.AddHours(3);
        var note = HourlyWindowEdges.Note(Aligned.AddMinutes(37), Aligned.AddHours(1), Aligned.AddHours(4).AddMinutes(20), ceiling);
        Assert.Contains("served from " + Aligned.AddHours(1).ToString("o") + " to " + ceiling.ToString("o"), note, StringComparison.Ordinal);
    }

    [Fact]
    public void ServedSpan_CutsTheEndAtTheCeiling()
    {
        var (start, end) = HourlyWindowEdges.ServedSpan(Aligned.AddMinutes(37), Aligned.AddHours(1), Aligned.AddHours(4).AddMinutes(20), Aligned.AddHours(3));
        Assert.Equal(Aligned.AddHours(1), start);
        Assert.Equal(Aligned.AddHours(3), end);
    }
}
