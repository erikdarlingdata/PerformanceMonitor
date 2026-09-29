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
        var note = HourlyWindowEdges.Note(Aligned.AddMinutes(37), Aligned.AddHours(1), Aligned.AddHours(4).AddMinutes(20));
        Assert.Contains("the partial hour from", note, StringComparison.Ordinal);
        Assert.Contains(Aligned.AddMinutes(37).ToString("o"), note, StringComparison.Ordinal);
        Assert.Contains(Aligned.AddHours(1).ToString("o"), note, StringComparison.Ordinal);
    }

    [Fact]
    public void AlignedStart_HasNoLeadingSentence()
    {
        var note = HourlyWindowEdges.Note(Aligned, Aligned, Aligned.AddHours(4).AddMinutes(20));
        Assert.DoesNotContain("partial hour", note, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(20)]
    [InlineData(0)]
    public void End_CountsTheWholeHourPastAsOf(int minute)
    {
        var end = Aligned.AddHours(4).AddMinutes(minute);
        var note = HourlyWindowEdges.Note(Aligned, Aligned, end);
        Assert.Contains("up to " + Aligned.AddHours(5).ToString("o"), note, StringComparison.Ordinal);
    }
}
