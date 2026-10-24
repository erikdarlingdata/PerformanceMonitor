/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.Ui;
using ScottPlot;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #6652: at 1366x768 the Viewer's stacked charts get short plots. A long left-axis title was cut at both ends (the unit
/// lost) and the y axis could show only "0". The title fit and the two-tick floor are the pure rules behind the fix.
/// </summary>
public sealed class ViewerShortPlotTests
{
    // The plot height a chart row at its minimum leaves for the rotated title.
    private static double MinRowTitleSpace => ChartLayout.MinPlotHeight - ChartAxisTitle.ReservedHeight;

    public static TheoryData<string> LongTitles => new()
    {
        "Lock Wait Time (ms/sec)",
        "Total Block Duration (ms)",
        "Total Deadlock Wait (ms)",
        "Total Wait Duration (ms)",
        "Avg Wait Time (ms/wait)",
        "Read Throughput (MB/s)",
        "Write Throughput (MB/s)",
        "Pressure Events per Hour",
        "Slowest run per bucket (ms)",
        "Plan Cache Size (MB)",
    };

    [Theory]
    [MemberData(nameof(LongTitles))]
    public void LongTitle_FitsTheMinimumRowAndKeepsItsUnit(string title)
    {
        var (text, size) = ChartAxisTitle.Fit(title, MinRowTitleSpace);

        Assert.InRange(size, ChartAxisTitle.MinFontSize, ChartAxisTitle.BaseFontSize);
        Assert.True(text.Length * 0.6 * size <= MinRowTitleSpace + 0.5, $"'{text}' at {size}pt is longer than {MinRowTitleSpace}px");
        if (title.EndsWith(')'))
        {
            Assert.EndsWith(title[title.LastIndexOf('(')..], text);
        }
    }

    [Fact]
    public void ShortTitle_IsLeftAlone()
    {
        Assert.Equal(("CPU %", ChartAxisTitle.BaseFontSize), ChartAxisTitle.Fit("CPU %", MinRowTitleSpace));
    }

    [Fact]
    public void LongSourceNote_IsTrimmedNotTreatedAsAUnit()
    {
        var (text, _) = ChartAxisTitle.Fit("Blocking events (blocked-process-report)", MinRowTitleSpace);
        Assert.StartsWith("Blocking", text);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    [InlineData(double.NaN)]
    public void UnknownSpace_ReturnsTheTitleUnchanged(double available)
    {
        Assert.Equal(("Lock wait ms/s", ChartAxisTitle.BaseFontSize), ChartAxisTitle.Fit("Lock wait ms/s", available));
    }

    [Fact]
    public void AllZeroAxis_GetsAtLeastTwoLabelledTicks()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(System.Array.Empty<Tick>(), 0, 1);

        Assert.True(ticks.Count(t => t.IsMajor) >= 2);
        Assert.Contains(ticks, t => t.Label == "0");
        Assert.Contains(ticks, t => t.Label == "1");
    }

    [Fact]
    public void OnlyZeroTick_GetsTheTopOfTheAxisAdded()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(new[] { Tick.Major(0, "0") }, 0, 1.15);

        Assert.Equal(new[] { 0.0, 1.15 }, ticks.Select(t => t.Position).ToArray());
        Assert.Equal("1.15", ticks[1].Label);
    }

    [Fact]
    public void EnoughTicks_AreReturnedUntouched()
    {
        var given = new[] { Tick.Major(0, "0"), Tick.Major(50, "50"), Tick.Major(100, "100") };
        Assert.Same(given, MinimumTickGenerator.EnsureMinimum(given, 0, 105));
    }

    [Fact]
    public void ZeroWidthRange_StillGetsTwoTicks()
    {
        var ticks = MinimumTickGenerator.EnsureMinimum(System.Array.Empty<Tick>(), 0, 0);
        Assert.Equal(new[] { 0.0, 1.0 }, ticks.Select(t => t.Position).ToArray());
    }

    [Fact]
    public void MinimumPlotHeight_IsTallEnoughForTicks()
    {
        Assert.InRange(ChartLayout.MinPlotHeight, 140, 200);
    }
}
