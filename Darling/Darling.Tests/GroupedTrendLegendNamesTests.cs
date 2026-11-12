/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8186: no two series in a Latches or Spinlocks legend read the same. The latch classes ACCESS_METHODS_HOBT_COUNT,
/// ACCESS_METHODS_HOBT_VIRTUAL_ROOT and ACCESS_METHODS_HOBT_FACTORY share their first twenty characters, so a legend that cuts a name
/// at a fixed head loses the only part that tells them apart. The shared renderer is driven on the shared STA thread against a real
/// chart and the legend text of each series is read back from the plot.
/// </summary>
public sealed class GroupedTrendLegendNamesTests
{
    private static readonly DateTime SampleUtc = new(2026, 1, 15, 12, 0, 0, DateTimeKind.Utc);

    /// <summary>Renders one series per name, each with one point, and returns the legend text of every series in legend order.</summary>
    private static List<string> LegendTexts(params string[] names)
        => StaTestThread.Run(() =>
        {
            var chart = new ScottPlot.WPF.WpfPlot();
            var data = names.Select((name, i) => (Name: name, Time: SampleUtc.AddMinutes(i))).ToList();
            new GroupedTrendChartRenderer(new ChartRenderHelper(), t => t)
                .Render(chart, null, data, d => d.Time, d => d.Name, _ => 4.0, "empty", "Y", SampleUtc.AddHours(-1).ToOADate(), SampleUtc.AddHours(1).ToOADate());
            return chart.Plot.GetPlottables().OfType<ScottPlot.Plottables.Scatter>().Select(s => s.LegendText).ToList();
        });

    [Fact]
    public void TheThreeHobtLatchClasses_ReadDifferently_AndEachKeepsItsTail()
    {
        var texts = LegendTexts("ACCESS_METHODS_HOBT_COUNT", "ACCESS_METHODS_HOBT_VIRTUAL_ROOT", "ACCESS_METHODS_HOBT_FACTORY");

        Assert.Equal(3, texts.Count);
        Assert.True(texts.Distinct(StringComparer.Ordinal).Count() == 3, "the three HOBT latch classes read the same in the legend: " + string.Join(" | ", texts));
        Assert.Contains(texts, t => t.EndsWith("COUNT", StringComparison.Ordinal));
        Assert.Contains(texts, t => t.EndsWith("VIRTUAL_ROOT", StringComparison.Ordinal));
        Assert.Contains(texts, t => t.EndsWith("FACTORY", StringComparison.Ordinal));
    }

    [Fact]
    public void TwoLongNamesThatShareALongPrefix_StillReadDifferently_AndKeepTheirTails()
    {
        var texts = LegendTexts("SERVICE_BROKER_TRANSMISSION_WORKTABLE_ALPHA", "SERVICE_BROKER_TRANSMISSION_WORKTABLE_BETA");

        Assert.Equal(2, texts.Count);
        Assert.True(texts.Distinct(StringComparer.Ordinal).Count() == 2, "two long names read the same in the legend: " + string.Join(" | ", texts));
        Assert.Contains(texts, t => t.EndsWith("ALPHA", StringComparison.Ordinal));
        Assert.Contains(texts, t => t.EndsWith("BETA", StringComparison.Ordinal));
    }

    [Fact]
    public void AShortName_IsKeptWhole()
        => Assert.Equal("LOCK_HASH", Assert.Single(LegendTexts("LOCK_HASH")));
}
