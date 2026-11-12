/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitorLite.Controls;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Overview lanes before their first read has come back. A tab opened on a server whose first collection is running waits for the
/// store, and the lanes meanwhile say they are loading, not that the window holds no data: the store may well hold the previous hour's
/// samples, which the read returns once it runs. The "No ... data in this window" sentence is the empty read's, drawn only once a read
/// has come back empty.
/// </summary>
public sealed class OverviewLanesLoadingTextTests
{
    [Fact]
    public void BeforeTheFirstRead_EachLaneSaysItIsLoading_NotThatTheWindowHoldsNoData()
    {
        var sentences = StaTestThread.Run(() =>
        {
            var control = new CorrelatedTimelineLanesControl();
            control.Initialize(null!, 1, () => TimeZoneInfo.Utc);
            return new[] { control.CpuChart, control.WaitStatsChart, control.BlockingChart, control.MemoryChart, control.FileIoChart }
                .Select(chart => string.Join(" | ", chart.Plot.GetPlottables<ScottPlot.Plottables.Text>().Select(t => t.LabelText)))
                .ToArray();
        });

        Assert.All(sentences, sentence => Assert.False(sentence.Contains("No ", StringComparison.Ordinal) && sentence.Contains("in this window", StringComparison.Ordinal),
            $"a lane that has not read yet claims the window is empty: '{sentence}'"));
        Assert.All(sentences, sentence => Assert.StartsWith("Loading ", sentence, StringComparison.Ordinal));
        Assert.Contains("SQL CPU", sentences[0], StringComparison.Ordinal);
        Assert.Contains("Wait Stats", sentences[1], StringComparison.Ordinal);
        Assert.Contains("Blocking", sentences[2], StringComparison.Ordinal);
    }
}
