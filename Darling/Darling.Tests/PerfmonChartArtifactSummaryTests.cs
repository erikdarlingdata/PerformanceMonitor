/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// A pure unit pin for <see cref="PerfmonChartArtifactSummary.TotalArtifactsSetAside"/> (#4476): sums
/// <see cref="PerfmonTrendPoint.ArtifactsSetAside"/> across every point of every PLOTTED counter, and
/// ignores a counter present in the trend map but not plotted.
/// </summary>
public sealed class PerfmonChartArtifactSummaryTests
{
    private static PerfmonTrendPoint Point(long artifactsSetAside = 0) =>
        new(DateTime.UtcNow, 1, null, null, null, artifactsSetAside);

    [Fact]
    public void SumsAcrossEveryPointOfEveryPlottedCounter()
    {
        var trends = new Dictionary<string, List<PerfmonTrendPoint>>
        {
            ["Waits started per second"] = new() { Point(0), Point(1), Point(0) },
            ["Waits in progress"] = new() { Point(0), Point(0), Point(0) },
        };

        var total = PerfmonChartArtifactSummary.TotalArtifactsSetAside(
            trends, new[] { "Waits started per second", "Waits in progress" });

        Assert.Equal(1, total);
    }

    [Fact]
    public void IgnoresACounterNotInThePlottedSet()
    {
        var trends = new Dictionary<string, List<PerfmonTrendPoint>>
        {
            ["Plotted"] = new() { Point(2) },
            ["NotPlotted"] = new() { Point(99) },
        };

        var total = PerfmonChartArtifactSummary.TotalArtifactsSetAside(trends, new[] { "Plotted" });

        Assert.Equal(2, total);
    }

    [Fact]
    public void IsZeroWhenNothingWasSetAside()
    {
        var trends = new Dictionary<string, List<PerfmonTrendPoint>>
        {
            ["Plotted"] = new() { Point(0), Point(0) },
        };

        var total = PerfmonChartArtifactSummary.TotalArtifactsSetAside(trends, new[] { "Plotted" });

        Assert.Equal(0, total);
    }
}
