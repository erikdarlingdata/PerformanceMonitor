/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>The floor a size chart's Y axis keeps, so a near-flat series reads flat and a moving series keeps its own axis.</summary>
public sealed class ChartYSpanTests
{
    /// <summary>A series 0.01 MB tall around 8,190 MB gets an axis 2 % of its magnitude tall, centred on the series.</summary>
    [Fact]
    public void ANearFlatSeries_GetsAnAxisTwoPercentOfItsMagnitude_CentredOnTheSeries()
    {
        var (min, max) = ChartYSpan.Widen(8190.25, 8190.26);

        Assert.Equal(0.02 * 8190.26, max - min, precision: 6);
        Assert.Equal(8190.255, (min + max) / 2, precision: 6);
    }

    /// <summary>A series near zero gets at least 1 unit of axis, and never a negative floor.</summary>
    [Fact]
    public void ASeriesNearZero_GetsAtLeastOneUnit_AndStartsAtZero()
    {
        var (min, max) = ChartYSpan.Widen(0.10, 0.12);

        Assert.Equal(0, min);
        Assert.Equal(1.0, max, precision: 9);
    }

    /// <summary>A series that moves more than the floor is handed back as it is, so the autoscale's own axis stands.</summary>
    [Fact]
    public void AMovingSeries_IsLeftAlone()
    {
        Assert.Equal((8190.0, 9340.0), ChartYSpan.Widen(8190.0, 9340.0));
        Assert.Equal((0.0, 5.0), ChartYSpan.Widen(0.0, 5.0));
    }

    /// <summary>A single value is a flat series: it gets the floor around it.</summary>
    [Fact]
    public void ASingleValue_GetsTheFloorAroundIt()
    {
        var (min, max) = ChartYSpan.Widen(500.0, 500.0);

        Assert.Equal(10.0, max - min, precision: 9);
        Assert.Equal(500.0, (min + max) / 2, precision: 9);
    }

    /// <summary>A series below zero keeps its negative axis: only a non-negative series is slid up to zero.</summary>
    [Fact]
    public void ANegativeSeries_IsNotSlidToZero()
    {
        var (min, max) = ChartYSpan.Widen(-50.0, -50.0);

        Assert.True(min < -50.0 && max > -50.0, $"{min}..{max}");
        Assert.Equal(1.0, max - min, precision: 9);
    }
}
