/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: the stamp window of the column statistics sweep follows the largest configured interval, never under a day.</summary>
public sealed class ColumnStatsRetentionCutoffTests
{
    private static readonly DateTime s_now = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Unspecified);

    [Theory]
    [InlineData(null, 24 * 60)]
    [InlineData(1, 24 * 60)]
    [InlineData(60, 24 * 60)]
    [InlineData(720, 24 * 60)]
    [InlineData(721, 2 * 721)]
    [InlineData(2 * 24 * 60, 4 * 24 * 60)]
    [InlineData(7 * 24 * 60, 14 * 24 * 60)]
    public void Cutoff_IsTwiceTheInterval_WithAOneDayFloor(int? interval, int expectedWindowMinutes)
    {
        Assert.Equal(s_now.AddMinutes(-expectedWindowMinutes), DarlingRetention.ColumnStatsStaleCutoff(s_now, interval));
    }

    [Fact]
    public void Cutoff_IsNull_ForARunOnLoadCollector()
    {
        Assert.Null(DarlingRetention.ColumnStatsStaleCutoff(s_now, 0));
    }

    [Fact]
    public void LargestFrequency_TakesTheLargestOfDefaultFleetAndServerRows_AndZeroWins()
    {
        Assert.Equal(60, StoreConfigProvider.ResolveLargestFrequencyMinutes("column_stats", []));
        Assert.Equal(300, StoreConfigProvider.ResolveLargestFrequencyMinutes("column_stats", [new ScheduleOverride(null, "column_stats", 300, null, null)]));
        Assert.Equal(
            10080,
            StoreConfigProvider.ResolveLargestFrequencyMinutes(
                "column_stats", [new ScheduleOverride(null, "column_stats", 120, null, null), new ScheduleOverride(7, "column_stats", 10080, null, null)]));
        Assert.Equal(60, StoreConfigProvider.ResolveLargestFrequencyMinutes("column_stats", [new ScheduleOverride(7, "column_stats", 10, null, null)]));
        Assert.Equal(0, StoreConfigProvider.ResolveLargestFrequencyMinutes("column_stats", [new ScheduleOverride(7, "column_stats", 0, null, null)]));
        Assert.Equal(60, StoreConfigProvider.ResolveLargestFrequencyMinutes("column_stats", [new ScheduleOverride(null, "wait_stats", 0, null, null)]));
    }
}
