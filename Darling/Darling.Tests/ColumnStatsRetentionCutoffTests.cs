/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Collectors;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>#8798: the stamp window of the column statistics sweep is the schema histories' rule: the collector's retention horizon.</summary>
public sealed class ColumnStatsRetentionCutoffTests
{
    private static readonly DateTime s_now = new(2026, 11, 17, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void Cutoff_IsTheShippedRetention_WhenNothingIsConfigured()
    {
        Assert.Equal(s_now.AddDays(-CollectorScheduleDefaults.All["column_stats"].RetentionDays), DarlingRetention.ColumnStatsStaleCutoff(s_now, null));
    }

    [Theory]
    [InlineData(7, 7)]
    [InlineData(90, 90)]
    [InlineData(0, 1)]
    [InlineData(-5, 1)]
    public void Cutoff_FollowsTheConfiguredRetention_NeverUnderADay(int configuredDays, int expectedDays)
    {
        Assert.Equal(
            s_now.AddDays(-expectedDays),
            DarlingRetention.ColumnStatsStaleCutoff(s_now, name => name == "column_stats" ? configuredDays : 30));
    }
}
