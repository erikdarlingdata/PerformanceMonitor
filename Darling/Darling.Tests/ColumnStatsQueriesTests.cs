/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Collectors;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8798: the build boundaries of the STAT_HEADER shape. The T-SQL itself is exercised against a real server in ColumnStatsEndToEndLiveTests;
/// its wording is not pinned here (.github/REVIEW-TRAPS.md).
/// </summary>
public sealed class ColumnStatsQueriesTests
{
    [Theory]
    [InlineData(13, 0, 4445, false)]
    [InlineData(13, 0, 4446, true)]
    [InlineData(13, 0, 5000, true)]
    [InlineData(13, 0, 2000, false)]
    [InlineData(12, 0, 6000, false)]
    [InlineData(14, 0, 3005, false)]
    [InlineData(14, 0, 3006, true)]
    [InlineData(14, 0, 1000, false)]
    [InlineData(15, 0, 2000, true)]
    [InlineData(16, 0, 1000, true)]
    [InlineData(17, 0, 1, true)]
    public void HeaderHasPersistedSamplePercent_Pins(int major, int minor, int build, bool expected)
    {
        Assert.Equal(expected, ColumnStatsQueries.HeaderHasPersistedSamplePercent(major, minor, build));
    }
}
