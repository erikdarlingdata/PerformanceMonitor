/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The elapsed time an Active Queries row shows drops the <c>00 </c> day count the collector stores ahead of the clock, and keeps a day count
/// of one or more, so a request a few minutes old reads as a clock and one a day old says so.
/// </summary>
public sealed class ElapsedTimeTextTests
{
    [Theory]
    [InlineData("00 00:05:12.345", "00:05:12.345")]
    [InlineData("00 23:59:59.999", "23:59:59.999")]
    [InlineData("01 02:03:04.005", "01 02:03:04.005")]
    [InlineData("12 00:00:00.000", "12 00:00:00.000")]
    [InlineData("00:05:12.345", "00:05:12.345")]
    [InlineData("00 ", "00 ")]
    [InlineData("", "")]
    [InlineData(null, "")]
    public void TheGridsElapsedTime_DropsAZeroDayCount_AndKeepsTheRest(string? stored, string shown)
    {
        Assert.Equal(shown, ElapsedTimeText.WithoutZeroDays(stored));
    }
}
