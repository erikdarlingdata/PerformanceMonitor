/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Controls;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// The Queries tab resolves a live range to two instants once per refresh pass and hands that one pair to the grid read and the
/// strip load, so the strip's window is the grid's window whatever the clock says when each of them runs. These pin the pieces:
/// the resolution itself, and that the two reads return the pair they are handed, not a pair of their own.
/// </summary>
public sealed class QueriesWindowResolutionTests
{
    private static readonly DateTime Now = new(2026, 11, 8, 12, 37, 20, DateTimeKind.Utc);

    [Fact]
    public void ALiveRange_ResolvesOnceToTheInstantsOfThePass()
    {
        var (start, end) = ServerTab.QueriesWindowUtc(24, null, null, Now);

        Assert.Equal(Now.AddHours(-24), start);
        Assert.Equal(Now, end);
    }

    [Fact]
    public void AFinishedRange_IsItsOwnTwoInstants()
    {
        var from = new DateTime(2026, 11, 1, 0, 0, 0, DateTimeKind.Utc);
        var to = new DateTime(2026, 11, 2, 0, 0, 0, DateTimeKind.Utc);

        Assert.Equal((from, to), ServerTab.QueriesWindowUtc(0, from, to, Now));
    }

    [Fact]
    public void TheGridReadAndTheStripLoad_ReturnThePairTheyAreHanded_WhateverTheirOwnClockSays()
    {
        var (start, end) = ServerTab.QueriesWindowUtc(24, null, null, Now);
        var later = Now.AddSeconds(7);

        Assert.Equal((start, end), LocalDataService.GetTimeRange(0, start, end, later));
        Assert.Equal((start, end), TimeWindows.ChartAxis(0, start, end, later));
        Assert.Equal((start, end), LocalDataService.GetQueriesTabWindowUtc(0, start, end));
    }
}
