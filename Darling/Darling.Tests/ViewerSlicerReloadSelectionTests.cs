/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Ui;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The selection a time-range slicer carries across a reload of its data (an auto-refresh, a filter change). The host loads its
/// grid for the whole window and then reloads the slicer, so a narrowed selection that survives is re-placed on the new span at
/// the same two instants and sent to the grid again; a full-window selection is not carried, and a selection the new span no longer
/// holds is dropped. Pure logic, so no WPF object is needed.
/// </summary>
public sealed class ViewerSlicerReloadSelectionTests
{
    private static readonly DateTime Day = new(2026, 9, 10, 0, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void ANarrowedSelection_IsCarriedOntoTheSameSpan_AtTheSamePositions()
    {
        var carried = SlicerReloadSelection.Carry(Day.AddHours(6), Day.AddHours(12), Day, Day.AddHours(24));

        Assert.NotNull(carried);
        Assert.Equal(0.25, carried.Value.Start, 12);
        Assert.Equal(0.5, carried.Value.End, 12);
    }

    [Fact]
    public void ANarrowedSelection_KeepsItsInstants_WhenTheSpanSlidesForward()
    {
        var carried = SlicerReloadSelection.Carry(Day.AddHours(6), Day.AddHours(12), Day.AddHours(2), Day.AddHours(26));

        Assert.NotNull(carried);
        Assert.Equal(4.0 / 24, carried.Value.Start, 12);
        Assert.Equal(10.0 / 24, carried.Value.End, 12);
    }

    [Fact]
    public void ASelectionThatStartsBeforeTheNewSpan_IsClampedToItsStart()
    {
        var carried = SlicerReloadSelection.Carry(Day.AddHours(-3), Day.AddHours(6), Day, Day.AddHours(24));

        Assert.NotNull(carried);
        Assert.Equal(0, carried.Value.Start, 12);
        Assert.Equal(0.25, carried.Value.End, 12);
    }

    [Fact]
    public void TheWholeWindow_IsNotCarried()
    {
        Assert.Null(SlicerReloadSelection.Carry(Day, Day.AddHours(24), Day, Day.AddHours(24)));
        Assert.Null(SlicerReloadSelection.Carry(Day.AddHours(-5), Day.AddHours(30), Day, Day.AddHours(24)));
    }

    [Fact]
    public void ASelectionTheNewSpanNoLongerHolds_IsDropped()
    {
        Assert.Null(SlicerReloadSelection.Carry(Day, Day.AddHours(2), Day.AddHours(6), Day.AddHours(30)));
        Assert.Null(SlicerReloadSelection.Carry(Day.AddHours(40), Day.AddHours(42), Day, Day.AddHours(24)));
    }

    [Fact]
    public void NoPreviousSelection_CarriesNothing()
    {
        Assert.Null(SlicerReloadSelection.Carry(null, null, Day, Day.AddHours(24)));
        Assert.Null(SlicerReloadSelection.Carry(Day.AddHours(6), null, Day, Day.AddHours(24)));
    }
}
