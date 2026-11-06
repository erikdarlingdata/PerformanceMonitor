/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The Overview's "Last hour" totals come from one cross-server read. When that read fails the roll-up must say
/// so rather than render zeros beside a coverage line that claims every server was read: a zero is a measurement,
/// and a failed read measured nothing.
/// </summary>
public sealed class ViewerFleetTotalsUnavailableTests
{
    private static ServerSummaryItem Card(int id) =>
        new() { ServerId = id, ServerName = $"target-{id}", DisplayName = $"target-{id}" };

    [Fact]
    public void BuildRollup_WithoutTotals_SaysTheTotalsAreUnavailable()
    {
        var rollup = OverviewCardView.BuildRollup(new[] { Card(1), Card(2) }, totals: null, registeredCount: 2, search: null);

        Assert.True(rollup.TotalsUnavailable);
        Assert.Equal("Blocking events: unavailable", rollup.TotalBlockingText);
        Assert.Equal("Deadlocks: unavailable", rollup.TotalDeadlocksText);
        Assert.Contains("unavailable", rollup.DeadlockCoverageText, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("read all", rollup.DeadlockCoverageText, StringComparison.OrdinalIgnoreCase);
        Assert.True(rollup.DeadlockCoverageIsPartial);
    }

    [Fact]
    public void BuildRollup_WithTotals_FormatsTheCounts()
    {
        var totals = new FleetTotals { TotalBlockingEvents = 12, TotalDeadlocks = 3 };

        var rollup = OverviewCardView.BuildRollup(new[] { Card(1), Card(2) }, totals, registeredCount: 2, search: null);

        Assert.False(rollup.TotalsUnavailable);
        Assert.Equal("Blocking events: 12", rollup.TotalBlockingText);
        Assert.Equal("Deadlocks: 3", rollup.TotalDeadlocksText);
        Assert.DoesNotContain("unavailable", rollup.DeadlockCoverageText, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildRollup_WithoutTotals_AndNoCards_StillSaysUnavailable()
    {
        var rollup = OverviewCardView.BuildRollup(Array.Empty<ServerSummaryItem>(), totals: null, registeredCount: 3, search: null);

        Assert.True(rollup.TotalsUnavailable);
        Assert.Equal("Blocking events: unavailable", rollup.TotalBlockingText);
        Assert.Equal("Deadlocks: unavailable", rollup.TotalDeadlocksText);
    }

    [Fact]
    public void BuildRollup_WithoutTotals_ASearchKeepsTheUnavailableState()
    {
        var rollup = OverviewCardView.BuildRollup(new[] { Card(1), Card(2) }, totals: null, registeredCount: 2, search: "target-1");

        Assert.True(rollup.TotalsUnavailable);
        Assert.Equal(2, rollup.FilteredOfTotal);
    }
}
