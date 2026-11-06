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
/// and a failed read measured nothing. A search narrows the totals to the matched cards' own counts, which come
/// from each card's own summary read, so the failed fleet-wide read has nothing to say about them.
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

    /// <summary>With a search on, the totals are the matched cards' own counts (#7778), and a card is in the set only
    /// when its own summary read succeeded. The fleet-wide read is not consulted, so its failure leaves the narrowed
    /// panel with real numbers, and clearing the search returns to the unavailable state.</summary>
    [Fact]
    public void BuildRollup_WithoutTotals_ASearchSumsTheMatchedCards()
    {
        var one = Card(1);
        one.BlockingCount = 5;
        one.DeadlockCount = 2;
        var two = Card(2);
        two.BlockingCount = 7;
        two.DeadlockCount = 3;
        var cards = new[] { one, two };

        var narrowed = OverviewCardView.BuildRollup(cards, totals: null, registeredCount: 2, search: "target-1");

        Assert.False(narrowed.TotalsUnavailable);
        Assert.Equal(5, narrowed.TotalBlockingEvents);
        Assert.Equal(2, narrowed.TotalDeadlocks);
        Assert.Equal("Blocking events: 5", narrowed.TotalBlockingText);
        Assert.Equal("Deadlocks: 2", narrowed.TotalDeadlocksText);
        Assert.DoesNotContain("unavailable", narrowed.DeadlockCoverageText, StringComparison.OrdinalIgnoreCase);
        Assert.Equal(2, narrowed.FilteredOfTotal);

        var cleared = OverviewCardView.BuildRollup(cards, totals: null, registeredCount: 2, search: null);

        Assert.True(cleared.TotalsUnavailable);
        Assert.Equal("Blocking events: unavailable", cleared.TotalBlockingText);
    }
}
