/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7616 / #7654: the Viewer's card for a PostgreSQL target reads its Blocking from the blocking captures, with the same rule the
/// service's fleet card uses. A measured 0 shows 0, and only a target with no capture in the hour shows "--".
/// </summary>
public sealed class ViewerPostgresBlockingCardTests
{
    private static ServerSummaryItem Card(long captures, long withBlocking, bool postgres = true) => new()
    {
        DisplayName = "pg",
        IsPostgres = postgres,
        PgBlockingCapturesTotal = captures,
        PgBlockingCapturesWithBlocking = withBlocking,
        BlockingWindow = TimeSpan.FromHours(1),
    };

    [Fact]
    public void ACardWithCapturesThatSawBlocking_ShowsTheCountAndBandsOnCapturesPerHour()
    {
        var card = Card(60, 25);
        Assert.Equal("25 captures", card.BlockingDisplay);
        Assert.Equal(HealthSeverity.Critical, card.BlockingSeverity);
        Assert.Equal("25.0/hr, of 60 sampled", card.BlockingDetail);
        Assert.Equal("1 capture", Card(60, 1).BlockingDisplay);
        Assert.Equal(HealthSeverity.Warning, Card(60, 6).BlockingSeverity);
    }

    [Fact]
    public void ACardWithCapturesAndNoBlocking_ShowsAMeasuredZero()
    {
        var card = Card(60, 0);
        Assert.Equal("0 captures", card.BlockingDisplay);
        Assert.Equal(HealthSeverity.Healthy, card.BlockingSeverity);
    }

    [Fact]
    public void ACardWithNoCaptureInTheHour_IsNotMeasured_NotAZero()
    {
        var card = Card(0, 0);
        Assert.Equal("--", card.BlockingDisplay);
        Assert.Equal(HealthSeverity.Unknown, card.BlockingSeverity);
        Assert.Null(card.BlockingRatePerHour);
        Assert.Equal("no capture in the hour", card.BlockingDetail);
    }

    [Fact]
    public void ASqlServerCard_KeepsItsWaitCount()
    {
        var card = Card(0, 0, postgres: false);
        card.BlockingCount = 4;
        Assert.Equal("4", card.BlockingDisplay);
    }
}
