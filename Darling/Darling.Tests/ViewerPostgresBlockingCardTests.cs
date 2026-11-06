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
    public void ACardWithFewerThanThreeCaptures_IsUnknownWithTheSameDetailAsTheServiceCard()
    {
        /* Registered a minute ago: 1 of 1 captures saw blocking. It shows its real count, but 1 of 1 scaled to 60 an hour is not a Critical claim. */
        var card = Card(1, 1);
        Assert.Equal("1 capture", card.BlockingDisplay);
        Assert.Equal(HealthSeverity.Unknown, card.BlockingSeverity);
        Assert.Null(card.BlockingRatePerHour);
        Assert.Equal(ServerHealthClassifier.PostgresBlockingTooFewCapturesDetail, card.BlockingDetail);
        Assert.Equal("fewer than 3 captures in this window", card.BlockingDetail);
        Assert.Equal(HealthSeverity.Unknown, System.Linq.Enumerable.ElementAt(ServerHealthClassifier.MetricSeverities(card.ToHealthMetrics()), 3));
    }

    [Fact]
    public void ASlowCadenceCard_BandsOnTheShareOfCapturesTaken_NotTheNominalHour()
    {
        var warning = Card(12, 3);
        Assert.Equal(HealthSeverity.Warning, warning.BlockingSeverity);
        Assert.Equal(15.0, warning.BlockingRatePerHour);
        Assert.Equal("15.0/hr, of 12 sampled", warning.BlockingDetail);

        var critical = Card(12, 12);
        Assert.Equal(HealthSeverity.Critical, critical.BlockingSeverity);
        Assert.Equal(60.0, critical.BlockingRatePerHour);
        Assert.Equal(HealthSeverity.Healthy, Card(59, 2).BlockingSeverity);
    }

    [Fact]
    public void TheFleetBlockingTotal_NamesBothUnitsWhenTheFleetHasBothEngines()
    {
        var totals = new FleetTotals { TotalBlockingEvents = 31 };
        var both = FleetRollup.Build([Card(60, 25), Card(0, 0, postgres: false)], totals);
        Assert.Contains("SQL Server blocked waits", both.TotalBlockingDetail, System.StringComparison.Ordinal);
        Assert.Contains("PostgreSQL blocking captures", both.TotalBlockingDetail, System.StringComparison.Ordinal);
        Assert.Equal("blocking captures", FleetRollup.Build([Card(60, 25)], totals).TotalBlockingDetail);
        Assert.Equal("blocked waits", FleetRollup.Build([Card(0, 0, postgres: false)], totals).TotalBlockingDetail);
    }

    [Fact]
    public void ASqlServerCard_KeepsItsWaitCount()
    {
        var card = Card(0, 0, postgres: false);
        card.BlockingCount = 4;
        Assert.Equal("4", card.BlockingDisplay);
    }
}
