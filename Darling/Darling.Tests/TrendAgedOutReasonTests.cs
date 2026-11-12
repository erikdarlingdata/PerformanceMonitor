/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Darling.Storage;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7416 item 21: a trend whose first point is later than the window's start may say the earlier part aged out of the tier only when the
/// requested start is older than what that tier keeps. A query first run five hours ago, or a server that was off earlier in the day,
/// gives the same late head with nothing aged out.
/// </summary>
public sealed class TrendAgedOutReasonTests
{
    private static readonly DateTime Now = new(2026, 11, 3, 12, 0, 0, DateTimeKind.Unspecified);

    [Fact]
    public void ALateFirstPoint_WithTheStartInsideTheRawTiersRetention_IsTruncatedButNotAgedOut()
    {
        var start = Now.AddHours(-24);
        var floor = DurationTrendRouting.TierFloorUtc(RetentionTier.Raw, rawRetentionApplies: true, Now);

        Assert.Equal(Now - TimeSpan.FromDays(4), floor);
        var (effective, truncated, olderThanTier) = DurationTrendRouting.DescribeCoverage(Now.AddHours(-5), start, floor);

        Assert.Equal(Now.AddHours(-5), effective);
        Assert.True(truncated);
        Assert.False(olderThanTier);
    }

    [Fact]
    public void ALateFirstPoint_WithTheStartOlderThanTheTiersFloor_IsAgedOut_ForRawAndForHourly()
    {
        var rawStart = Now.AddDays(-6);
        var rawFloor = DurationTrendRouting.TierFloorUtc(RetentionTier.Raw, rawRetentionApplies: true, Now);
        var raw = DurationTrendRouting.DescribeCoverage(Now.AddDays(-4), rawStart, rawFloor);
        Assert.True(raw.Truncated);
        Assert.True(raw.StartOlderThanTier);
        Assert.Equal(4, DurationTrendRouting.TierKeepsDays(RetentionTier.Raw, rawRetentionApplies: true));

        var hourlyStart = Now.AddDays(-120);
        var hourlyFloor = DurationTrendRouting.TierFloorUtc(RetentionTier.Hourly, rawRetentionApplies: true, Now);
        var hourly = DurationTrendRouting.DescribeCoverage(Now.AddDays(-90), hourlyStart, hourlyFloor);
        Assert.True(hourly.Truncated);
        Assert.True(hourly.StartOlderThanTier);
        Assert.Equal(90, DurationTrendRouting.TierKeepsDays(RetentionTier.Hourly, rawRetentionApplies: true));
    }

    [Fact]
    public void AnAnswerThatIsNotTruncated_OrATierThatKeepsEverything_IsNeverAgedOut()
    {
        var floor = DurationTrendRouting.TierFloorUtc(RetentionTier.Raw, rawRetentionApplies: true, Now);

        /* The head is within the slack of the start: not truncated, so no reason is wanted even though the start is old. */
        var covered = DurationTrendRouting.DescribeCoverage(Now.AddDays(-6).AddMinutes(30), Now.AddDays(-6), floor);
        Assert.False(covered.Truncated);
        Assert.False(covered.StartOlderThanTier);

        /* A store without the extension never drops raw rows (#1664): no floor, no aged-out claim, no days to name. */
        Assert.Null(DurationTrendRouting.TierFloorUtc(RetentionTier.Raw, rawRetentionApplies: false, Now));
        Assert.Null(DurationTrendRouting.TierKeepsDays(RetentionTier.Raw, rawRetentionApplies: false));
        var noFloor = DurationTrendRouting.DescribeCoverage(Now.AddDays(-4), Now.AddDays(-6), null);
        Assert.True(noFloor.Truncated);
        Assert.False(noFloor.StartOlderThanTier);

        /* The two-argument form is unchanged. */
        Assert.Equal((Now.AddDays(-4), true), DurationTrendRouting.DescribeCoverage(Now.AddDays(-4), Now.AddDays(-6)));
    }

    [Fact]
    public void TheChartCaption_NamesAgedOutAndHowLongTheTierKeeps_OnlyForAStartOlderThanTheTier()
    {
        var head = Now.AddHours(-5);
        var points = new List<QueryTrendPoint>();

        var late = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(points, RetentionTier.Raw, head, true, 0, false, 4));
        Assert.EndsWith(" because the store holds nothing earlier in this window at this tier.", late, StringComparison.Ordinal);
        Assert.DoesNotContain("aged out", late, StringComparison.Ordinal);

        var old = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(points, RetentionTier.Raw, head, true, 0, true, 4));
        Assert.EndsWith(" because the earlier part has aged out of this tier, which keeps 4 days of data.", old, StringComparison.Ordinal);

        var oldNoDays = ViewerServerTab.DescribeTrendCoverage(new QueryTrendSeries(points, RetentionTier.Hourly, head, true, 0, true));
        Assert.EndsWith(" because the earlier part has aged out of this tier.", oldNoDays, StringComparison.Ordinal);
    }
}
