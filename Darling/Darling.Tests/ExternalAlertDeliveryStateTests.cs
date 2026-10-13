/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.Json;
using System.Text.RegularExpressions;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The pieces of the external alert drain that need no store (#5654): the history pattern the reap matches a queue row by,
/// and how the history grids treat an external alert's stored value.
/// </summary>
public sealed class ExternalAlertDeliveryStateTests
{
    /// <summary>The SQL <c>LIKE</c> match for a pattern with only <c>%</c> wildcards (what the helper builds needs no escape).</summary>
    private static bool Like(string text, string pattern) =>
        Regex.IsMatch(text, "^" + string.Join(".*", pattern.Split('%').Select(Regex.Escape)) + "$", RegexOptions.Singleline);

    [Fact]
    public void ThePendingIdPattern_MatchesWhatARealExternalDtoSerializes_AndAnchorsOnTheClosingBrace()
    {
        var json = JsonSerializer.Serialize(new AlertExternalDto("nagios", "disk", 12));
        var pattern = AlertContextSerializer.BuildPendingIdLikePattern(12);

        Assert.True(Like(json, pattern));
        Assert.False(Like(JsonSerializer.Serialize(new AlertExternalDto("nagios", "disk", 123)), pattern));
        Assert.False(Like(JsonSerializer.Serialize(new AlertExternalDto("nagios", "disk", 112)), pattern));

        /* The row the drain really writes: the DTO nested in a whole serialized context. */
        var context = AlertContextSerializer.Serialize(new AlertContext { External = new AlertExternalOrigin("nagios", "disk", 12) });
        Assert.True(Like(context, pattern));
        Assert.False(Like(context, AlertContextSerializer.BuildPendingIdLikePattern(1)));
    }

    [Fact]
    public void ThePendingIdPattern_StopsMatching_WhenPendingIdIsNotTheLastMemberOfTheDto()
    {
        Assert.Equal("PendingId", typeof(AlertExternalDto).GetProperties().Last().Name);

        /* The same three members with PendingId first: the closing-brace anchor no longer finds it. */
        var reordered = JsonSerializer.Serialize(new { PendingId = 12L, Source = "nagios", Key = "disk" });
        Assert.False(Like(reordered, AlertContextSerializer.BuildPendingIdLikePattern(12)));
    }

    [Fact]
    public void AnExternalMetric_IsStateOnly_SoAZeroShowsAsADash_AndANonZeroValueStaysANumber()
    {
        var metric = AlertFamily.ExternalMetricPrefix + "nagios:disk";

        Assert.StartsWith("External:", metric, StringComparison.Ordinal);
        Assert.True(AlertMetricClassifier.IsStateOnly(metric));
        Assert.Equal(AlertMetricClassifier.StateOnlyDisplay, AlertMetricClassifier.FormatHistoryValue(metric, 0));
        Assert.Equal("91.50", AlertMetricClassifier.FormatHistoryValue(metric, 91.5));
        Assert.False(AlertMetricClassifier.IsStateOnly("High CPU"));
        Assert.False(AlertMetricClassifier.IsStateOnly("Externally Hosted"));
    }
}
