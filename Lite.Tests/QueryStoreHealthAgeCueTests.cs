/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Services;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8576: Lite's Configuration > Query Store row says how old its capture is once it is older than its collector's cadence, as the
/// Viewer's does. A readable secondary takes no new capture, so its row can be weeks old and read as current without the cue.
/// </summary>
public sealed class QueryStoreHealthAgeCueTests
{
    private static QueryStoreHealthRow Row(int intervalMinutes) => new()
    {
        CaptureTime = new DateTime(2026, 10, 20, 12, 0, 0, DateTimeKind.Utc),
        DatabaseName = "db",
        CollectorIntervalMinutes = intervalMinutes,
    };

    [Fact]
    public void ARowOlderThanItsCollectorsCadence_SaysHowOldItIs()
    {
        var text = Row(60).CollectedText(new DateTime(2026, 11, 10, 9, 0, 0, DateTimeKind.Utc));

        Assert.EndsWith("(20 days 21 hours old)", text, StringComparison.Ordinal);
    }

    [Fact]
    public void ARowWithinItsCollectorsCadence_OrWithNoKnownCadence_SaysNothingOfAge()
    {
        var row = Row(60);

        Assert.DoesNotContain("old", row.CollectedText(row.CaptureTime.AddMinutes(90)), StringComparison.Ordinal);
        Assert.DoesNotContain("old", Row(0).CollectedText(new DateTime(2026, 11, 10, 9, 0, 0, DateTimeKind.Utc)), StringComparison.Ordinal);
    }
}
