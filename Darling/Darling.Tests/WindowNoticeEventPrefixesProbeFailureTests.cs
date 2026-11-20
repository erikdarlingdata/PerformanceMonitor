/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// An event page builds one window notice for every length the response budget can cut it to
/// (<see cref="DarlingMcpWindowNotice.ReadEventPrefixesAsync"/>), all from one coverage probe. When that probe fails, the read
/// goes without its notice and the failure is logged once for the call, not once for every event on the page.
/// </summary>
public sealed class WindowNoticeEventPrefixesProbeFailureTests
{
    [Fact]
    public async Task AFailedCoverageProbe_IsReadOnce_AndLoggedOnce_HoweverManyEventsThePageHolds()
    {
        var logger = new CapturingTestLogger();
        var end = new DateTime(2026, 3, 4, 12, 0, 0, DateTimeKind.Utc);
        var start = end.AddHours(-4);
        var probes = 0;

        var notices = await DarlingMcpWindowNotice.ReadEventPrefixesAsync(
            () =>
            {
                probes++;
                return Task.FromException<DateTime?>(new InvalidOperationException("the coverage probe failed"));
            },
            [end.AddHours(-1), end.AddHours(-2), end.AddHours(-3)], start, end, "deadlocks", logger: logger,
            cancellationToken: TestContext.Current.CancellationToken);

        Assert.Equal(4, notices.Length);
        Assert.All(notices.Skip(1), notice => Assert.True(notice.IsUnavailable));
        Assert.Equal(1, probes);
        Assert.True(logger.CountAtLevel(LogLevel.Warning) == 1,
            $"The failed probe was logged {logger.CountAtLevel(LogLevel.Warning)} times for a page of 3 events: {logger.Joined}");
    }
}
