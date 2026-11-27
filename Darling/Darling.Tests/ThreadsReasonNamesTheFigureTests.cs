/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #8825: the reason line names the figure that set the Threads band, in the chip's words. #8780 moved the band to
/// starved requests (Critical) and 20 or more runnable tasks (Warning), but the service's reason still said
/// "Threads low" for the runnable case.
/// </summary>
public class ThreadsReasonNamesTheFigureTests
{
    [Fact]
    public void TheServiceFleetCard_WarningFromRunnableTasks_NamesTheRunnableCount()
    {
        var card = new FleetServerCard
        {
            IsOnline = true,
            TotalThreads = 576,
            ThreadsWaitingForCpu = 47,
            RequestsWaitingForThreads = 0,
            ThreadsSeverity = ServerHealthClassifier.ThreadsSeverity(576, 47, 0),
        };

        Assert.Equal(HealthSeverity.Warning, card.ThreadsSeverity);
        var reason = DarlingFleetReader.BuildReason(card);
        Assert.Contains("Threads 47 runnable", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("Threads low", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheServiceFleetCard_StarvedRequests_StillNameTheStarvedCount()
    {
        var card = new FleetServerCard
        {
            IsOnline = true,
            TotalThreads = 576,
            ThreadsWaitingForCpu = 47,
            RequestsWaitingForThreads = 3,
            ThreadsSeverity = ServerHealthClassifier.ThreadsSeverity(576, 47, 3),
        };

        Assert.Equal(HealthSeverity.Critical, card.ThreadsSeverity);
        var reason = DarlingFleetReader.BuildReason(card);
        Assert.Contains("Threads 3 starved", reason, StringComparison.Ordinal);
        Assert.DoesNotContain("runnable", reason, StringComparison.Ordinal);
    }

    [Fact]
    public void TheViewerFleetCard_SaysTheSameWordsAsTheServiceCard()
    {
        var runnable = new ServerSummaryItem { IsOnline = true, TotalThreads = 576, ThreadsWaitingForCpu = 47 };
        Assert.Equal(HealthSeverity.Warning, runnable.ThreadsSeverity);
        Assert.Contains("Threads 47 runnable", FleetRollup.BuildReason(runnable), StringComparison.Ordinal);

        var starved = new ServerSummaryItem { IsOnline = true, TotalThreads = 576, ThreadsWaitingForCpu = 47, RequestsWaitingForThreads = 3 };
        Assert.Contains("Threads 3 starved", FleetRollup.BuildReason(starved), StringComparison.Ordinal);
    }
}
