/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4223: the three report alerts get a deliberate "no notebook" — the Fleet Sweep Rollup's link goes to
/// the SPA's sweep timeline (<c>#/sweeps</c>) instead of a triage page, and the two digests ("Collector Cost
/// Digest", "Analysis Singles Digest") carry no link at all, because a scheduled document names no single
/// incident for a triage page to anchor on. A non-report metric keeps its full triage URL unchanged.
/// </summary>
public class TriageLinkReportsTests
{
    private static readonly DateTime Fired = new(2026, 9, 26, 12, 0, 0, DateTimeKind.Utc);

    [Fact]
    public void Build_RoutesTheFleetSweepRollup_ToTheSweepsPage_WithNoQuery()
    {
        var url = TriageLink.Build("http://box:5153", "srv", "Fleet Sweep Rollup", Fired, "dedup-key");

        Assert.Equal("http://box:5153/#/sweeps", url);
    }

    [Theory]
    [InlineData("Collector Cost Digest")]
    [InlineData("Analysis Singles Digest")]
    public void Build_ReturnsNull_ForEitherDigest(string metricName)
    {
        var url = TriageLink.Build("http://box:5153", "srv", metricName, Fired, "dedup-key");

        Assert.Null(url);
    }

    [Fact]
    public void Build_KeepsTheFullTriageUrl_ForANonReportMetric()
    {
        var url = TriageLink.Build("http://box:5153", "srv", "Blocking Detected", Fired, "dedup-key");

        Assert.Equal(
            "http://box:5153/#/triage?server=srv&metric=Blocking%20Detected&at=2026-09-26T12%3A00%3A00Z&dedup=dedup-key",
            url);
    }

    [Theory]
    [InlineData("Fleet Sweep Rollup")]
    [InlineData("Collector Cost Digest")]
    [InlineData("Analysis Singles Digest")]
    [InlineData("Blocking Detected")]
    public void Build_ReturnsNull_WhenTheBaseUrlIsEmpty_RegardlessOfMetric(string metricName)
    {
        Assert.Null(TriageLink.Build("", "srv", metricName, Fired, "dedup-key"));
        Assert.Null(TriageLink.Build(null, "srv", metricName, Fired, "dedup-key"));
    }

    [Fact]
    public void LinkLabel_NamesTheSweepTimeline_ForTheSweepsRoute()
    {
        var url = TriageLink.Build("http://box:5153", "srv", "Fleet Sweep Rollup", Fired);

        Assert.Equal("Open sweep timeline", TriageLink.LinkLabel(url));
    }

    [Fact]
    public void LinkLabel_NamesTheTriagePage_ForEveryOtherLink()
    {
        var url = TriageLink.Build("http://box:5153", "srv", "Blocking Detected", Fired);

        Assert.Equal("Open triage page", TriageLink.LinkLabel(url));
    }
}
