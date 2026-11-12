/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7884: QUERY_SPILLS is graded on spilled pages per OBSERVED hour, so the same spill rate scores the same at every
/// hours_back, and its bars sit at the fleet tail: warning at 57,000 pages an hour, critical at 173,000. The two
/// amplifiers that take the fact (DISK_SPACE, MEMORY_GRANT_PENDING) match only once it clears the root bar, so a
/// handful of spilled pages lends no boost.
/// </summary>
public sealed class QuerySpillsPerHourScoreTests
{
    private const double WarningPagesPerHour = 57_000;
    private const double CriticalPagesPerHour = 173_000;
    private const double RootBar = 0.5;
    private const double HourMs = 3_600_000;

    /// <summary>A collected QUERY_SPILLS fact: the window's page total, with the period and coverage the producers stamp.</summary>
    private static Fact Spills(double pages, double hours, double coverage = 1.0) =>
        new()
        {
            Source = "queries",
            Key = "QUERY_SPILLS",
            Value = pages,
            Metadata = new Dictionary<string, double>
            {
                ["total_spills"] = pages,
                ["period_duration_ms"] = hours * HourMs,
                ["coverage_fraction"] = coverage,
            }
        };

    private static double Base(Fact fact)
    {
        new FactScorer().ScoreAll(new List<Fact> { fact });
        return fact.BaseSeverity;
    }

    [Theory]
    [InlineData(200)]       // under the warning bar
    [InlineData(100_000)]   // inside the warning ramp
    [InlineData(500_000)]   // past critical
    public void TheSameRate_ScoresTheSame_At4And24Hours(double pagesPerHour)
    {
        var fourHours = Base(Spills(pagesPerHour * 4, hours: 4));
        var day = Base(Spills(pagesPerHour * 24, hours: 24));

        Assert.Equal(fourHours, day, precision: 9);
    }

    [Fact]
    public void ARateInsideTheRamp_SitsBetweenTheRootBarAndSaturation_AtBothWindows()
    {
        var fourHours = Base(Spills(100_000 * 4, hours: 4));
        var day = Base(Spills(100_000 * 24, hours: 24));

        Assert.InRange(fourHours, RootBar + 0.01, 0.99);
        Assert.Equal(fourHours, day, precision: 9);
    }

    [Fact]
    public void HalfCoverage_DividesByObservedHours_NotTheWallWindow()
    {
        // 1,200,000 pages over a 24 h window the collector covered for 12 h is 100,000 pages per observed hour,
        // the same rate as 400,000 pages in a fully covered 4 h window.
        var partDay = Base(Spills(1_200_000, hours: 24, coverage: 0.5));
        var fourHours = Base(Spills(400_000, hours: 4));

        Assert.Equal(fourHours, partDay, precision: 9);
    }

    [Fact]
    public void JustUnderTheWarningBar_StaysUnderTheRootBar()
    {
        Assert.True(Base(Spills(WarningPagesPerHour - 1, hours: 1)) < RootBar);
    }

    [Fact]
    public void AtTheWarningBar_ScoresTheRootBar()
    {
        Assert.Equal(RootBar, Base(Spills(WarningPagesPerHour * 4, hours: 4)), precision: 9);
    }

    [Fact]
    public void JustUnderTheCriticalBar_DoesNotSaturate()
    {
        Assert.True(Base(Spills((CriticalPagesPerHour - 1) * 4, hours: 4)) < 1.0);
    }

    [Fact]
    public void AtTheCriticalBar_Saturates()
    {
        Assert.Equal(1.0, Base(Spills(CriticalPagesPerHour * 4, hours: 4)), precision: 9);
    }

    [Fact]
    public void AFactWithNoPeriod_ReadsAsOneHour()
    {
        var bare = new Fact { Source = "queries", Key = "QUERY_SPILLS", Value = WarningPagesPerHour };

        Assert.Equal(RootBar, Base(bare), precision: 9);
    }

    private static Fact CriticalDisk() =>
        new() { Source = "disk", Key = "DISK_SPACE", Value = 0.01, Metadata = new() { ["min_free_mb"] = 500 } };

    private static Fact GrantPending() =>
        new() { Source = "memory", Key = "MEMORY_GRANT_PENDING", Value = 5, Metadata = new() { ["max_waiters"] = 5 } };

    [Fact]
    public void OneSpilledPage_LendsNoBoostToDiskSpace()
    {
        var disk = CriticalDisk();
        var spills = Spills(1, hours: 4);
        new FactScorer().ScoreAll(new List<Fact> { disk, spills });

        Assert.True(disk.BaseSeverity > 0);
        Assert.Equal(disk.BaseSeverity, disk.Severity, precision: 9);
    }

    [Fact]
    public void OneSpilledPage_LendsNoBoostToMemoryGrantPending()
    {
        var grant = GrantPending();
        var spills = Spills(1, hours: 4);
        new FactScorer().ScoreAll(new List<Fact> { grant, spills });

        Assert.True(grant.BaseSeverity > 0);
        Assert.Equal(grant.BaseSeverity, grant.Severity, precision: 9);
    }

    [Fact]
    public void SpillsAtTheWarningBar_StillBoostDiskSpace()
    {
        var disk = CriticalDisk();
        var spills = Spills(WarningPagesPerHour * 4, hours: 4);
        new FactScorer().ScoreAll(new List<Fact> { disk, spills });

        Assert.Equal(disk.BaseSeverity * 1.2, disk.Severity, precision: 9);
    }

    [Fact]
    public void Advice_StatesThePagesPerObservedHour_AndBothBars()
    {
        // 460,000 pages over a fully covered 4 h window is 115,000 pages an hour.
        var fact = Spills(460_000, hours: 4);
        var advice = FactAdvice.Compose("QUERY_SPILLS", new Dictionary<string, Fact> { ["QUERY_SPILLS"] = fact })!;

        Assert.Contains("460,000 pages (3,594 MB) spilled to tempdb this window", advice.Investigation);
        Assert.Contains("115,000 pages (898 MB) per observed hour", advice.Investigation);
        Assert.Contains("57,000 pages (445 MB) an hour", advice.Investigation);
        Assert.Contains("173,000 pages (1,352 MB)", advice.Investigation);
    }
}
