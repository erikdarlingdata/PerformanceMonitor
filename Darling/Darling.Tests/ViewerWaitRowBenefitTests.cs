/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Pins <see cref="WaitRowText.Benefit"/>: the join between a wait row and the "Wait: {type}"
/// finding <c>BenefitScorer.EmitWaitStatWarnings</c> emits for it, which is what the ported wait-row
/// layout (#4576) hangs its "up to N%" Auto column on. WPF can't run a unit test on macOS, so this
/// pins the pure lookup/format logic the WPF code-behind calls, not the grid itself.
/// </summary>
public sealed class ViewerWaitRowBenefitTests
{
    [Fact]
    public void Benefit_ReturnsUpToNPercent_WhenAMatchingFindingHasAPositiveScore()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: CXPACKET", MaxBenefitPercent = 42.6 },
        };

        Assert.Equal("up to 43%", WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_MatchesCaseInsensitively_OnTheWaitType()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: cxpacket", MaxBenefitPercent = 12.0 },
        };

        Assert.Equal("up to 12%", WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_UsesAWholeNumber_AtAndAbove100()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: PAGEIOLATCH_SH", MaxBenefitPercent = 100.0 },
        };

        Assert.Equal("up to 100%", WaitRowText.Benefit("PAGEIOLATCH_SH", warnings));
    }

    [Fact]
    public void Benefit_RoundsAFractionalScore_ToTheNearestWholePercent()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: CXPACKET", MaxBenefitPercent = 0.4 },
        };

        Assert.Equal("up to 0%", WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_ReturnsNull_WhenNoFindingMatchesTheWaitType()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: WRITELOG", MaxBenefitPercent = 40.0 },
        };

        Assert.Null(WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_ReturnsNull_WhenTheMatchingFindingHasNoScore()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: CXPACKET", MaxBenefitPercent = null },
        };

        Assert.Null(WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_ReturnsNull_WhenTheMatchingFindingScoredZeroOrNegative()
    {
        var warnings = new List<PlanWarning>
        {
            new() { WarningType = "Wait: CXPACKET", MaxBenefitPercent = 0.0 },
        };

        Assert.Null(WaitRowText.Benefit("CXPACKET", warnings));
    }

    [Fact]
    public void Benefit_ReturnsNull_WhenThereAreNoStatementWarningsAtAll()
    {
        Assert.Null(WaitRowText.Benefit("CXPACKET", new List<PlanWarning>()));
    }
}
