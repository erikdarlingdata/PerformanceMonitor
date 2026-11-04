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
/// #7583: the DISK_SPACE fact scored a flat 1.0 (Warning) under 5% free, so a volume at 1% free read the same as
/// one at 4.9% while the low disk alert called the first Critical. Under the alert's critical cut (3 percent or
/// 2 GB, either one) the fact now scores 1.5, the Critical band; above the cut it is unchanged.
/// </summary>
public sealed class DiskFactCriticalScoreTests
{
    private const double CriticalBand = 1.5;

    private static double Score(double freeFraction, double? freeMb)
    {
        var meta = new Dictionary<string, double> { ["min_free_pct"] = freeFraction };
        if (freeMb is { } mb) meta["min_free_mb"] = mb;
        var fact = new Fact { Source = "disk", Key = "DISK_SPACE", Value = freeFraction, Metadata = meta };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        return fact.BaseSeverity;
    }

    [Fact]
    public void OnePercentFree_IsCritical()
    {
        Assert.True(Score(0.01, 100_000) >= CriticalBand);
    }

    [Fact]
    public void FortyEightMbOnALargeVolume_IsCritical()
    {
        // The reported shape: 47.59 MB free, 0.0097 of the volume.
        Assert.True(Score(0.0097, 47.59) >= CriticalBand);
    }

    [Fact]
    public void ThreePercentExactly_IsCritical_AsTheAlertGradesIt()
    {
        Assert.True(Score(0.03, 300_000) >= CriticalBand);
    }

    [Fact]
    public void UnderTwoGbFree_IsCritical_EvenWhenThePercentIsAboveTheCut()
    {
        // 1.5 GB on a 20 GB volume is 7.5% free, above the percent cut, under the GB one.
        Assert.True(Score(0.075, 1536) >= CriticalBand);
    }

    [Fact]
    public void JustAboveTheCut_StaysAWarning()
    {
        // 3.1% of a 1 TB volume is about 32 GB free: above both floors, under 5%.
        Assert.Equal(1.0, Score(0.031, 32_000));
        Assert.Equal(1.0, Score(0.049, 50_000));
    }

    [Fact]
    public void AboveFivePercent_EasesDownAsBefore()
    {
        Assert.Equal(0.75, Score(0.075, 80_000), precision: 6);
        Assert.Equal(0.0, Score(0.20, 200_000));
    }

    [Fact]
    public void AFactWithoutFreeMb_GradesOnThePercentAlone()
    {
        Assert.Equal(1.0, Score(0.04, null));
        Assert.True(Score(0.02, null) >= CriticalBand);
    }

    [Fact]
    public void AmplifiersStillAddOnTopOfTheCriticalBase_CappedAtTwo()
    {
        var disk = new Fact { Source = "disk", Key = "DISK_SPACE", Value = 0.01, Metadata = new() { ["min_free_mb"] = 500 } };
        new FactScorer().ScoreAll(new List<Fact> { disk });
        Assert.InRange(disk.Severity, CriticalBand, 2.0);
    }
}
