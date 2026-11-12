/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7897: the ANOMALY_OBJECT_GROWTH card for a table that grew from empty says "from empty" and prints no percent, because a
/// percent of nothing has no value. A table with a prior size keeps its percent. <see cref="FactAdvice.Compose"/> is shared by
/// both apps, so the words are pinned once, here.
/// </summary>
public sealed class ObjectGrowthFromEmptyAdviceTests
{
    private static Dictionary<string, Fact> Growth(double priorMb, double currentMb, double? growthPct)
    {
        var metadata = new Dictionary<string, double>
        {
            ["prior_mb"] = priorMb,
            ["current_mb"] = currentMb,
            ["growth_mb"] = currentMb - priorMb,
            ["growth_ratio"] = 5,
        };
        if (growthPct is not null)
            metadata["growth_pct"] = growthPct.Value;
        var fact = new Fact
        {
            Source = "anomaly",
            Key = "ANOMALY_OBJECT_GROWTH",
            Value = currentMb - priorMb,
            DatabaseName = "AppDb",
            ObjectName = "dbo.Staging",
            Metadata = metadata,
            BaseSeverity = 0.8,
            Severity = 0.8,
        };
        return new Dictionary<string, Fact> { [fact.Key] = fact };
    }

    [Fact]
    public void FromEmpty_SaysFromEmpty_AndPrintsNoPercent()
    {
        var advice = FactAdvice.Compose("ANOMALY_OBJECT_GROWTH", Growth(priorMb: 0, currentMb: 500, growthPct: null))!;

        Assert.Contains("from empty", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("500 MB", advice.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("%", advice.Headline, StringComparison.Ordinal);
        Assert.Contains("from empty", advice.Investigation, StringComparison.Ordinal);
        Assert.DoesNotContain("%", advice.Investigation, StringComparison.Ordinal);
    }

    [Fact]
    public void WithAPriorSize_KeepsThePercent()
    {
        var advice = FactAdvice.Compose("ANOMALY_OBJECT_GROWTH", Growth(priorMb: 200, currentMb: 600, growthPct: 200))!;

        Assert.Contains("(+200%)", advice.Headline, StringComparison.Ordinal);
        Assert.DoesNotContain("from empty", advice.Headline, StringComparison.Ordinal);
    }
}
