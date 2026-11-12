/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using PerformanceMonitor.Analysis;
using PerformanceMonitorLite.Analysis.Recommendations;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>#7583: Lite's recommendations grid bands the scored DISK_SPACE fact Critical under the alert's critical cut, Warning above it.</summary>
public sealed class DiskFactCriticalBandTests
{
    private static LiteRecommendationSeverity Band(double freeFraction, double freeMb)
    {
        var fact = new Fact { Source = "disk", Key = "DISK_SPACE", Value = freeFraction, Metadata = new() { ["min_free_mb"] = freeMb } };
        new FactScorer().ScoreAll(new List<Fact> { fact });
        return LiteRecommendationsReader.SeverityBand(fact.BaseSeverity);
    }

    [Fact]
    public void OnePercentFree_ReadsCritical() => Assert.Equal(LiteRecommendationSeverity.Critical, Band(0.01, 100_000));

    [Fact]
    public void FourPointNinePercentFree_StillReadsWarning() => Assert.Equal(LiteRecommendationSeverity.Warning, Band(0.049, 50_000));
}
