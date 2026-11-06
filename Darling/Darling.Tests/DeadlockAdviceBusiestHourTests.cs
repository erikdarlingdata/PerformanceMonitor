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
/// #7893: the DEADLOCKS advice names the busiest hour, the figure the fact is graded on, beside the whole-window average
/// whenever that hour stands above the average. A steady rate names the average alone, and a fact carrying no peak reads
/// as it always has. The composer is shared, so Lite's <c>DeadlockPeakHourTests</c> reads the same sentence off its store.
/// </summary>
public sealed class DeadlockAdviceBusiestHourTests
{
    private static AdviceBlock Compose(double count, double perHour, double? peakHour)
    {
        var fact = new Fact { Key = "DEADLOCKS", Value = peakHour ?? perHour };
        fact.Metadata["deadlock_count"] = count;
        fact.Metadata["deadlocks_per_hour"] = perHour;
        if (peakHour is not null) fact.Metadata["peak_1h_deadlock_count"] = peakHour.Value;
        return FactAdvice.Compose("DEADLOCKS", new Dictionary<string, Fact> { ["DEADLOCKS"] = fact })!;
    }

    [Fact]
    public void AStormInsideOneHour_NamesTheBusiestHour_BesideTheAverage()
    {
        var advice = Compose(count: 20, perHour: 20.0 / 24.0, peakHour: 20);

        Assert.StartsWith("20 deadlocks this window, about 0.8 per hour, up to 20 in the busiest hour.", advice.Investigation);
    }

    [Fact]
    public void ASteadyRate_NamesTheAverageAlone()
    {
        var advice = Compose(count: 8, perHour: 2, peakHour: 2);

        Assert.StartsWith("8 deadlocks this window, about 2 per hour.", advice.Investigation);
        Assert.DoesNotContain("busiest hour", advice.Investigation);
    }

    [Fact]
    public void AFactWithoutThePeak_ReadsAsItDid()
    {
        var advice = Compose(count: 20, perHour: 20.0 / 24.0, peakHour: null);

        Assert.StartsWith("20 deadlocks this window, about 0.8 per hour.", advice.Investigation);
        Assert.DoesNotContain("busiest hour", advice.Investigation);
    }
}
