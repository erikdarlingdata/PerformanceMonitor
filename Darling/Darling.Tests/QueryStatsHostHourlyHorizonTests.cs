/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9684: the horizon the hourly host rollup is kept to. Where raw <c>query_stats</c> is a raw hypertable it is dropped by the gated raw purge at
/// <see cref="TimescaleSupport.RawRetentionSpan"/>, whatever the collector's retention, so the rollup is kept no longer than that. Where raw is a plain
/// table it is cleared at the collector's retention, which is the rollup's too.
/// </summary>
public sealed class QueryStatsHostHourlyHorizonTests
{
    private const int CollectorRetentionDays = 30;

    [Fact]
    public void OnATieredStore_TheHorizonIsRawsGatedSpanInWholeDays()
    {
        Assert.Equal((int)TimescaleSupport.RawRetentionSpan.TotalDays, QueryStatsHostHourly.HorizonDays(rawIsTiered: true, CollectorRetentionDays));
    }

    /// <summary>The gate, not a custom retention, decides when raw purges on a raw hypertable, so a shorter custom retention leaves raw's span.</summary>
    [Fact]
    public void OnATieredStore_AShorterCustomRetention_StillGivesRawsGatedSpan()
    {
        var gatedDays = (int)TimescaleSupport.RawRetentionSpan.TotalDays;
        var shorter = gatedDays - 2;
        Assert.True(shorter > 0);
        Assert.Equal(gatedDays, QueryStatsHostHourly.HorizonDays(rawIsTiered: true, shorter));
    }

    [Theory]
    [InlineData(30)]
    [InlineData(2)]
    public void OnAPlainStore_TheHorizonIsTheCollectorsRetention(int collectorRetentionDays)
    {
        Assert.Equal(collectorRetentionDays, QueryStatsHostHourly.HorizonDays(rawIsTiered: false, collectorRetentionDays));
    }
}
