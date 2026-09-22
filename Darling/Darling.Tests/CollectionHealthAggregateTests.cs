/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using PerformanceMonitor.Darling.Storage;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The fleet collection-health rollup's off-grid registration (#3893 arm 2): what it must never take (a grid
/// minute, a compression target) and the one bound its cadence must never cross (the real-time tail reaching
/// compressed <c>collection_log</c>).
/// </summary>
public class CollectionHealthAggregateTests
{
    /// <summary>The tail a read serves from raw must stay well inside uncompressed data: if a cadence or offset
    /// change let its worst-case age reach <see cref="TimescaleSupport.CompressAfterDays"/>, every read would
    /// decompress again. "Well under" is held to a quarter of compress_after.</summary>
    [Fact]
    public void TailWorstCaseAge_StaysWellUnderCompressAfter()
    {
        var compressAfter = TimeSpan.FromDays(TimescaleSupport.CompressAfterDays);
        Assert.Equal(TimeSpan.FromHours(4), TimescaleSupport.CollectionHealthTailWorstCaseAge);
        Assert.True(TimescaleSupport.CollectionHealthTailWorstCaseAge * 4 <= compressAfter,
            $"worst-case tail {TimescaleSupport.CollectionHealthTailWorstCaseAge} is not well under compress_after {compressAfter}");
        /* The steady-state refresh window must also sit inside uncompressed rows. */
        Assert.True(TimescaleSupport.CollectionHealthRefreshStartSpan + TimescaleSupport.CollectionHealthRefreshRunAllowance < compressAfter);
    }

    [Fact]
    public void Twins_MatchTheirIntervalText()
    {
        Assert.Equal("3 hours", TimescaleSupport.CollectionHealthRefreshStartOffset);
        Assert.Equal(TimeSpan.FromHours(3), TimescaleSupport.CollectionHealthRefreshStartSpan);
        Assert.Equal("1 hour", TimescaleSupport.CollectionHealthRefreshScheduleInterval);
        Assert.Equal(TimeSpan.FromHours(1), TimescaleSupport.CollectionHealthRefreshScheduleSpan);
        Assert.Equal("8 days", TimescaleSupport.CollectionHealthRetentionInterval);
        Assert.Equal(TimeSpan.FromDays(8), TimescaleSupport.CollectionHealthRetentionSpan);
        /* Retention covers the 7-day consumer + a bucket + the refresh start span. */
        Assert.True(TimescaleSupport.CollectionHealthRetentionSpan >= TimeSpan.FromDays(7) + TimescaleSupport.HourlyBucket + TimescaleSupport.CollectionHealthRefreshStartSpan);
    }

    /// <summary>Off the grid by construction: no initial_start (finish-to-start), and on none of the lists the
    /// grid, the converges and the compression band derive from.</summary>
    [Fact]
    public void Policy_IsOffGrid_AndOnNoRegistryList()
    {
        var sql = TimescaleSupport.AddCollectionHealthRefreshPolicySql();
        Assert.DoesNotContain("initial_start", sql, StringComparison.Ordinal);
        Assert.Contains("start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour', schedule_interval => INTERVAL '1 hour'", sql, StringComparison.Ordinal);
        var view = TimescaleSupport.CollectionHealthHourlyView;
        Assert.DoesNotContain(view, TimescaleSupport.HourlyRefreshPhaseOrder);
        Assert.DoesNotContain(TimescaleSupport.HourlyAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.DailyAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.BaselineAggregates, a => a.View == view);
        Assert.DoesNotContain(TimescaleSupport.AggregateCompressionTargets, a => a.View == view);
        Assert.Single(TimescaleSupport.OffGridAggregates, a => a.View == view);
        Assert.Single(TimescaleSupport.RetentionPolicies, p => p.Relation == view);
    }
}
