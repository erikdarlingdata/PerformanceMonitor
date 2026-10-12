using System;
using PerformanceMonitor.Darling.Analysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// The two blocking-chain reads window on the report's own time like their siblings, so they also carry the
/// partition-column floor on <c>collection_time</c> (no upper side: a late-collected report still counts). Without
/// it every retained chunk of <c>blocked_process_reports</c> is opened to answer a window.
/// </summary>
public sealed class BlockingChainReadsFloorTests
{
    [Theory]
    [InlineData("fact")]
    [InlineData("drill-down")]
    public void ChainReads_CarryTheCollectionTimeFloor_AndNoUpperCollectionTimeBound(string read)
    {
        var sql = read == "fact" ? PgFactCollector.BlockingChainSql : PgDrillDownCollector.ReconstructedChainsSql;

        Assert.Contains("event_time >= $2", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("collection_time <", sql, StringComparison.Ordinal);
    }
}
