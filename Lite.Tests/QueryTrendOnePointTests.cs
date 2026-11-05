using System;
using System.Collections.Generic;
using PerformanceMonitorLite.Mcp;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #7608: Lite's <c>get_query_trend</c> reads one row per sql_handle per collection, so one collection_time could
/// carry several points. The tool folds them to one point per collection with the same rule Darling's raw tier uses.
/// </summary>
public sealed class QueryTrendOnePointTests
{
    private static readonly DateTime T1 = new(2026, 11, 4, 10, 0, 0, DateTimeKind.Unspecified);

    private static QueryStatsHistoryRow Row(DateTime at, long executions, long cpuUs, int minDop, int maxDop, string hash) => new()
    {
        CollectionTime = at,
        DeltaExecutions = executions,
        DeltaCpuUs = cpuUs,
        DeltaElapsedUs = cpuUs * 2,
        DeltaLogicalReads = executions * 10,
        MinDop = minDop,
        MaxDop = maxDop,
        QueryPlanHash = hash,
    };

    [Fact]
    public void ThreeHandlesAtOneCollection_AreOnePoint_WithSummedDeltas_AndNoPlanHashWhenPlansDiffer()
    {
        var rows = new List<QueryStatsHistoryRow>
        {
            Row(T1, 10, 1000, 1, 4, "0xA"),
            Row(T1, 5, 500, 2, 8, "0xB"),
            Row(T1, 7, 300, 1, 2, "0xC"),
        };

        var point = Assert.Single(McpQueryTools.OnePointPerCollection(rows));

        Assert.Equal(22, point.DeltaExecutions);
        Assert.Equal(1800, point.DeltaCpuUs);
        Assert.Equal(3600, point.DeltaElapsedUs);
        Assert.Equal(220, point.DeltaLogicalReads);
        Assert.Equal(1, point.MinDop);
        Assert.Equal(8, point.MaxDop);
        Assert.Equal("", point.QueryPlanHash);
    }

    [Fact]
    public void SamePlanAcrossHandles_KeepsTheHash_AndSeparateCollectionsStaySeparate()
    {
        var rows = new List<QueryStatsHistoryRow>
        {
            Row(T1, 1, 10, 1, 1, "0xSAME"),
            Row(T1, 2, 20, 1, 2, "0xSAME"),
            Row(T1.AddMinutes(5), 4, 40, 1, 1, "0xONLY"),
        };

        var points = McpQueryTools.OnePointPerCollection(rows);

        Assert.Equal(2, points.Count);
        Assert.Equal(3, points[0].DeltaExecutions);
        Assert.Equal("0xSAME", points[0].QueryPlanHash);
        Assert.Equal(4, points[1].DeltaExecutions);
        Assert.Equal("0xONLY", points[1].QueryPlanHash);
    }
}
