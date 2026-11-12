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
        Assert.Equal(3, point.PlanCount);
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
        Assert.Equal(1, points[0].PlanCount);
        Assert.Equal(1, points[1].PlanCount);
    }

    [Fact]
    public void AStalePlanWithNoExecutions_DoesNotLendItsLifetimeDopToThePoint()
    {
        var rows = new List<QueryStatsHistoryRow>
        {
            Row(T1, 0, 0, 1, 8, "0xSTALE"),
            Row(T1, 6, 600, 1, 2, "0xLIVE"),
            Row(T1.AddMinutes(5), 0, 0, 1, 8, "0xSTALE"),
            Row(T1.AddMinutes(5), 0, 0, 1, 2, "0xLIVE"),
        };

        var points = McpQueryTools.OnePointPerCollection(rows);

        Assert.Equal(2, points.Count);
        Assert.Equal(6, points[0].DeltaExecutions);
        Assert.Equal(2, points[0].MaxDop);
        Assert.Equal("0xLIVE", points[0].QueryPlanHash);
        Assert.Equal(1, points[0].PlanCount);

        /* Nothing ran: the point still shows both plans, but a DOP describes an execution, so the pair is null. */
        Assert.Null(points[1].MinDop);
        Assert.Null(points[1].MaxDop);
        Assert.Equal("", points[1].QueryPlanHash);
        Assert.Equal(2, points[1].PlanCount);
    }

    [Fact]
    public void ASingleHandleWithNoExecutions_HasNullDop()
    {
        var points = McpQueryTools.OnePointPerCollection(new List<QueryStatsHistoryRow> { Row(T1, 0, 0, 1, 8, "0xSTALE") });

        var point = Assert.Single(points);
        Assert.Null(point.MinDop);
        Assert.Null(point.MaxDop);
        Assert.Equal("0xSTALE", point.QueryPlanHash);
    }

    [Fact]
    public void ThePlanRegressionRecommendation_PointsAtQueryStoreTop_ForPointsWhereTheHashIsNull()
    {
        var json = System.Text.Json.JsonSerializer.Serialize(ToolRecommendations.GetForStoryPath(PerformanceMonitor.Analysis.StoryKeys.OfPath("PLAN_REGRESSION")));
        using var doc = System.Text.Json.JsonDocument.Parse(json);
        string? reason = null;
        foreach (var e in doc.RootElement.EnumerateArray())
            if (e.GetProperty("tool").GetString() == "get_query_trend") reason = e.GetProperty("reason").GetString();

        Assert.NotNull(reason);
        Assert.Contains("plan_count is above 1", reason, StringComparison.Ordinal);
        Assert.Contains("get_query_store_top", reason, StringComparison.Ordinal);
    }
}
