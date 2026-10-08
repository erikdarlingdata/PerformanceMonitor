/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Reflection;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>#5234: the surface and the per-plan fold of get_query_store_query_history.</summary>
public sealed class DarlingMcpQueryStoreHistoryToolsTests
{
    private static MethodInfo Tool() =>
        typeof(DarlingMcpQueryStoreHistoryTools).GetMethod(nameof(DarlingMcpQueryStoreHistoryTools.GetQueryStoreQueryHistory))!;

    [Fact]
    public void TheToolName_AndItsHeadDescription_AreWithinTheBudget()
    {
        var attribute = Tool().GetCustomAttribute<McpServerToolAttribute>()!;
        Assert.Equal("get_query_store_query_history", attribute.Name);
        var description = Tool().GetCustomAttribute<DescriptionAttribute>()!.Description;
        var head = description[..description.IndexOf("<<GUIDE>>", StringComparison.Ordinal)].Trim();
        Assert.True(head.Length <= 160, $"the head is {head.Length} characters");
    }

    [Fact]
    public void TheRequiredParameters_AreExactlyTheDatabaseAndTheQueryId()
    {
        var required = Tool().GetParameters()
            .Where(p => !p.HasDefaultValue && p.ParameterType != typeof(Npgsql.NpgsqlDataSource))
            .Select(p => p.Name)
            .OrderBy(n => n, StringComparer.Ordinal);
        Assert.Equal(new[] { "database_name", "query_id" }, required);
    }

    [Fact]
    public void TheHistorySql_DedupesPerInterval_OnTheSharedPartition()
    {
        Assert.Contains("PARTITION BY plan_id, runtime_stats_interval_id, first_execution_time, execution_type_desc, replica_role",
            DarlingMcpQueryStoreHistoryTools.HistorySql, StringComparison.Ordinal);
        Assert.Contains("ORDER BY collection_time DESC, execution_count DESC", DarlingMcpQueryStoreHistoryTools.HistorySql, StringComparison.Ordinal);
        Assert.Contains("WHERE rn = 1", DarlingMcpQueryStoreHistoryTools.HistorySql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheFold_WeightsAveragesByExecutions_AndTotalsPerPlan()
    {
        var t0 = new DateTime(2026, 3, 4, 5, 0, 0, DateTimeKind.Unspecified);
        var rows = new List<DarlingMcpQueryStoreHistoryTools.HistoryRow>
        {
            new(7, t0, 10, 100, 40, t0.AddMinutes(-60), t0, false),
            new(7, t0.AddHours(1), 30, 200, 80, t0.AddMinutes(-5), t0.AddHours(1), true),
            new(3, t0, 5, null, null, null, null, false),
        };
        var plans = DarlingMcpQueryStoreHistoryTools.FoldPlans(rows);
        Assert.Equal(new long[] { 3, 7 }, plans.Select(p => p.PlanId));
        var p7 = plans[1];
        Assert.Equal(40, p7.ExecutionCount);
        Assert.Equal(10 * 100 + 30 * 200, p7.TotalDurationMs);
        Assert.Equal(7000.0 / 40, p7.AvgDurationMs);
        Assert.Equal(10 * 40 + 30 * 80, p7.TotalCpuMs);
        Assert.Equal(2800.0 / 40, p7.AvgCpuMs);
        Assert.Equal(t0.AddMinutes(-60), p7.FirstExecutionTime);
        Assert.Equal(t0.AddHours(1), p7.LastExecutionTime);
        Assert.True(p7.IsForcedPlan);
        Assert.False(plans[0].IsForcedPlan);
        Assert.Equal(0, plans[0].TotalDurationMs);
    }

    private static DarlingMcpQueryStoreHistoryTools.HistoryRow Row(long plan, DateTime at, long executions, double? durationMs, double? cpuMs = null) =>
        new(plan, at, executions, durationMs, cpuMs, null, null, false);

    [Fact]
    public void TheFold_GivesOnePointPerPlanAndInstant_CombiningOutcomesAndReplicas_ExecutionWeighted()
    {
        var t = new DateTime(2026, 3, 4, 5, 0, 0, DateTimeKind.Unspecified);
        var rows = new[]
        {
            Row(7, t, 1000, 5),                // Regular
            Row(7, t, 2, 30_000),              // Aborted, same instant
            Row(9, t, 10, 100, 10),            // one replica
            Row(9, t, 30, 200, 30),            // another replica
            Row(7, t.AddMinutes(15), 40, 5.123456789),
        };
        var points = DarlingMcpQueryStoreHistoryTools.FoldPoints(rows);
        Assert.Equal(new[] { (7L, t), (9L, t), (7L, t.AddMinutes(15)) }, points.Select(p => (p.PlanId, p.CollectionTime)));
        Assert.Equal(1002, points[0].ExecutionCount);
        Assert.Equal((1000 * 5 + 2 * 30_000.0) / 1002, points[0].AvgDurationMs!.Value, 9);
        Assert.Equal(40, points[1].ExecutionCount);
        Assert.Equal((10 * 100 + 30 * 200.0) / 40, points[1].AvgDurationMs!.Value, 9);
        Assert.Equal((10 * 10 + 30 * 30.0) / 40, points[1].AvgCpuMs!.Value, 9);
        Assert.Equal(5.123456789, points[2].AvgDurationMs);   // a lone row passes through unchanged
    }

    [Fact]
    public void DroppedPlans_DoNotUseUpThePointCap()
    {
        var t = new DateTime(2026, 3, 4, 0, 0, 0, DateTimeKind.Unspecified);
        var rows = new List<DarlingMcpQueryStoreHistoryTools.HistoryRow>();
        for (var i = 0; i < 4; i++)
            foreach (var plan in new long[] { 1, 2, 3, 4 })
                rows.Add(Row(plan, t.AddMinutes(15 * i), 10, plan));
        var listed = DarlingMcpQueryStoreHistoryTools.BuildPoints(rows, new long[] { 1, 2 });
        Assert.Equal(8, listed.Count);
        Assert.All(listed, p => Assert.True(p.PlanId is 1 or 2));
        var cut = DarlingMcpQueryStoreHistoryTools.NewestWithinBudget(listed, _ => 1, 4, int.MaxValue);
        /* A cap of 4 holds the newest two instants of the two listed plans; with plans 3 and 4 in the list it held one instant of all four. */
        Assert.Equal(new long[] { 1, 2, 1, 2 }, cut.Select(p => p.PlanId));
        Assert.Equal(
            new[] { t.AddMinutes(30), t.AddMinutes(30), t.AddMinutes(45), t.AddMinutes(45) }.Select(x => x.ToString("o", CultureInfo.InvariantCulture)),
            cut.Select(p => p.CollectionTime));
    }

    [Fact]
    public void Points_ComeOutInOneOrder_WhateverOrderTheRowsArriveIn()
    {
        var t = new DateTime(2026, 3, 4, 0, 0, 0, DateTimeKind.Unspecified);
        var rows = new[] { Row(9, t, 1, 1), Row(3, t, 1, 1), Row(5, t, 1, 1), Row(5, t, 2, 2), Row(9, t.AddMinutes(-15), 1, 1) };
        var forward = DarlingMcpQueryStoreHistoryTools.BuildPoints(rows, new long[] { 3, 5, 9 }).Select(p => (p.CollectionTime, p.PlanId)).ToList();
        var backward = DarlingMcpQueryStoreHistoryTools.BuildPoints(rows.Reverse(), new long[] { 9, 5, 3 }).Select(p => (p.CollectionTime, p.PlanId)).ToList();
        Assert.Equal(forward, backward);
        Assert.Equal(new long[] { 9, 3, 5, 9 }, forward.Select(p => p.PlanId));
    }

    [Fact]
    public void ThePoint_SerializesWithTheSnakeCaseKeysThePageReads()
    {
        var point = new DarlingMcpQueryStoreHistoryTools.HistoryPoint("2026-03-04T05:00:00.0000000", 7, 10, 100, 40);
        Assert.Equal(
            "{\"collection_time\":\"2026-03-04T05:00:00.0000000\",\"plan_id\":7,\"execution_count\":10,\"avg_duration_ms\":100,\"avg_cpu_ms\":40}",
            System.Text.Json.JsonSerializer.Serialize(point));
    }

    [Fact]
    public void TheHistoryOrder_IsTotalOverTheDedupedRows_InBothReads()
    {
        const string order = "ORDER BY collection_time, plan_id, runtime_stats_interval_id, execution_type_desc, replica_role, first_execution_time";
        Assert.EndsWith(order, DarlingMcpQueryStoreHistoryTools.HistorySql, StringComparison.Ordinal);
        Assert.EndsWith(order, DarlingMcpQueryStoreHistoryTools.HistoryTableSql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheTableRead_TakesTheIntervalTablesOneRowPerInterval_WithNoSecondDedupe()
    {
        var sql = DarlingMcpQueryStoreHistoryTools.HistoryTableSql;
        Assert.Contains("FROM query_store_interval_wide", sql, StringComparison.Ordinal);
        Assert.Contains("WHERE server_id = $1 AND database_name = $2 AND query_id = $3", sql, StringComparison.Ordinal);
        Assert.Contains("collection_time >= $4 AND collection_time <= $5", sql, StringComparison.Ordinal);
        Assert.Contains("first_execution_time >= $4 - interval '", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("ROW_NUMBER", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHistory_FollowsTheGridsTierChoice_ThroughTheGridsOwnPreCheckAndGate()
    {
        var history = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpQueryStoreHistoryTools.cs");
        Assert.Contains("DarlingDataReader.QueryStoreTopMayReadTable(start, end)", history, StringComparison.Ordinal);
        Assert.Contains("QueryStoreIntervalWide.ResolveReadAsync(", history, StringComparison.Ordinal);
        Assert.Contains("DarlingDataReader.QueryStoreTopMinWindow", history, StringComparison.Ordinal);
        var reader = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingDataReader.cs");
        Assert.Contains("if (QueryStoreTopMayReadTable(startUtc, endUtc))", reader, StringComparison.Ordinal);
    }

    [Fact]
    public void TheHostRegistersTheClass_AndTheWebCatalogListsTheTool()
    {
        var host = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        Assert.Contains("WithGeminiCompatibleTools<DarlingMcpQueryStoreHistoryTools>()", host, StringComparison.Ordinal);
    }
}
