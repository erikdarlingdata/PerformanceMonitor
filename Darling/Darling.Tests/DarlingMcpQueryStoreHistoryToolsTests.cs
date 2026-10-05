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

    [Fact]
    public void TheHostRegistersTheClass_AndTheWebCatalogListsTheTool()
    {
        var host = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs");
        Assert.Contains("WithGeminiCompatibleTools<DarlingMcpQueryStoreHistoryTools>()", host, StringComparison.Ordinal);
    }
}
