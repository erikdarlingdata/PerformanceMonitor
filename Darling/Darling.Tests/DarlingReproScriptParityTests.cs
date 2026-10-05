/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using PerformanceMonitor.Darling.Viewer;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5233: the web repro script is the Darling Viewer's repro script. For each kind that keeps query text,
/// <see cref="DarlingReproScript.Build"/> must produce byte-for-byte what
/// <c>ViewerServerTab.BuildReproScriptForRow</c> produces from the same stored fields, so the two seats cannot drift.
/// </summary>
public sealed class DarlingReproScriptParityTests
{
    private const string Text = "SELECT o.OrderId FROM dbo.Orders AS o WHERE o.CustomerId = @CustomerId;";
    private const string Plan =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\"><BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\"><QueryPlan><ParameterList>" +
        "<ColumnReference Column=\"@CustomerId\" ParameterDataType=\"int\" ParameterCompiledValue=\"(42)\" />" +
        "</ParameterList></QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    [Theory]
    [InlineData(Plan)]
    [InlineData(null)]
    public void QueryStats_MatchesTheViewer(string? plan)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ViewerServerTab is a WPF type; the Mac cannot load it. The builder-argument pins below run everywhere.");
        RunQueryStatsParity(plan);
    }

    private static void RunQueryStatsParity(string? plan)
    {
        var viewer = ViewerServerTab.BuildReproScriptForRow(
            new ViewerQueryStatsRow { QueryText = Text, DatabaseName = "Orders", QueryHash = "0xAB" }, plan, DarlingReproScript.ProductName);
        Assert.NotNull(viewer);
        Assert.Equal(viewer, DarlingReproScript.Build("query_hash", Text, "Orders", plan, "READ COMMITTED"));
    }

    [Theory]
    [InlineData(Plan)]
    [InlineData(null)]
    public void QueryStore_MatchesTheViewer(string? plan)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ViewerServerTab is a WPF type; the Mac cannot load it. The builder-argument pins below run everywhere.");
        RunQueryStoreParity(plan);
    }

    private static void RunQueryStoreParity(string? plan)
    {
        var viewer = ViewerServerTab.BuildReproScriptForRow(
            new ViewerQueryStoreRow { QueryText = Text, DatabaseName = "Orders", QueryId = 42, PlanId = 7 }, plan, DarlingReproScript.ProductName);
        Assert.NotNull(viewer);
        Assert.Equal(viewer, DarlingReproScript.Build("query_store", Text, "Orders", plan, "READ COMMITTED"));
    }

    [Theory]
    [InlineData(Plan, "SERIALIZABLE")]
    [InlineData(null, "READ COMMITTED")]
    public void ActiveSnapshot_MatchesTheViewer_AndKeepsItsIsolationLevel(string? plan, string isolation)
    {
        Assert.SkipUnless(OperatingSystem.IsWindows(), "ViewerServerTab is a WPF type; the Mac cannot load it. The builder-argument pins below run everywhere.");
        RunActiveSnapshotParity(plan, isolation);
    }

    private static void RunActiveSnapshotParity(string? plan, string isolation)
    {
        var viewer = ViewerServerTab.BuildReproScriptForRow(
            new ViewerQuerySnapshotRow { QueryText = Text, DatabaseName = "Orders", TransactionIsolationLevel = isolation }, plan, DarlingReproScript.ProductName);
        Assert.NotNull(viewer);
        var web = DarlingReproScript.Build("active_snapshot", Text, "Orders", plan, isolation);
        Assert.Equal(viewer, web);
        Assert.Contains("SET TRANSACTION ISOLATION LEVEL", web, StringComparison.Ordinal);
    }

    /* The same arguments, straight into the shared builder: runs on every platform, so the mapping (source label,
       isolation only for Active Queries, product name, no Azure flag) is pinned even where the Viewer cannot load. */
    [Theory]
    [InlineData("query_hash", "Top Queries (dm_exec_query_stats)", false)]
    [InlineData("query_store", "Query Store", false)]
    [InlineData("active_snapshot", "Active Queries", true)]
    public void EachKind_PassesTheViewersArgumentsToTheSharedBuilder(string kind, string source, bool carriesIsolation)
    {
        var expected = ReproScriptBuilder.BuildReproScript(
            Text, "Orders", Plan, carriesIsolation ? "SERIALIZABLE" : null, source, productName: DarlingReproScript.ProductName);
        Assert.Equal(expected, DarlingReproScript.Build(kind, Text, "Orders", Plan, "SERIALIZABLE"));
        Assert.Contains(source, expected, StringComparison.Ordinal);
        Assert.Equal(carriesIsolation, expected.Contains("SET TRANSACTION ISOLATION LEVEL", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("query_hash")]
    [InlineData("query_store")]
    [InlineData("active_snapshot")]
    public void NoQueryText_BuildsNoScript(string kind)
    {
        Assert.Null(DarlingReproScript.Build(kind, null, "Orders", Plan, null));
        Assert.Null(DarlingReproScript.Build(kind, "", "Orders", Plan, null));
    }

    [Fact]
    public void AProcedure_BuildsNoScript() =>
        Assert.Null(DarlingReproScript.Build("procedure", Text, "Orders", Plan, null));

    [Fact]
    public void TheQueryStoreText_IsReadFromTheCollectorTable_BeforeTheInlineFactColumn()
    {
        var sql = DarlingReproScript.QueryStoreTextSql;
        Assert.True(
            sql.IndexOf("query_store_text", StringComparison.Ordinal) is var first and >= 0
            && first < sql.IndexOf("query_store_stats", StringComparison.Ordinal),
            "query_store_text must be consulted before the inline query_store_stats.query_text");
        Assert.Contains("COALESCE", sql, StringComparison.Ordinal);
    }
}
