/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #8147 - Lite's analyze_plan_xml lists the statistics a plan used through the shared formatter, so its
/// statistics_used equals what the formatter returns for the same plan.
/// </summary>
[Trait("Stage", "Guard")]
public sealed class PlanStatisticsUsedLiteTests
{
    private const string PlanXml =
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.Orders\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"1\" StatementEstRows=\"1\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">" +
        "<OptimizerStatsUsage>" +
        "<StatisticsInfo Database=\"SalesDb\" Schema=\"dbo\" Table=\"Orders\" Statistics=\"IX_Orders_CustomerId\" ModificationCount=\"450\" SamplingPercent=\"100\" LastUpdate=\"2026-10-30T02:14:07.53\"/>" +
        "<StatisticsInfo Database=\"SalesDb\" Schema=\"dbo\" Table=\"Orders\" Statistics=\"PK_Orders\" ModificationCount=\"5000\" SamplingPercent=\"42.5\" LastUpdate=\"2026-10-29T01:00:00\"/>" +
        "</OptimizerStatsUsage>" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Table Scan\" LogicalOp=\"Table Scan\" EstimateRows=\"1\" EstimateIO=\"0.003\" EstimateCPU=\"0.0001\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"0.0032\" TableCardinality=\"1000\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList/><TableScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues/>" +
        "<Object Database=\"[SalesDb]\" Schema=\"[dbo]\" Table=\"[Orders]\" IndexKind=\"Heap\" Storage=\"RowStore\"/></TableScan></RelOp>" +
        "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    [Fact]
    public void Lite_AnalyzePlanXml_ReturnsTheSameStatisticsUsedAsTheSharedFormatter()
    {
        using var lite = JsonDocument.Parse(McpPlanTools.AnalyzePlanXml(PlanXml));
        using var shared = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(PlanXml, null, "xml", null));

        var liteStmt = lite.RootElement.GetProperty("statements")[0];
        var sharedStmt = shared.RootElement.GetProperty("statements")[0];

        var liteRows = liteStmt.GetProperty("statistics_used").EnumerateArray().ToList();
        var sharedRows = sharedStmt.GetProperty("statistics_used").EnumerateArray().ToList();

        Assert.Equal(2, sharedRows.Count);
        Assert.Equal(sharedRows.Count, liteRows.Count);
        for (var i = 0; i < sharedRows.Count; i++)
        {
            Assert.Equal(sharedRows[i].GetRawText(), liteRows[i].GetRawText());
        }
        Assert.Equal(5000, liteRows[0].GetProperty("modification_count").GetInt64());
        Assert.Equal("recorded", liteStmt.GetProperty("statistics_usage").GetString());
    }
}
