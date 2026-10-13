/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5633 item 21: <c>analyze_query_plan</c>, <c>analyze_plan_xml</c> and <c>analyze_query_store_plan</c> returned
/// <c>"dop": 0</c> and a null <c>serial_reason</c> for a parallel estimated plan. A plan from the cache or Query Store
/// carries no <c>QueryPlan/@DegreeOfParallelism</c> (only an actual plan does), and the parser cast the missing
/// attribute to 0, which reads as "serial". A missing attribute is now null, and a <c>parallel</c> flag is read off
/// the operators marked <c>Parallel="1"</c>, which every plan source carries.
/// </summary>
public sealed class PlanDopParallel5633Tests
{
    private static string Plan(string queryPlanAttrs, string parallelFlag) =>
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\"" + queryPlanAttrs + ">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"100\" TableCardinality=\"10\" Parallel=\"" + parallelFlag + "\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
        "<OutputList /><IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\"><DefinedValues />" +
        "<Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" /></IndexScan></RelOp>" +
        "</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>";

    private static JsonElement Statement(string xml, out JsonDocument doc)
    {
        doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "s", "xml", null));
        return doc.RootElement.GetProperty("statements")[0];
    }

    [Fact]
    public void ParallelOperatorsWithNoQueryPlanDop_ReportDopNull_AndParallelTrue()
    {
        var stmt = Statement(Plan("", "1"), out var doc);
        using (doc)
        {
            Assert.Equal(JsonValueKind.Null, stmt.GetProperty("dop").ValueKind);
            Assert.True(stmt.GetProperty("parallel").GetBoolean());
        }
    }

    [Fact]
    public void SerialOperatorsWithNoQueryPlanDop_ReportDopNull_AndParallelFalse()
    {
        var stmt = Statement(Plan("", "0"), out var doc);
        using (doc)
        {
            Assert.Equal(JsonValueKind.Null, stmt.GetProperty("dop").ValueKind);
            Assert.False(stmt.GetProperty("parallel").GetBoolean());
        }
    }

    [Fact]
    public void ActualPlanDop_StillReportedAsTheNumber()
    {
        var stmt = Statement(Plan(" DegreeOfParallelism=\"6\"", "1"), out var doc);
        using (doc)
        {
            Assert.Equal(6, stmt.GetProperty("dop").GetInt32());
            Assert.True(stmt.GetProperty("parallel").GetBoolean());
        }
    }

    [Fact]
    public void ExplicitSerialDop_IsOneNotNull()
    {
        var stmt = Statement(Plan(" DegreeOfParallelism=\"1\" NonParallelPlanReason=\"MaxDOPSetToOne\"", "0"), out var doc);
        using (doc)
        {
            Assert.Equal(1, stmt.GetProperty("dop").GetInt32());
            Assert.Equal("MaxDOPSetToOne", stmt.GetProperty("serial_reason").GetString());
            Assert.False(stmt.GetProperty("parallel").GetBoolean());
        }
    }
}
