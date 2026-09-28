/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4569: the MCP plan output's <c>memory_grant</c> block carries <c>desired_kb</c> and
/// <c>serial_required_kb</c> — the optimizer's pre-execution "desired" and "serial required"
/// grant sizes, PerformanceStudio's <c>DesiredKB</c>/<c>SerialRequiredKB</c> JSON names
/// (<c>desired_kb</c>/<c>serial_required_kb</c>). <c>desired_kb</c> already existed; this pins
/// <c>serial_required_kb</c> alongside it on both an estimated plan (no runtime grant) and an
/// actual plan (a runtime grant present) — the MCP counterpart of
/// <see cref="Viewer4569Tests"/>'s parser/helper pins.
/// </summary>
public sealed class McpMemoryGrantV4569Tests
{
    [Fact]
    public void EstimatedPlan_MemoryGrant_CarriesDesiredAndSerialRequiredKB()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(PlanWithMemoryGrant(withActuals: false), "s", "xml", null));
        var stmt = doc.RootElement.GetProperty("statements")[0];
        var grant = stmt.GetProperty("memory_grant");

        Assert.Equal(51200, grant.GetProperty("desired_kb").GetInt64());
        Assert.Equal(25600, grant.GetProperty("serial_required_kb").GetInt64());
    }

    [Fact]
    public void ActualPlan_MemoryGrant_CarriesDesiredAndSerialRequiredKB()
    {
        using var doc = JsonDocument.Parse(McpPlanAnalysisFormatter.BuildAnalysisResult(PlanWithMemoryGrant(withActuals: true), "s", "xml", null));
        var stmt = doc.RootElement.GetProperty("statements")[0];
        var grant = stmt.GetProperty("memory_grant");

        Assert.Equal(51200, grant.GetProperty("desired_kb").GetInt64());
        Assert.Equal(25600, grant.GetProperty("serial_required_kb").GetInt64());
        Assert.Equal(51200, grant.GetProperty("granted_kb").GetInt64());
    }

    private static string PlanWithMemoryGrant(bool withActuals)
    {
        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\" encoding=\"utf-16\"?>");
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">");
        sb.Append("<BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">");
        if (withActuals)
        {
            sb.Append("<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">");
            sb.Append("<MemoryGrantInfo SerialRequiredMemory=\"25600\" SerialDesiredMemory=\"51200\" RequiredMemory=\"51200\" DesiredMemory=\"51200\" RequestedMemory=\"51200\" GrantedMemory=\"51200\" MaxUsedMemory=\"40000\" GrantWaitTime=\"0\" />");
        }
        else
        {
            sb.Append("<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" NonParallelPlanReason=\"NoParallelDueToEstimatedRowCount\">");
            sb.Append("<MemoryGrantInfo SerialRequiredMemory=\"25600\" SerialDesiredMemory=\"51200\" RequiredMemory=\"51200\" DesiredMemory=\"51200\" />");
        }

        sb.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">");
        sb.Append("<OutputList />");
        sb.Append("<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">");
        sb.Append("<DefinedValues /><Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />");
        sb.Append("</IndexScan></RelOp>");
        sb.Append("</QueryPlan></StmtSimple></Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }
}
