/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Text;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4569: the plan model parses <c>MemoryGrantInfo</c>'s <c>DesiredMemory</c>/<c>SerialRequiredMemory</c>
/// attributes, and the pure helper <see cref="EstimatedMemoryGrantDisplay.EstimatedMemoryGrantRows"/> renders
/// the same estimated-plan memory-grant row(s) PerformanceStudio's Runtime card shows
/// (erikdarlingdata/PerformanceStudio@b7ef358). The runtime-card wiring itself lands with #4570.
/// </summary>
public sealed class Viewer4569Tests
{
    /// <summary>A minimal ShowPlan XML for one statement, with or without <c>RunTimeInformation</c>
    /// (an actual plan) and with the given <c>MemoryGrantInfo</c> attributes.</summary>
    private static string PlanXml(bool actual, long desiredKb, long serialRequiredKb)
    {
        var sb = new StringBuilder();
        sb.Append("<?xml version=\"1.0\" encoding=\"utf-16\"?>");
        sb.Append("<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">");
        sb.Append("<BatchSequence><Batch><Statements>");
        sb.Append("<StmtSimple StatementText=\"SELECT 1\" StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"100\" StatementEstRows=\"10\">");
        sb.Append("<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">");
        if (actual)
            sb.Append("<QueryTimeStats CpuTime=\"5\" ElapsedTime=\"7\" />");
        sb.Append($"<MemoryGrantInfo SerialRequiredMemory=\"{serialRequiredKb}\" SerialDesiredMemory=\"{serialRequiredKb}\" RequiredMemory=\"{desiredKb}\" DesiredMemory=\"{desiredKb}\" RequestedMemory=\"{desiredKb}\" GrantedMemory=\"0\" MaxUsedMemory=\"0\" GrantWaitTime=\"0\" />");
        sb.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" EstimatedTotalSubtreeCost=\"100\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">");
        sb.Append("<OutputList />");
        sb.Append("<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">");
        sb.Append("<DefinedValues /><Object Database=\"[StackOverflow]\" Schema=\"[dbo]\" Table=\"[Posts]\" Index=\"[PK_Posts]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />");
        sb.Append("</IndexScan>");
        sb.Append("</RelOp>");
        sb.Append("</QueryPlan></StmtSimple>");
        sb.Append("</Statements></Batch></BatchSequence></ShowPlanXML>");
        return sb.ToString();
    }

    [Fact]
    public void Parser_reads_desired_and_serial_required_memory_from_MemoryGrantInfo()
    {
        var plan = ShowPlanParser.Parse(PlanXml(actual: false, desiredKb: 2048, serialRequiredKb: 512));
        var stmt = plan.Batches[0].Statements[0];

        Assert.NotNull(stmt.MemoryGrant);
        Assert.Equal(2048, stmt.MemoryGrant!.DesiredMemoryKB);
        Assert.Equal(512, stmt.MemoryGrant.SerialRequiredMemoryKB);
    }

    [Fact]
    public void Helper_shows_desired_and_serial_required_rows_when_they_differ()
    {
        var plan = ShowPlanParser.Parse(PlanXml(actual: false, desiredKb: 2048, serialRequiredKb: 512));
        var stmt = plan.Batches[0].Statements[0];

        var rows = EstimatedMemoryGrantDisplay.EstimatedMemoryGrantRows(stmt);

        Assert.Equal(2, rows.Count);
        Assert.Equal("Memory (estimated)", rows[0].Label);
        Assert.Equal("2.0 MB desired", rows[0].Value);
        Assert.Equal("Serial required", rows[1].Label);
        Assert.Equal("512 KB", rows[1].Value);
    }

    [Fact]
    public void Helper_omits_serial_required_row_when_it_equals_desired()
    {
        var plan = ShowPlanParser.Parse(PlanXml(actual: false, desiredKb: 1024, serialRequiredKb: 1024));
        var stmt = plan.Batches[0].Statements[0];

        var rows = EstimatedMemoryGrantDisplay.EstimatedMemoryGrantRows(stmt);

        Assert.Single(rows);
        Assert.Equal("Memory (estimated)", rows[0].Label);
        Assert.Equal("1.0 MB desired", rows[0].Value);
    }

    [Fact]
    public void Helper_returns_nothing_for_actual_plans()
    {
        var plan = ShowPlanParser.Parse(PlanXml(actual: true, desiredKb: 2048, serialRequiredKb: 512));
        var stmt = plan.Batches[0].Statements[0];

        var rows = EstimatedMemoryGrantDisplay.EstimatedMemoryGrantRows(stmt);

        Assert.Empty(rows);
    }

    [Fact]
    public void Helper_returns_nothing_when_desired_grant_is_zero()
    {
        var plan = ShowPlanParser.Parse(PlanXml(actual: false, desiredKb: 0, serialRequiredKb: 0));
        var stmt = plan.Batches[0].Statements[0];

        var rows = EstimatedMemoryGrantDisplay.EstimatedMemoryGrantRows(stmt);

        Assert.Empty(rows);
    }
}
