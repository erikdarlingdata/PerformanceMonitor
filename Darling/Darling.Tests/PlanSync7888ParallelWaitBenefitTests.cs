/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #7888. A parallelism wait (CXPACKET, CXCONSUMER, CXSYNC_*) on a parallel plan is scored from the plan's
/// DOP efficiency gap, elapsed minus CPU over DOP. That gap is bounded by the wait's own share of elapsed
/// time, so a few milliseconds of CXCONSUMER on an I/O-bound plan cannot be the Critical finding at the top
/// of the list. A wait that really is most of the elapsed time keeps its score.
/// </summary>
public sealed class PlanSync7888ParallelWaitBenefitTests
{
    /// <summary>Elapsed 10,000 ms, CPU 1,000 ms, DOP 8: the efficiency gap is 98.75 percent of elapsed.</summary>
    private const long ElapsedMs = 10_000;

    private static PlanStatement Analyze(string waitType, long waitTimeMs)
    {
        var plan = ShowPlanParser.Parse(ParallelPlan(waitType, waitTimeMs));
        PlanAnalysisPipeline.Run(plan, null, null, CancellationToken.None);
        return PlanStatements.EnumerateAll(plan).Single();
    }

    private static PlanWarning WaitWarning(PlanStatement stmt, string waitType) =>
        Assert.Single(stmt.PlanWarnings, w => w.WarningType == "Wait: " + waitType);

    [Fact]
    public void TrivialCxConsumerWait_OnAnInefficientParallelPlan_IsInfo()
    {
        var stmt = Analyze("CXCONSUMER", 3);

        Assert.Equal(PlanWarningSeverity.Info, WaitWarning(stmt, "CXCONSUMER").Severity);
    }

    [Fact]
    public void TrivialCxConsumerWait_ScoresNoMoreThanItsOwnShareOfElapsed()
    {
        var stmt = Analyze("CXCONSUMER", 3);

        var ownShare = Math.Round(3.0 / ElapsedMs * 100, 1);
        Assert.Equal(ownShare, WaitWarning(stmt, "CXCONSUMER").MaxBenefitPercent);
    }

    [Fact]
    public void TrivialCxPacketWait_IsInfo()
    {
        var stmt = Analyze("CXPACKET", 3);

        Assert.Equal(PlanWarningSeverity.Info, WaitWarning(stmt, "CXPACKET").Severity);
    }

    /// <summary>6,000 ms of CXPACKET over 10,000 ms elapsed is 60 percent of the statement. The gap is
    /// 98.75 percent, so the wait's own share is the lower figure and the one reported.</summary>
    [Fact]
    public void LargeCxPacketWait_ScoresItsOwnShare_AndStaysCritical()
    {
        var stmt = Analyze("CXPACKET", 6_000);
        var warning = WaitWarning(stmt, "CXPACKET");

        Assert.Equal(60.0, warning.MaxBenefitPercent);
        Assert.Equal(PlanWarningSeverity.Critical, warning.Severity);
    }

    /// <summary>When the wait is larger than the gap, the gap still bounds it: 9,900 ms of CXPACKET is
    /// 99 percent of elapsed, but the plan can only gain its 98.75 percent of inefficiency.</summary>
    [Fact]
    public void CxPacketWaitAboveTheGap_IsBoundedByTheGap()
    {
        var stmt = Analyze("CXPACKET", 9_900);

        Assert.Equal(98.8, WaitWarning(stmt, "CXPACKET").MaxBenefitPercent);
    }

    /// <summary>The scorer's WaitBenefits list, which the wait-stats card reads, carries the same bounded figure.</summary>
    [Fact]
    public void WaitBenefits_CarryTheBoundedFigure()
    {
        var stmt = Analyze("CXCONSUMER", 3);

        var benefit = Assert.Single(stmt.WaitBenefits, b => b.WaitType == "CXCONSUMER");
        Assert.Equal(Math.Round(3.0 / ElapsedMs * 100, 1), benefit.MaxBenefitPercent);
    }

    /// <summary>A DOP 8 actual plan whose elapsed time is far above CPU over DOP: a Gather Streams over a
    /// parallel clustered index scan, 10,000 ms elapsed against 1,000 ms CPU, with one wait of the given
    /// type and time recorded in WaitStats.</summary>
    private static string ParallelPlan(string waitType, long waitTimeMs)
    {
        return
            "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
            "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.564\" Build=\"16.0.4215.2\">" +
            "<BatchSequence><Batch><Statements>" +
            "<StmtSimple StatementText=\"SELECT * FROM dbo.t\" StatementId=\"1\" StatementCompId=\"1\" StatementType=\"SELECT\" " +
            "StatementSubTreeCost=\"100\" StatementEstRows=\"1000000\" StatementOptmLevel=\"FULL\">" +
            "<QueryPlan DegreeOfParallelism=\"8\" CachedPlanSize=\"24\" CompileTime=\"2\" CompileCPU=\"2\" CompileMemory=\"200\">" +
            "<WaitStats><Wait WaitType=\"" + waitType + "\" WaitTimeMs=\"" + waitTimeMs + "\" WaitCount=\"8\" /></WaitStats>" +
            "<QueryTimeStats CpuTime=\"1000\" ElapsedTime=\"" + ElapsedMs + "\" />" +
            "<RelOp NodeId=\"0\" PhysicalOp=\"Parallelism\" LogicalOp=\"Gather Streams\" " +
            "EstimateRows=\"1000000\" EstimateIO=\"0\" EstimateCPU=\"5\" AvgRowSize=\"20\" " +
            "EstimatedTotalSubtreeCost=\"100\" Parallel=\"1\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
            "<OutputList />" +
            "<RunTimeInformation><RunTimeCountersPerThread Thread=\"0\" ActualRows=\"1000000\" ActualExecutions=\"1\" " +
            "ActualElapsedms=\"10000\" ActualCPUms=\"200\" /></RunTimeInformation>" +
            "<Parallelism>" +
            "<RelOp NodeId=\"1\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
            "EstimateRows=\"1000000\" EstimateIO=\"90\" EstimateCPU=\"5\" AvgRowSize=\"20\" " +
            "EstimatedTotalSubtreeCost=\"95\" Parallel=\"1\" EstimateRebinds=\"0\" EstimateRewinds=\"0\" EstimatedExecutionMode=\"Row\">" +
            "<OutputList />" +
            "<RunTimeInformation>" +
            "<RunTimeCountersPerThread Thread=\"1\" ActualRows=\"500000\" ActualExecutions=\"1\" ActualElapsedms=\"9900\" ActualCPUms=\"400\" ActualLogicalReads=\"50000\" ActualPhysicalReads=\"50000\" />" +
            "<RunTimeCountersPerThread Thread=\"2\" ActualRows=\"500000\" ActualExecutions=\"1\" ActualElapsedms=\"9900\" ActualCPUms=\"400\" ActualLogicalReads=\"50000\" ActualPhysicalReads=\"50000\" />" +
            "<RunTimeCountersPerThread Thread=\"0\" ActualRows=\"0\" ActualExecutions=\"0\" ActualElapsedms=\"0\" ActualCPUms=\"0\" />" +
            "</RunTimeInformation>" +
            "<IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
            "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" IndexKind=\"Clustered\" Storage=\"RowStore\" />" +
            "</IndexScan>" +
            "</RelOp>" +
            "</Parallelism>" +
            "</RelOp>" +
            "</QueryPlan></StmtSimple>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";
    }
}
