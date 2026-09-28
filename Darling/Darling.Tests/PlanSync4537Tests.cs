/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4537: mirrors PerformanceStudio's removal of three plan-analysis rules. Rule 21 (CTE Multiple
/// References) was dropped because an actual plan's own runtime stats already show where time went,
/// so a statement-text regex guessing at CTE reuse was redundant. Rules 25 (Ineffective Parallelism)
/// and 31 (Parallel Wait Bottleneck) were dropped because they were meta-findings guessing at causes
/// that the wait stats list and the runtime summary's CPU:Elapsed ratio already show directly.
///
/// <para>Each fixture below reproduces the exact condition that used to fire the removed rule on dev
/// (a CTE referenced twice by FROM/JOIN; a parallel plan with elapsed well above CPU; a parallel plan
/// with CPU close to elapsed but far below its DOP potential) and now expects no finding of that
/// type.</para>
/// </summary>
public sealed class PlanSync4537Tests
{
    [Fact]
    public void CteReferencedTwice_NoLongerRaisesMultiReferenceWarning()
    {
        var plan = ShowPlanParser.Parse(CteMultiReferenceXml);
        PlanAnalysisPipeline.Run(plan);

        var stmt = plan.Batches[0].Statements[0];
        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "CTE Multiple References");
    }

    [Fact]
    public void ParallelPlanElapsedFarAboveCpu_NoLongerRaisesWaitBottleneckWarning()
    {
        var plan = ShowPlanParser.Parse(ParallelWaitBottleneckXml);
        PlanAnalysisPipeline.Run(plan);

        var stmt = plan.Batches[0].Statements[0];
        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Parallel Wait Bottleneck");
    }

    [Fact]
    public void ParallelPlanBelowDopPotential_NoLongerRaisesIneffectiveParallelismWarning()
    {
        var plan = ShowPlanParser.Parse(IneffectiveParallelismXml);
        PlanAnalysisPipeline.Run(plan);

        var stmt = plan.Batches[0].Statements[0];
        Assert.DoesNotContain(stmt.PlanWarnings, w => w.WarningType == "Ineffective Parallelism");
    }

    // A CTE named c1 is referenced by two FROM targets after its own definition — this used to
    // trigger DetectMultiReferenceCte's refCount > 1 branch.
    private const string CteMultiReferenceXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"WITH c1 AS (SELECT id FROM dbo.t) SELECT * FROM c1 JOIN c1 b ON 1 = 1\" " +
        "StatementId=\"1\" StatementType=\"SELECT\" StatementSubTreeCost=\"1\" StatementEstRows=\"10\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\">" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"1\" Parallel=\"0\" EstimateRebinds=\"0\" EstimateRewinds=\"0\">" +
        "<OutputList /><IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" " +
        "IndexKind=\"Clustered\" Storage=\"RowStore\" /></IndexScan></RelOp>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    // DOP 4, CPU well below Elapsed (speedup < 0.5): used to trigger the Rule 31 wait-bottleneck branch.
    private const string ParallelWaitBottleneckXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.t\" StatementId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"100\" StatementEstRows=\"10\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" DegreeOfParallelism=\"4\">" +
        "<QueryTimeStats CpuTime=\"1000\" ElapsedTime=\"5000\" />" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"100\" Parallel=\"1\" EstimateRebinds=\"0\" EstimateRewinds=\"0\">" +
        "<OutputList /><IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" " +
        "IndexKind=\"Clustered\" Storage=\"RowStore\" /></IndexScan></RelOp>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";

    // DOP 4, CPU close to Elapsed (speedup >= 0.5) but efficiency well below 40%: used to trigger the
    // Rule 25 ineffective-parallelism branch.
    private const string IneffectiveParallelismXml =
        "<?xml version=\"1.0\" encoding=\"utf-16\"?>" +
        "<ShowPlanXML xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\" Version=\"1.539\" Build=\"16.0.1000.6\">" +
        "<BatchSequence><Batch><Statements>" +
        "<StmtSimple StatementText=\"SELECT * FROM dbo.t\" StatementId=\"1\" StatementType=\"SELECT\" " +
        "StatementSubTreeCost=\"100\" StatementEstRows=\"10\">" +
        "<QueryPlan CachedPlanSize=\"16\" CompileTime=\"1\" CompileCPU=\"1\" CompileMemory=\"64\" DegreeOfParallelism=\"4\">" +
        "<QueryTimeStats CpuTime=\"1100\" ElapsedTime=\"1000\" />" +
        "<RelOp NodeId=\"0\" PhysicalOp=\"Clustered Index Scan\" LogicalOp=\"Clustered Index Scan\" " +
        "EstimateRows=\"10\" EstimateIO=\"0.1\" EstimateCPU=\"0.1\" AvgRowSize=\"9\" " +
        "EstimatedTotalSubtreeCost=\"100\" Parallel=\"1\" EstimateRebinds=\"0\" EstimateRewinds=\"0\">" +
        "<OutputList /><IndexScan Ordered=\"0\" ForcedIndex=\"0\" ForceScan=\"0\" NoExpandHint=\"0\" Storage=\"RowStore\">" +
        "<DefinedValues /><Object Database=\"[db]\" Schema=\"[dbo]\" Table=\"[t]\" Index=\"[PK_t]\" " +
        "IndexKind=\"Clustered\" Storage=\"RowStore\" /></IndexScan></RelOp>" +
        "</QueryPlan></StmtSimple>" +
        "</Statements></Batch></BatchSequence></ShowPlanXML>";
}
