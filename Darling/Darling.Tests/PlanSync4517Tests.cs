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
/// #4517 — <c>BenefitScorer.ScoreWaitStats</c> used the standard per-thread
/// <c>max(0, elapsed - cpu)</c> wait math for every wait type. External and preemptive waits
/// (<c>MEMORY_ALLOCATION*</c>, <c>PREEMPTIVE_*</c>) keep the worker thread CPU-busy in the kernel,
/// so elapsed is about equal to cpu for those threads and the old math barely scored them.
///
/// <para>Mirrors erikdarlingdata/PerformanceStudio's <c>6b61d79</c>: a new
/// <see cref="BenefitScorer.IsExternalWait"/> classifier routes those waits through a separate
/// formula that scales the wait's share of statement CPU by the sum of each operator's max
/// per-thread self-CPU (<c>PlanAnalyzer.GetOperatorMaxThreadOwnCpuMs</c>), instead of the
/// elapsed-minus-cpu wait profile.</para>
/// </summary>
public sealed class PlanSync4517Tests
{
    private const string Ns = "http://schemas.microsoft.com/sqlserver/2004/07/showplan";

    private static ParsedPlan ScoreOnly(PlanStatement stmt)
    {
        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        BenefitScorer.Score(plan);
        return plan;
    }

    private static PlanWarning? WaitWarning(PlanStatement stmt, string waitType) =>
        stmt.PlanWarnings.FirstOrDefault(w => w.WarningType == "Wait: " + waitType);

    // ---- IsExternalWait classifier ----------------------------------------------------------

    [Theory]
    [InlineData("MEMORY_ALLOCATION_EXT", true)]
    [InlineData("RESERVED_MEMORY_ALLOCATION_EXT", true)]
    [InlineData("PREEMPTIVE_OS_WRITEFILEGATHER", true)]
    [InlineData("PREEMPTIVE_OS_FILEOPS", true)]
    [InlineData("PAGEIOLATCH_SH", false)]
    [InlineData("LCK_M_X", false)]
    public void IsExternalWait_MatchesTheMemoryAllocationAndPreemptivePrefixes(string waitType, bool expected)
    {
        Assert.Equal(expected, BenefitScorer.IsExternalWait(waitType));
    }

    // ---- PS's worked example: 48.3% -----------------------------------------------------------

    [Fact]
    public void WorkedExample_FromPerformanceStudio_ExternalWaitScoresFortyEightPointThreePercent()
    {
        // erikdarlingdata/PerformanceStudio@6b61d79's worked example:
        //   (26307 / 45075) * (11341 + 42) / 13748 = 48.3%
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 45075, ElapsedTimeMs = 13748 },
            DegreeOfParallelism = 2,
            // A stats-less container over two independent leaf operators, so each
            // leaf's self-CPU is its own PerThreadStats total (no parent-child
            // subtraction eating into it) — matching PS's "sum of max-thread-self-cpu
            // across operators" without needing a real nested plan shape.
            RootNode = new PlanNode
            {
                NodeId = 0,
                PhysicalOp = "Parallelism",
                HasActualStats = false,
                Children =
                [
                    new PlanNode
                    {
                        NodeId = 1,
                        PhysicalOp = "Index Scan",
                        HasActualStats = true,
                        ActualCPUMs = 11341,
                        PerThreadStats =
                        [
                            new PerThreadRuntimeInfo { ThreadId = 1, ActualElapsedMs = 11341, ActualCPUMs = 11341 },
                            new PerThreadRuntimeInfo { ThreadId = 2, ActualElapsedMs = 0, ActualCPUMs = 0 }
                        ]
                    },
                    new PlanNode
                    {
                        NodeId = 2,
                        PhysicalOp = "Nested Loops",
                        HasActualStats = true,
                        ActualCPUMs = 42,
                        PerThreadStats =
                        [
                            new PerThreadRuntimeInfo { ThreadId = 1, ActualElapsedMs = 42, ActualCPUMs = 42 },
                            new PerThreadRuntimeInfo { ThreadId = 2, ActualElapsedMs = 0, ActualCPUMs = 0 }
                        ]
                    }
                ]
            },
            WaitStats = [new WaitStatInfo { WaitType = "MEMORY_ALLOCATION_EXT", WaitTimeMs = 26307, WaitCount = 1 }]
        };

        ScoreOnly(stmt);

        var benefit = stmt.WaitBenefits.Single(b => b.WaitType == "MEMORY_ALLOCATION_EXT");
        Assert.Equal(48.3, benefit.MaxBenefitPercent);
    }

    // ---- PREEMPTIVE_OS_WRITEFILEGATHER on a CPU-busy thread: well above the old formula --------

    [Fact]
    public void PreemptiveWaitOnACpuBusyThread_ScoresWellAboveTheOldElapsedMinusCpuFormula()
    {
        // Node A: 2 threads, both CPU-busy in the kernel (elapsed == cpu) -> old wait math sees 0.
        // Node B: 2 threads with a small amount of real wait and little CPU.
        // Old (elapsed-cpu) formula: sumMax=145, sumTotal=290 -> benefit = 145*1000/290/1000*100 = 50%.
        // New (CPU-share) formula: sumMaxCpu=1800+5=1805, share=1000/2000 -> 90.2%.
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 2000, ElapsedTimeMs = 1000 },
            DegreeOfParallelism = 2,
            RootNode = new PlanNode
            {
                NodeId = 0,
                PhysicalOp = "Parallelism",
                HasActualStats = false,
                Children =
                [
                    new PlanNode
                    {
                        NodeId = 1,
                        PhysicalOp = "Hash Match",
                        HasActualStats = true,
                        ActualCPUMs = 1800,
                        PerThreadStats =
                        [
                            new PerThreadRuntimeInfo { ThreadId = 1, ActualElapsedMs = 1800, ActualCPUMs = 1800 },
                            new PerThreadRuntimeInfo { ThreadId = 2, ActualElapsedMs = 100, ActualCPUMs = 100 }
                        ]
                    },
                    new PlanNode
                    {
                        NodeId = 2,
                        PhysicalOp = "Index Scan",
                        HasActualStats = true,
                        ActualCPUMs = 10,
                        PerThreadStats =
                        [
                            new PerThreadRuntimeInfo { ThreadId = 1, ActualElapsedMs = 150, ActualCPUMs = 5 },
                            new PerThreadRuntimeInfo { ThreadId = 2, ActualElapsedMs = 150, ActualCPUMs = 5 }
                        ]
                    }
                ]
            },
            WaitStats = [new WaitStatInfo { WaitType = "PREEMPTIVE_OS_WRITEFILEGATHER", WaitTimeMs = 1000, WaitCount = 4 }]
        };

        ScoreOnly(stmt);

        var benefit = stmt.WaitBenefits.Single(b => b.WaitType == "PREEMPTIVE_OS_WRITEFILEGATHER");
        Assert.Equal(90.2, benefit.MaxBenefitPercent);
        Assert.True(benefit.MaxBenefitPercent > 50.0, "the new formula must score well above the old elapsed-cpu formula's 50%.");
    }

    // ---- a non-external wait: routing is unchanged ---------------------------------------------

    [Fact]
    public void NonExternalWait_TakesTheOldRoute_Unchanged()
    {
        // Serial statement, ordinary I/O wait: still the plain wait/elapsed ratio.
        var stmt = new PlanStatement
        {
            QueryTimeStats = new QueryTimeInfo { CpuTimeMs = 200, ElapsedTimeMs = 1000 },
            WaitStats = [new WaitStatInfo { WaitType = "PAGEIOLATCH_SH", WaitTimeMs = 800, WaitCount = 100 }]
        };

        ScoreOnly(stmt);

        var benefit = stmt.WaitBenefits.Single(b => b.WaitType == "PAGEIOLATCH_SH");
        Assert.Equal(80.0, benefit.MaxBenefitPercent);
    }

    // ---- GetOperatorMaxThreadOwnCpuMs: serial self-cpu subtracts child cpu ---------------------

    [Fact]
    public void GetOperatorMaxThreadOwnCpuMs_SerialNode_SubtractsChildCpu()
    {
        var child = new PlanNode { NodeId = 1, PhysicalOp = "Index Seek", HasActualStats = true, ActualCPUMs = 30 };
        var parent = new PlanNode
        {
            NodeId = 0,
            PhysicalOp = "Nested Loops",
            HasActualStats = true,
            ActualCPUMs = 100,
            Children = [child]
        };

        Assert.Equal(70, PlanAnalyzer.GetOperatorMaxThreadOwnCpuMs(parent));
    }

    // ---- through the product's own call path: parse -> analyze -> score ------------------------

    [Fact]
    public void ThroughTheParsedShowPlanPipeline_AnExternalWaitIsScored()
    {
        var xml =
            $"<ShowPlanXML xmlns=\"{Ns}\"><BatchSequence><Batch><Statements>" +
            "<StmtSimple StatementText=\"SELECT a FROM dbo.t\" StatementType=\"SELECT\">" +
            "<QueryPlan>" +
            "<WaitStats><Wait WaitType=\"MEMORY_ALLOCATION_EXT\" WaitTimeMs=\"500\" WaitCount=\"10\" /></WaitStats>" +
            "<QueryTimeStats CpuTime=\"1000\" ElapsedTime=\"1000\" />" +
            "</QueryPlan>" +
            "</StmtSimple>" +
            "</Statements></Batch></BatchSequence></ShowPlanXML>";

        var plan = ShowPlanParser.Parse(xml);
        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);

        var stmt = plan.Batches.Single().Statements.Single();
        var warning = WaitWarning(stmt, "MEMORY_ALLOCATION_EXT");
        Assert.NotNull(warning);
    }
}
