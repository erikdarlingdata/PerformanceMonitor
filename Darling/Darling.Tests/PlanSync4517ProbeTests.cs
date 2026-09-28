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
/// Standalone probe for #4517's pipeline fact, built only against members that
/// exist on dev before #4517 lands: a plan tree runs through
/// <c>PlanAnalyzer.Analyze</c> then <c>BenefitScorer.Score</c>, then reads a
/// PREEMPTIVE_* wait's benefit off <c>stmt.WaitBenefits</c>. None of #4517's own
/// new members (<c>BenefitScorer.IsExternalWait</c>,
/// <c>PlanAnalyzer.GetOperatorMaxThreadOwnCpuMs</c>) appear here — this exercises
/// only the pre-existing per-thread elapsed-minus-cpu wait math dev already ships.
///
/// <para>Run against a detached <c>origin/dev</c> worktree (before #4517), this
/// probe fails with Expected: 90.2, Actual: 50 — the old per-thread
/// elapsed-minus-cpu formula's answer for this exact shape, versus #4517's new
/// CPU-share formula's 90.2%.</para>
/// </summary>
public sealed class PlanSync4517ProbeTests
{
    [Fact]
    public void PreemptiveWaitOnACpuBusyThread_ScoresLowUnderTheOldElapsedMinusCpuFormula()
    {
        // Same shape as #4517's worked example: Node A has two threads both
        // CPU-busy in the kernel (elapsed == cpu), so the OLD per-thread
        // elapsed-minus-cpu formula sees ~0 self-wait time on the busy thread.
        // #4517's own comment records the old formula's answer for this exact
        // shape as 50%; the new CPU-share formula gives 90.2% instead.
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

        var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
        PlanAnalyzer.Analyze(plan);
        BenefitScorer.Score(plan);

        var benefit = stmt.WaitBenefits.Single(b => b.WaitType == "PREEMPTIVE_OS_WRITEFILEGATHER");

        // #4517's new CPU-share formula puts this at 90.2%.
        Assert.Equal(90.2, benefit.MaxBenefitPercent);
    }
}
