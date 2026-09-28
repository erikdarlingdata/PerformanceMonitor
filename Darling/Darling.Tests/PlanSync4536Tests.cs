/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Globalization;
using System.Linq;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4536 — the parallel-skew message formatted a percentage with <c>:P0</c>, which is
/// culture-sensitive in the space before the sign (en-US -&gt; "100%", InvariantCulture ->
/// "100 %"). Ported from erikdarlingdata/PerformanceStudio@582c88e's <c>{pct * 100:N0}%</c>
/// replacement.
/// </summary>
public sealed class PlanSync4536Tests
{
    [Fact]
    public void ParallelSkewMessage_UnderInvariantCulture_HasNoSpaceBeforeThePercentSign()
    {
        var original = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

            var node = new PlanNode
            {
                PhysicalOp = "Hash Match",
                LogicalOp = "Inner Join",
                PerThreadStats =
                [
                    new PerThreadRuntimeInfo { ThreadId = 1, ActualRows = 9999 },
                    new PerThreadRuntimeInfo { ThreadId = 2, ActualRows = 1 }
                ]
            };
            var stmt = new PlanStatement { RootNode = node };
            var plan = new ParsedPlan { Batches = [new PlanBatch { Statements = [stmt] }] };
            PlanAnalyzer.Analyze(plan);

            var resultNode = plan.Batches[0].Statements[0].RootNode!;
            var warning = Assert.Single(resultNode.Warnings.Where(w => w.WarningType == "Parallel Skew"));

            Assert.Contains("100%", warning.Message);
            Assert.DoesNotContain("100 %", warning.Message);
        }
        finally
        {
            CultureInfo.CurrentCulture = original;
        }
    }
}
