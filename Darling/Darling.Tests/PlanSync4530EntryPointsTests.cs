/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Linq;
using System.Text.Json;
using System.Threading;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4530 step 2 — wiring the resolved server's <see cref="ServerMetadata"/> (edition, MAXDOP) into every
/// plan-analysis ENTRY POINT, so rule 38 (Standard Edition DOP 2 limitation, ported and pinned by
/// <see cref="PlanSync4530Tests"/>) can give its Warning instead of its uninformative Info branch at the MCP
/// tools and the drill-downs. This file pins the two shared-layer overloads the entry points call —
/// <see cref="McpPlanAnalysisFormatter.BuildAnalysisResult(string, string?, string, string?, ServerMetadata?, CancellationToken)"/>
/// and <see cref="PlanAdvisoryAggregator.ExtractCancellable(System.Collections.Generic.IEnumerable{string}, ServerMetadata?, CancellationToken)"/>
/// — actually carry the metadata through to the analyzer, rather than the rule's own logic (already covered).
/// </summary>
public sealed class PlanSync4530EntryPointsTests
{
    /// <summary>DOP 2, one batch-mode node (a columnstore scan). Rule 38's condition.</summary>
    private static string BuildBatchModeDop2Xml(bool maxdop2Hint = false) => $"""
        <ShowPlanXML xmlns="http://schemas.microsoft.com/sqlserver/2004/07/showplan" Version="1.564" Build="16.0.4215.2"><BatchSequence><Batch><Statements>
        <StmtSimple StatementText="SELECT SUM(a) FROM dbo.t{(maxdop2Hint ? " OPTION (MAXDOP 2)" : "")}" StatementId="1" StatementCompId="1" StatementType="SELECT" StatementSubTreeCost="5" StatementOptmLevel="FULL">
          <QueryPlan CachedPlanSize="16" CompileTime="1" CompileCPU="1" CompileMemory="104" DegreeOfParallelism="2">
            <RelOp NodeId="0" PhysicalOp="Hash Match" LogicalOp="Aggregate" EstimateRows="1" EstimateIO="0" EstimateCPU="0" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="0" Parallel="1" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Batch">
              <OutputList/>
              <RelOp NodeId="1" PhysicalOp="Columnstore Index Scan" LogicalOp="Columnstore Index Scan" EstimateRows="1000" EstimateIO="5" EstimateCPU="0.1" AvgRowSize="9" EstimatedTotalSubtreeCost="5" TableCardinality="1000" Parallel="1" EstimateRebinds="0" EstimateRewinds="0" EstimatedExecutionMode="Batch">
                <OutputList/>
              </RelOp>
            </RelOp>
          </QueryPlan>
        </StmtSimple>
        </Statements></Batch></BatchSequence></ShowPlanXML>
        """;

    /// <summary>
    /// The MCP-tool entry point: the formatter's metadata overload produces the Warning, and the 5-arg
    /// overload (no metadata, what the pasted-plan / analyze_plan_xml path uses) produces the Info, for
    /// the SAME plan XML.
    /// </summary>
    [Fact]
    public void BuildAnalysisResult_WithMetadata_StandardAndMaxDop8_ProducesWarning_NoMetadata_ProducesInfo()
    {
        var xml = BuildBatchModeDop2Xml();
        var metadata = new ServerMetadata { Edition = "Standard Edition", MaxDop = 8 };

        var withMetadata = McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "srv", "xml", null, metadata, CancellationToken.None);
        var withoutMetadata = McpPlanAnalysisFormatter.BuildAnalysisResult(xml, "srv", "xml", null, CancellationToken.None);

        using var withDoc = JsonDocument.Parse(withMetadata);
        using var withoutDoc = JsonDocument.Parse(withoutMetadata);

        var withWarnings = withDoc.RootElement.GetProperty("statements")[0].GetProperty("warnings")
            .EnumerateArray().Where(w => w.GetProperty("type").GetString() == "Standard Edition DOP Limitation").ToList();
        var withoutWarnings = withoutDoc.RootElement.GetProperty("statements")[0].GetProperty("warnings")
            .EnumerateArray().Where(w => w.GetProperty("type").GetString() == "Standard Edition DOP Limitation").ToList();

        Assert.Single(withWarnings);
        Assert.Equal("Warning", withWarnings[0].GetProperty("severity").GetString());
        Assert.Contains("MAXDOP is set to 8", withWarnings[0].GetProperty("message").GetString());

        Assert.Single(withoutWarnings);
        Assert.Equal("Info", withoutWarnings[0].GetProperty("severity").GetString());
    }

    /// <summary>
    /// The drill-down/aggregator entry point: <c>PlanAdvisoryAggregator</c>'s new metadata overload
    /// surfaces the rule-38 Warning in <c>Details.Warnings</c>; the metadata-less overload does not.
    /// </summary>
    [Fact]
    public void PlanAdvisoryAggregator_ExtractCancellable_WithMetadata_SurfacesRule38Warning()
    {
        var xml = BuildBatchModeDop2Xml();
        var metadata = new ServerMetadata { Edition = "Standard Edition", MaxDop = 8 };

        var withMetadata = PlanAdvisoryAggregator.ExtractCancellable([xml], metadata, CancellationToken.None);
        var withoutMetadata = PlanAdvisoryAggregator.ExtractCancellable([xml], CancellationToken.None);

        Assert.Contains(withMetadata.Warnings, w => w.RuleNumber == 38 && w.Severity == PlanWarningSeverity.Warning);
        Assert.DoesNotContain(withoutMetadata.Warnings, w => w.RuleNumber == 38 && w.Severity == PlanWarningSeverity.Warning);
    }
}
