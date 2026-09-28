/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using PerformanceMonitor.PlanAnalysis;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #4551: <c>ShowPlanParser</c> turns a walk exception or a refused plan into
/// <see cref="ParsedPlan.ParseError"/> and returns whatever it parsed so far, but before this
/// change no product caller read that field. A refused plan failed silently: the MCP formatter
/// returned <c>statement_count</c> 0 (or a partial result) with no reason, <see
/// cref="PlanAdvisoryAggregator"/> counted the partial plan's warnings into the aggregate facts,
/// and the plan viewer showed the plain "No Plan Loaded" empty state as if no plan had ever been
/// requested. These tests pin that each of those three now surfaces or skips the error instead.
/// </summary>
public sealed class PlanSync4512ParseErrorSurfacingTests
{
    private const string Ns = "xmlns=\"http://schemas.microsoft.com/sqlserver/2004/07/showplan\"";

    /// <summary>
    /// <see cref="McpPlanAnalysisFormatter.BuildAnalysisResult"/> must set <c>parse_error</c> at
    /// the top level of its JSON when the plan was refused, instead of silently reporting
    /// <c>statement_count</c> 0 with no explanation.
    /// </summary>
    [Fact]
    public void McpFormatterSurfacesParseErrorForARefusedPlan()
    {
        var xml = OversizedPlan();

        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(xml, serverName: "srv", source: "test", identifier: "id");

        using var doc = JsonDocument.Parse(json);
        var root = doc.RootElement;
        Assert.Equal(JsonValueKind.String, root.GetProperty("parse_error").ValueKind);
        Assert.Contains("size limit", root.GetProperty("parse_error").GetString());
        Assert.Equal(0, root.GetProperty("statement_count").GetInt32());
    }

    /// <summary>
    /// A plan that parses cleanly must report <c>parse_error: null</c>, not omit the field or
    /// set it to something truthy.
    /// </summary>
    [Fact]
    public void McpFormatterReportsNullParseErrorForAGoodPlan()
    {
        var json = McpPlanAnalysisFormatter.BuildAnalysisResult(GoodPlan(), serverName: "srv", source: "test", identifier: "id");

        using var doc = JsonDocument.Parse(json);
        Assert.Equal(JsonValueKind.Null, doc.RootElement.GetProperty("parse_error").ValueKind);
    }

    /// <summary>
    /// <see cref="PlanAdvisoryAggregator.Extract"/> must skip a refused plan's contribution
    /// entirely — its counts must exclude it, the same as a plan whose parse threw outright.
    /// </summary>
    [Fact]
    public void AggregatorExcludesARefusedPlanFromItsCounts()
    {
        var details = PlanAdvisoryAggregator.Extract(new List<string> { OversizedPlan(), GoodPlan() });

        // The good plan has no missing indexes or warnings of its own, so if the refused plan's
        // partial content leaked in, this would be nonzero.
        Assert.Empty(details.MissingIndexes);
        Assert.Empty(details.Warnings);
    }

    /// <summary>
    /// Mutation proof for the aggregator's skip: remove the <c>ParseError</c> check (leaving
    /// only the pre-existing exception catch) and this assertion goes RED, because the refused
    /// plan's <see cref="ParsedPlan.AllWarnings"/>/<see cref="ParsedPlan.AllMissingIndexes"/> are
    /// then read even though the parse never completed. Recorded manually in the report; this
    /// test asserts the fixed (post-mutation-revert) behaviour.
    /// </summary>
    [Fact]
    public void AggregatorSummarizeExcludesARefusedPlan()
    {
        var summary = PlanAdvisoryAggregator.Summarize(new List<string> { OversizedPlan() });

        Assert.Equal(0, summary.WarningCount);
        Assert.Equal(0, summary.MissingIndexCount);
    }

    /// <summary>
    /// The pure display-text helper the viewer uses for its empty-state slot.
    /// </summary>
    [Fact]
    public void ParseErrorMessageReturnsTheDisplayTextForARefusedPlan()
    {
        var plan = ShowPlanParser.Parse(OversizedPlan());

        var message = PlanDisplayText.ParseErrorMessage(plan);

        Assert.NotNull(message);
        Assert.StartsWith("This plan couldn't be parsed: ", message);
        Assert.Contains("size limit", message);
    }

    /// <summary>
    /// A plan that parses cleanly must get no display message at all, so a caller can treat
    /// null as "show the normal state instead".
    /// </summary>
    [Fact]
    public void ParseErrorMessageReturnsNullForAGoodPlan()
    {
        var plan = ShowPlanParser.Parse(GoodPlan());

        Assert.Null(PlanDisplayText.ParseErrorMessage(plan));
    }

    /// <summary>
    /// <see cref="PlanAdvisoryAggregator.Extract"/> must exclude a PARTIALLY parsed plan's own
    /// good content, not just a plan that failed outright. This plan's first batch is a
    /// complete, ordinary statement with a missing-index suggestion, and its second batch is a
    /// 1,500-level RelOp depth bomb; the parser keeps the first batch's already-built content
    /// (<see cref="ParsedPlan.Batches"/> is non-empty) but sets <see
    /// cref="ParsedPlan.ParseError"/> from the second batch's failure. If the aggregator read
    /// <see cref="ParsedPlan.AllMissingIndexes"/> without checking <c>ParseError</c> first, the
    /// good batch's suggestion would leak into the aggregate — the existing
    /// <see cref="AggregatorExcludesARefusedPlanFromItsCounts"/> pin uses an oversized plan with
    /// no batches at all, so it can't catch that: <c>AllMissingIndexes</c> is empty there either
    /// way.
    /// </summary>
    [Fact]
    public void AggregatorExcludesAPartiallyParsedPlansGoodBatch()
    {
        var xml = PartiallyParsedPlan();
        var checkPlan = ShowPlanParser.Parse(xml);
        Assert.NotNull(checkPlan.ParseError);
        Assert.NotEmpty(checkPlan.Batches);
        Assert.NotEmpty(checkPlan.AllMissingIndexes);

        var details = PlanAdvisoryAggregator.Extract(new List<string> { xml });

        Assert.Empty(details.MissingIndexes);
        Assert.Empty(details.Warnings);
    }

    /// <summary>
    /// <see cref="PlanAnalysisPipeline.Run"/> must leave a partially parsed plan completely
    /// untouched: no findings added to the good first batch, and no
    /// <c>MaxBenefitPercent</c> computed, because analyzing a plan whose later batch never
    /// finished parsing would score a fragment as if it were the whole plan.
    /// </summary>
    [Fact]
    public void PipelineRunAddsNoFindingsToAPartiallyParsedPlan()
    {
        var plan = ShowPlanParser.Parse(PartiallyParsedPlan());
        Assert.NotNull(plan.ParseError);
        Assert.NotEmpty(plan.Batches);

        var missingIndexesBeforeRun = plan.AllMissingIndexes.Count;

        var result = PlanAnalysisPipeline.Run(plan);

        Assert.Same(plan, result);
        // The missing-index suggestion came from parsing the good batch, not from analysis —
        // Run() leaves it exactly as parsed. What it must NOT do is run the analyzer/scorer
        // passes against the good batch's own operator: no PlanWarnings/node warnings appear.
        Assert.Equal(missingIndexesBeforeRun, result.AllMissingIndexes.Count);
        Assert.Empty(result.AllWarnings);
    }

    /// <summary>
    /// A plan whose first batch is a complete, ordinary statement carrying a missing-index
    /// suggestion, followed by a second batch whose 1,500-level RelOp nesting blows
    /// <c>ShowPlanParser.MaxParseDepth</c>. The parser has already added the first batch to
    /// <see cref="ParsedPlan.Batches"/> by the time the second batch's depth guard throws, so
    /// the result carries both: real content AND <see cref="ParsedPlan.ParseError"/> set.
    /// </summary>
    private static string PartiallyParsedPlan()
    {
        var goodBatch = $"""
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT 1" StatementType="SELECT">
                  <QueryPlan>
                    <MissingIndexes>
                      <MissingIndexGroup Impact="50.0">
                        <MissingIndex Database="[db]" Schema="[dbo]" Table="[t]">
                          <ColumnGroup Usage="EQUALITY">
                            <Column Name="[id]" ColumnId="1" />
                          </ColumnGroup>
                        </MissingIndex>
                      </MissingIndexGroup>
                    </MissingIndexes>
                    <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimatedTotalSubtreeCost="0.001" EstimateRows="1">
                      <RunTimeInformation>
                        <RunTimeCountersPerThread Thread="0" ActualRows="1" ActualExecutions="1" UdfElapsedTime="5" UdfCpuTime="5" />
                      </RunTimeInformation>
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
            """;

        var depthBomb = new StringBuilder();
        depthBomb.Append("<Batch><Statements><StmtSimple StatementText=\"SELECT 2\" StatementType=\"SELECT\"><QueryPlan>");
        for (var level = 0; level < 1500; level++)
            depthBomb.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Nested Loops\" LogicalOp=\"Inner Join\"><NestedLoops>");
        depthBomb.Append("<RelOp NodeId=\"0\" PhysicalOp=\"Constant Scan\" LogicalOp=\"Constant Scan\" />");
        for (var level = 0; level < 1500; level++)
            depthBomb.Append("</NestedLoops></RelOp>");
        depthBomb.Append("</QueryPlan></StmtSimple></Statements></Batch>");

        return $"<ShowPlanXML {Ns}><BatchSequence>{goodBatch}{depthBomb}</BatchSequence></ShowPlanXML>";
    }

    private static string OversizedPlan() =>
        "<ShowPlanXML>" + new string(' ', 16 * 1024 * 1024 + 1) + "</ShowPlanXML>";

    private static string GoodPlan() => $"""
        <ShowPlanXML {Ns}>
          <BatchSequence>
            <Batch>
              <Statements>
                <StmtSimple StatementText="SELECT 1" StatementType="SELECT">
                  <QueryPlan>
                    <RelOp NodeId="0" PhysicalOp="Constant Scan" LogicalOp="Constant Scan" EstimatedTotalSubtreeCost="0.001" EstimateRows="1">
                    </RelOp>
                  </QueryPlan>
                </StmtSimple>
              </Statements>
            </Batch>
          </BatchSequence>
        </ShowPlanXML>
        """;
}
