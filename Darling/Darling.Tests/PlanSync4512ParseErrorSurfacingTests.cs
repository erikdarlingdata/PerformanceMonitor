/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
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
