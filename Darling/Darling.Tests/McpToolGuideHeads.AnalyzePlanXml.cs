/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D3 head pins for <c>analyze_plan_xml</c> (wave E, lane e3), a twin: Darling and Lite share the
/// exact same description (no server-scoped read, no store fetch — it parses the XML the caller passes
/// in). Follows the pattern in <see cref="McpToolGuideHeadsHealthParserTests"/>.
///
/// <para>
/// <c>Lite.Tests/McpToolGuideHeadsAnalyzePlanXmlTests</c> is the Lite twin. The cross-SKU
/// byte-identical-head check itself is the generic
/// <see cref="McpToolGuideTests.EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads"/>
/// pin; this file only pins the guardrail facts this tool's head must state.
/// </para>
///
/// <para>
/// <c>Lite.Tests/McpDescriptionTruthPinTests.PlanTools_DescribeTheStatedOperatorCut_AndTheCreateStatement</c>
/// (and its Darling twin in <c>DarlingMcpPlanToolsTests</c>) already re-points its read to
/// <c>McpToolGuide.Split(...).Head</c> for the three plan tools that enumerate the payload, including this
/// one — those fragments (impact_basis, create_statement, the corroboration/never-a-diagnosis clause, the
/// fixed caveat, the regression-risk clause, operators_ranked_by, operators_returned/total_operators/truncated)
/// are guardrails a caller needs before choosing the tool, so they stay in the HEAD, not the tail. This file
/// re-pins them here too, next to the tool they belong to.
/// </para>
/// </summary>
public sealed class McpToolGuideHeadsAnalyzePlanXmlTests
{
    private const string Tool = "analyze_plan_xml";

    /// <summary>The guardrail fragments the pre-existing truth pin (see class summary) requires in the HEAD,
    /// verbatim, plus this lane's own additions: what makes this tool different from the other plan tools (it
    /// takes XML directly, no server/store lookup) and the empty/refusal semantics a caller needs before
    /// calling it (D9: malformed input degrades to a zero-statement result rather than an error).</summary>
    private static readonly string[] HeadFacts =
    [
        "not a stored plan",
        "labelled impact_basis",
        "create_statement — the optimizer's suggested CREATE INDEX for this statement",
        "corroboration for a statement already measured slow, never a diagnosis",
        "every row carries the fixed caveat",
        "regression risk for other plans",
        "operators_ranked_by",
        "operators_returned / total_operators / truncated",
        "Malformed or non-plan XML doesn't error: statement_count 0.",
        "Blank plan_xml is refused.",
    ];

    [Fact]
    public void Head_CarriesEveryGuardrailFact_AndThePointer_AtOrUnder620()
    {
        var served = McpToolGuideTests.Served(Tool);
        Assert.NotNull(served.Tail);
        Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
        Assert.True(served.Served.Length <= 620, $"{Tool}: served head {served.Served.Length} is over the 620 target");

        foreach (var fact in HeadFacts)
        {
            Assert.Contains(fact, served.Served, StringComparison.Ordinal);
        }

        /* #3696's dropped suppression sentence must not come back (Lite.Tests/McpDescriptionTruthPinTests). */
        Assert.DoesNotContain("no CREATE INDEX text", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("a hint, not a design", served.Served, StringComparison.Ordinal);

        Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{Tool}.{p.Parameter}: {p.Length} > 200"));
    }

    /// <summary>Nothing dropped: the tail (served only by get_tool_guide) still opens with the original first
    /// sentence and carries the rest verbatim — no D4 removal applied here, since the original description
    /// carried no issue reference or anecdote to take off the wire.</summary>
    [Fact]
    public void Tail_CarriesTheOriginalSentencesVerbatim()
    {
        var tail = McpToolGuideTests.Served(Tool).Tail!;
        Assert.StartsWith("Analyzes raw showplan XML directly. Use when you have plan XML from any source ", tail, StringComparison.Ordinal);
        Assert.Contains("(clipboard, file, another tool). ", tail, StringComparison.Ordinal);
        Assert.Contains("otherwise the optimizer's cost_percent estimate.", tail, StringComparison.Ordinal);
    }
}
