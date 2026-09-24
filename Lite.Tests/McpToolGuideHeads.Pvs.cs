/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D3 head pins for the "Pvs" family's Lite twin: <c>get_pvs_stats</c>. Darling's twin is
/// <c>Darling.Tests/McpToolGuideHeadsPvsTests</c>; the cross-SKU byte-identical-head check is the generic
/// D6 pin in <c>McpToolGuideTests</c>, not repeated here.
/// </summary>
public sealed class McpToolGuideHeadsPvsTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_pvs_stats",
    ];

    /// <summary>The guardrail phrases the head must state. The pct_of_database rule names both null
    /// causes and the zero denominator, because <c>PctReason</c> returns a reason for a missing OR zero database
    /// size, and a measured 0 MB only reads 0.00 against a known size.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_pvs_stats", "cleaner times (a start with no end = mid-run)"),
        ("get_pvs_stats", "LATEST IS A TIME: the newest snapshot, not a window; as_of is when it was taken."),
        ("get_pvs_stats", "looks back from now, not from as_of, over the top-5 databases by current size"),
        ("get_pvs_stats", "Cleaner times are de-skewed to UTC, like as_of."),
        ("get_pvs_stats", "pct_of_database is null only if PVS size is unmeasured (pvs_measured false) or database size is missing or 0"),
        ("get_pvs_stats", "else measured 0 MB = 0.00"),
        ("get_pvs_stats", "No rows: not_collected if this engine can't collect PVS, else empty"),
    ];

    [Fact]
    public void EveryConvertedHead_CarriesItsGuardrailFact_AndThePointer()
    {
        foreach (var tool in ConvertedTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 620, $"{tool}: served head {served.Served.Length} is over the 620 target");
            Assert.All(served.ParameterDescriptionLengths, p => Assert.True(p.Length <= 200, $"{tool}.{p.Parameter}: {p.Length} > 200"));
        }

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }
    }

    /// <summary>Fields and usage guidance the head has no room for stay in the tail, verbatim from the
    /// original: the online-index size and oldest-transaction-id fields, and the "use when" guidance.</summary>
    [Fact]
    public void FieldsAndUsageGuidance_NotRepeatedInHead_StayInTail()
    {
        var served = McpToolGuideTests.Served("get_pvs_stats");
        Assert.DoesNotContain("online-index version store size", served.Served, StringComparison.Ordinal);
        Assert.DoesNotContain("oldest active/aborted transaction ids", served.Served, StringComparison.Ordinal);
        Assert.Contains("online-index version store size", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("oldest active/aborted transaction ids", served.Tail!, StringComparison.Ordinal);
        Assert.Contains("Use when a database's size is growing without table growth", served.Tail!, StringComparison.Ordinal);
        /* Coordinator correction: PctReason also nulls the share for a zero denominator, so the tail names it. */
        Assert.Contains("or the denominator is absent or zero, with pct_of_database_reason saying which", served.Tail!, StringComparison.Ordinal);
    }
}
