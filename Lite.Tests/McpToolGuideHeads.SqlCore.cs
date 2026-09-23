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
/// #3898 D3 head pins for the "sqlCore" family's Lite twins: <c>analyze_server</c> and <c>compare_analysis</c>.
/// Darling's twin is <c>Darling.Tests/McpToolGuideHeadsSqlCoreTests</c>, which also holds the cross-SKU lockstep
/// pin. Both apps' tool bodies mirror each other field-for-field, so the original prose (and now the head and
/// tail) is byte-identical on both SKUs.
/// </summary>
public sealed class McpToolGuideHeadsSqlCoreTests
{
    private static readonly string[] ConvertedTools =
    [
        "analyze_server",
        "compare_analysis",
    ];

    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("analyze_server", "confidence is evidence, not probability or diagnosis"),
        ("analyze_server", "Needs 24h+ history, else insufficient_data"),
        ("analyze_server", "unavailable: window unobserved (dead collector), no verdict either way"),
        ("analyze_server", "empty: window observed, nothing fired (a true all-clear)"),
        ("analyze_server", "Unanchored runs persist findings to the store; as_of runs are exploratory, not saved"),
        ("analyze_server", "remediation_command is advisory, never auto-executed"),
        ("compare_analysis", "must exceed hours_back, else refused"),
        ("compare_analysis", "N=1 vs N=1: never proves causation"),
        ("compare_analysis", "unavailable = BOTH windows had zero facts"),
        ("compare_analysis", "coverage_caveat flags a partly-collected side"),
        ("compare_analysis", "band_source/band_rules say which"),
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

    /// <summary>D2: <c>baseline_hours_back</c>'s description was trimmed from 211 to 139 characters to clear the
    /// 200-character parameter cap; the sentence it dropped (the baseline period is always as long as the
    /// comparison period) rides on <c>compare_analysis</c>'s own tail instead of being lost.</summary>
    [Fact]
    public void CompareAnalysis_TailCarriesTheBaselineDurationRuleTrimmedFromTheParameter()
    {
        Assert.Contains("same duration as the comparison period", McpToolGuideTests.Served("compare_analysis").Tail!, StringComparison.Ordinal);
    }
}
