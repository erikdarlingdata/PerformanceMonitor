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
/// #3898 D3 head pins for Lite's <c>get_deadlocks</c> (a twin; Darling's copy is pinned in
/// <c>Darling.Tests/McpToolGuideHeads.Deadlocks.cs</c>) and <c>get_blocked_process_reports</c> (Lite-only;
/// get_blocking's counterpart, with no Darling twin under this name). Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsDeadlocksTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_blocked_process_reports",
        "get_deadlocks",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("get_blocked_process_reports", "not_collected wins if the engine can't run blocked_process_report"),
        ("get_blocked_process_reports", "else empty means none in the window, or none collected in it"),
        ("get_blocked_process_reports", "limit caps ROWS, not hours_back"),
        ("get_blocked_process_reports", "wait_time_ms is milliseconds"),
        ("get_blocked_process_reports", "last_tran/last_batch stamps are de-skewed for direct comparison to event_time"),
        ("get_deadlocks", "not_collected wins if the engine can't run deadlocks"),
        ("get_deadlocks", "then precondition names a fixable gap"),
        ("get_deadlocks", "else empty means none in the window, or none collected in it"),
        ("get_deadlocks", "limit caps ROWS, not hours_back"),
        ("get_deadlocks", "Darling: dedup_key scans the whole window before limit"),
        ("get_deadlocks", "a no-match answer is still empty"),
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

    /// <summary>Lite's get_deadlocks has no dedup_key parameter (that is Darling-only), so the shared head's
    /// "Darling: dedup_key ..." clause names a parameter this SKU's own served schema does not have. D6: the
    /// head still has to be true and byte-identical on both products.</summary>
    [Fact]
    public void GetDeadlocks_HasNoDedupKeyParameter()
    {
        var served = McpToolGuideTests.Served("get_deadlocks");
        Assert.DoesNotContain(served.ParameterDescriptionLengths, p => p.Parameter == "dedup_key");
    }
}
