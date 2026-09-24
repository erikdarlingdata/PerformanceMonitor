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
/// #3898 D3 head pins for <c>get_deadlocks</c>, a twin converted with a byte-identical head on both products.
/// <c>get_blocked_process_reports</c> (Lite-only, get_blocking's counterpart there) is pinned alongside Lite's
/// own copy of this tool in <c>Lite.Tests/McpToolGuideHeads.Deadlocks.cs</c>. Follows the pattern in
/// <see cref="McpToolGuideHeadsHealthParserTests"/>.
/// </summary>
public sealed class McpToolGuideHeadsDeadlocksTests
{
    private static readonly string[] ConvertedTools =
    [
        "get_deadlocks",
    ];

    /// <summary>The per-tool guardrail phrase each head must state.</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
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

    /// <summary>The dedup_key parameter description was over the D2 200-char cap (406, now 191): the same shape
    /// PR #4108 (get_blocking) already uses, kept for consistency within this file. Its "BEFORE limit" and
    /// "display name" scoping sentences both stay on the parameter, because two existing pins read them straight
    /// off the parameter attribute (<see cref="McpPageContractTests"/>'s <c>FingerprintReaders_NameTheScanCeilingFields</c>
    /// and <c>DarlingMcpBlockingToolsSurfaceAndSqlTests.ParamContract_DedupKeyDescription_AdvertisesItsScoping</c>);
    /// the full original parameter text (minus its issue ref) is also repeated verbatim in the tool's own tail,
    /// labeled "dedup_key:", so nothing the parameter used to say is lost.</summary>
    [Fact]
    public void DedupKeyParam_OverflowMovedVerbatim_IntoTheTail()
    {
        var served = McpToolGuideTests.Served("get_deadlocks");
        Assert.Contains(served.ParameterDescriptionLengths, p => p.Parameter == "dedup_key" && p.Length <= 200);
        Assert.Contains(
            "dedup_key: Optional alert fingerprint (the alert's Dedup Key). When supplied, returns only the incident with that key — paste it straight from an alert or ticket instead of scanning the window. The key is scoped to the server's display name and the incident's involved objects. The fingerprint scan runs over the window BEFORE limit, up to the scan ceiling the payload reports as rows_examined / scan_truncated.",
            served.Tail!, StringComparison.Ordinal);
    }
}
