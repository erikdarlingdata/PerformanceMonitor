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
/// #3898 D3 head pins for <c>validate_custom_view</c> and <c>run_custom_view_panel</c> (Darling only; neither
/// has a Lite twin). Follows the <see cref="McpToolGuideHeadsHealthParserTests"/> pattern: per-tool guardrail
/// facts pinned in the head, and the load-bearing facts that moved to the tail (rather than being restated)
/// pinned there instead, using <see cref="McpToolGuideTests"/>'s shared <see cref="McpToolGuideTests.Served"/>
/// helper.
/// </summary>
public sealed class McpToolGuideHeadsCustomViewTests
{
    private const int ValidateCustomViewServedCap = 561;
    private const int RunCustomViewPanelServedCap = 609;

    /// <summary>Guardrail facts the head alone must carry for each tool: what a caller could otherwise misread
    /// about the answer (dry-run vs write, exhaustiveness, strictness, status shape, and what an absent
    /// <c>notice</c> means).</summary>
    private static readonly (string Tool, string Fact)[] HeadFacts =
    [
        ("validate_custom_view", "WITHOUT persisting anything"),
        ("validate_custom_view", "naming only the FIRST problem found; fixing it can reveal more"),
        ("validate_custom_view", "Unknown keys are ERRORS at every level (root, panel, cell, filter, overlay), never silently dropped"),
        ("validate_custom_view", "A notebook cell is FLAT (carries the panel's own keys directly); only run_custom_view_panel's spec nests under 'panel'."),
        ("run_custom_view_panel", "with no 'status' field on success"),
        ("run_custom_view_panel", "Failures return {status, message}: \"invalid\" for a bad spec or panel, or a failed or timed-out query; \"error\" for an internal fault."),
        ("run_custom_view_panel", "notice is a partial-window caveat: the tier's retention could not cover the window, or the panel's own data starts after the window does, or the row cap truncated the result; absent means the window was served whole."),
        ("run_custom_view_panel", "Window ends now: 'hours' (default 24)"),
        ("run_custom_view_panel", "Only 'panel' is required."),
    ];

    /// <summary>D9: facts the ORIGINAL description carried that are load-bearing but didn't fit the 620-char
    /// head, so they moved to the tail verbatim rather than being dropped or paraphrased away.</summary>
    private static readonly (string Tool, string Fact)[] TailCarriesFacts =
    [
        ("validate_custom_view", "with a did-you-mean for a near-miss"),
        ("validate_custom_view", "This is the exact authority create_custom_view / update_custom_view run before saving"),
        ("run_custom_view_panel", "This is the SAME compile-and-run the web composer's live preview uses"),
        ("run_custom_view_panel", "the panel is validated, compiled to catalog-only bound SQL, and executed against the collected store under a statement_timeout"),
        // The window and server sentences the head gave up so the three-cause notice contract fits its budget.
        ("run_custom_view_panel", "omit 'server' (or use \"All\") for the whole fleet"),
        ("run_custom_view_panel", "the span is capped at 90 days, and old windows are served from the retention rollups automatically"),
    ];

    /// <summary>What <c>notice</c> means, said the same way in the head and in the tail: a partial-window caveat
    /// with three causes, and its absence means the window was served whole (#4953). The old wording named two
    /// causes (retention, the row cap), so a caller read a panel whose own data started late as a complete one.</summary>
    private static readonly string[] NoticeContract =
    [
        "notice is a partial-window caveat",
        "the tier's retention could not cover the window",
        "the panel's own data starts after the window does",
        "the row cap truncated the result",
        "absent means the window was served whole",
    ];

    [Fact]
    public void BothHeads_StayUnderTheirCap_AndCarryTheirGuardrailFacts()
    {
        var validate = McpToolGuideTests.Served("validate_custom_view");
        Assert.NotNull(validate.Tail);
        Assert.True(validate.Served.Length <= ValidateCustomViewServedCap,
            $"validate_custom_view: served head {validate.Served.Length} is over the {ValidateCustomViewServedCap} target");
        Assert.EndsWith(McpToolGuide.GuidePointer, validate.Served, StringComparison.Ordinal);

        var run = McpToolGuideTests.Served("run_custom_view_panel");
        Assert.NotNull(run.Tail);
        Assert.True(run.Served.Length <= RunCustomViewPanelServedCap,
            $"run_custom_view_panel: served head {run.Served.Length} is over the {RunCustomViewPanelServedCap} target");
        Assert.EndsWith(McpToolGuide.GuidePointer, run.Served, StringComparison.Ordinal);

        foreach (var (tool, fact) in HeadFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Served, StringComparison.Ordinal);
        }

        Assert.All(new[] { "validate_custom_view", "run_custom_view_panel" },
            tool => Assert.All(McpToolGuideTests.Served(tool).ParameterDescriptionLengths, p => Assert.True(p.Length <= 200)));
    }

    /// <summary>The contract of <c>notice</c> is in BOTH places a caller reads it: the served head names all three
    /// causes and what an absent notice means, and the tail says the same.</summary>
    [Fact]
    public void TheNoticeContract_NamesAllThreeCauses_InTheHeadAndInTheTail()
    {
        var run = McpToolGuideTests.Served("run_custom_view_panel");
        Assert.NotNull(run.Tail);

        foreach (var fact in NoticeContract)
        {
            Assert.Contains(fact, run.Served, StringComparison.Ordinal);
            Assert.Contains(fact, run.Tail!, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("neither happened", run.Served, StringComparison.Ordinal);
    }

    /// <summary>Nothing the old descriptions said was lost: every fact the head had no room for is still in the
    /// tail, verbatim, so one <c>get_tool_guide</c> call recovers it.</summary>
    [Fact]
    public void BothTails_StillCarryTheFactsTheHeadHadNoRoomFor()
    {
        foreach (var (tool, fact) in TailCarriesFacts)
        {
            Assert.Contains(fact, McpToolGuideTests.Served(tool).Tail!, StringComparison.Ordinal);
        }
    }
}
