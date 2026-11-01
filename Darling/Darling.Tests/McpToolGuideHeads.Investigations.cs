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
/// #5632 Phase A head pins for the four investigation tools (<c>list_investigation_requests</c>,
/// <c>claim_investigation_request</c>, <c>submit_investigation_report</c>, <c>get_investigation_reports</c>), Darling's
/// side (Lite's twin, <c>Lite.Tests/McpToolGuideHeads.Investigations.cs</c>, checks the head length and the guide, #5820). Follows the alerting family's pattern
/// (<see cref="McpToolGuideHeadsAlertingTests"/>): each head says what the tool does, whether it writes and its status
/// words, and the facts that moved off the head to keep tools/list inside its budget stay in the guide tail.
/// </summary>
public sealed class McpToolGuideHeadsInvestigationsTests
{
    private static readonly string[] InvestigationTools =
    [
        "list_investigation_requests",
        "claim_investigation_request",
        "submit_investigation_report",
        "get_investigation_reports",
    ];

    [Fact]
    public void EveryInvestigationHead_StaysAtOrUnder300Characters_AndPointsToTheGuide()
    {
        foreach (var tool in InvestigationTools)
        {
            var served = McpToolGuideTests.Served(tool);
            Assert.NotNull(served.Tail);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 300, $"{tool}: served head {served.Served.Length} is over the 300 target");
        }
    }

    /// <summary>The two reads say they write nothing; the two writes say they write only investigation state; every head
    /// names its status words.</summary>
    [Fact]
    public void EveryHead_SaysWhatItWrites_AndNamesItsStatusWords()
    {
        var list = McpToolGuideTests.Served("list_investigation_requests").Served;
        Assert.Contains("Writes nothing.", list, StringComparison.Ordinal);
        Assert.Contains("status ok or empty", list, StringComparison.Ordinal);

        var claim = McpToolGuideTests.Served("claim_investigation_request").Served;
        Assert.Contains("Writes only investigation state.", claim, StringComparison.Ordinal);
        Assert.Contains("status claimed, renewed, already_claimed, not_open, not_found or", claim, StringComparison.Ordinal);

        var submit = McpToolGuideTests.Served("submit_investigation_report").Served;
        Assert.Contains("Writes only investigation state.", submit, StringComparison.Ordinal);
        Assert.Contains("status reported, invalid,", submit, StringComparison.Ordinal);
        Assert.Contains("lease_lost", submit, StringComparison.Ordinal);

        var reports = McpToolGuideTests.Served("get_investigation_reports").Served;
        Assert.Contains("status ok or empty", reports, StringComparison.Ordinal);
    }

    /// <summary>The facts that moved off the heads stay reachable in the tails.</summary>
    [Fact]
    public void Tails_CarryTheFactsTheHeadsCouldNotFit()
    {
        var list = McpToolGuideTests.Served("list_investigation_requests").Tail!;
        Assert.Contains("a lapsed lease reads open", list, StringComparison.Ordinal);
        Assert.Contains("the read writes nothing", list, StringComparison.Ordinal);
        Assert.Contains("self_monitor", list, StringComparison.Ordinal);

        var claim = McpToolGuideTests.Served("claim_investigation_request").Tail!;
        Assert.Contains("at most three claims", claim, StringComparison.Ordinal);
        Assert.Contains("claims afresh, with a new claim_id", claim, StringComparison.Ordinal);
        Assert.Contains("masked at your data detail level", claim, StringComparison.Ordinal);
        Assert.Contains("released (it stays counted)", claim, StringComparison.Ordinal);

        var submit = McpToolGuideTests.Served("submit_investigation_report").Tail!;
        Assert.Contains("verdict (real, false_positive, needs_person)", submit, StringComparison.Ordinal);
        Assert.Contains("invalid lists every missing or wrong field at once", submit, StringComparison.Ordinal);
        Assert.Contains("any action_proposals is refused", submit, StringComparison.Ordinal);
        Assert.Contains("proposed_note at most 2,000", submit, StringComparison.Ordinal);
        Assert.Contains("root_cause at most 8,000; evidence at most 50 items", submit, StringComparison.Ordinal);
        Assert.Contains("status refused, naming the field and the cap", submit, StringComparison.Ordinal);

        var reports = McpToolGuideTests.Served("get_investigation_reports").Tail!;
        Assert.Contains("stored as submitted and masked at answer time", reports, StringComparison.Ordinal);
        Assert.Contains("unreviewed and self-attributed", reports, StringComparison.Ordinal);
        Assert.Contains("at most 200 rows", reports, StringComparison.Ordinal);
    }
}
