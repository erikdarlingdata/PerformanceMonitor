/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 Phase 2 (D2, D5): the server instructions dropped their per-tool "Tool Reference" tables and prose —
/// duplicated content nothing enforced stayed in sync with the tool descriptions that are the real interface
/// (#3696, #3821 both drifted). D2's target is 8,000 characters or less; this pins the shape rather than a
/// single measured number, so a future PR can grow the instructions back toward the old size one paragraph
/// at a time without ever noticing it crossed the line back into a "Tool Reference" table. A red-watch
/// confirmed this fails against the pre-#3898-Phase-2 instructions (88,532 / 52,942 characters, and the old
/// "### Diagnostic-analysis tools" family headers present).
/// </summary>
public sealed class DarlingMcpInstructionsBudgetTests
{
    private const int InstructionsBudget = 8_000;

    [Fact]
    public void Instructions_AreWithinTheD2Budget()
    {
        var length = DarlingMcpInstructions.Text.Length;
        Assert.True(length <= InstructionsBudget,
            $"DarlingMcpInstructions.Text is {length} characters; D2's budget is {InstructionsBudget}");
    }

    /// <summary>No fleet-coverage declaration reproduces <see cref="DarlingMcpInstructions.Text"/> exactly
    /// (<c>DarlingPeerDisclosureTests</c> pins that identity); this just confirms the budget holds through the
    /// public entry point a real host calls, not only the internal constant.</summary>
    [Fact]
    public void Build_WithNoDeclaredPeers_StaysWithinTheD2Budget()
    {
        var length = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty).Length;
        Assert.True(length <= InstructionsBudget,
            $"DarlingMcpInstructions.Build with no peers is {length} characters; D2's budget is {InstructionsBudget}");
    }

    /// <summary>The defect this whole phase fixes: a "Tool Reference" table restates what a tool's own
    /// description already says, and nothing enforces that the copies agree (#3696, #3821). This fails if
    /// either the old family headings or the old three-column table shape comes back wholesale — the one
    /// deliberately-kept single-row "Notes" table (<c>get_query_store_health</c>'s capture-mode footnote,
    /// <c>DarlingMcpConfigHistoryToolsTests</c>' pin) is two columns and carries no "Key Parameters" header, so
    /// it does not trip this.</summary>
    [Fact]
    public void Instructions_CarryNoPerFamilyToolReferenceTable()
    {
        var text = DarlingMcpInstructions.Text;
        Assert.DoesNotContain("### Diagnostic-analysis tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Core data-read tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Trend data-read tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("| Tool | Purpose | Key Parameters |", text, StringComparison.Ordinal);
    }
}
