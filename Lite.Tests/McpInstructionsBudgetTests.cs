/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3898 Phase 2 (D2, D5, D6): Lite moves in lockstep with Darling's instructions rewrite. See
/// <c>DarlingMcpInstructionsBudgetTests</c> for the full rationale — the per-tool "Tool Reference" tables and
/// prose are gone because nothing enforced they stayed in sync with the tool descriptions that are the real
/// interface. A red-watch confirmed this fails against the pre-#3898-Phase-2 instructions (52,942 characters,
/// with the old per-family table headings present).
/// </summary>
public sealed class McpInstructionsBudgetTests
{
    private const int InstructionsBudget = 8_000;

    [Fact]
    public void Instructions_AreWithinTheD2Budget()
    {
        var length = McpInstructions.Text.Length;
        Assert.True(length <= InstructionsBudget,
            $"McpInstructions.Text is {length} characters; D2's budget is {InstructionsBudget}");
    }

    [Fact]
    public void Instructions_CarryNoPerFamilyToolReferenceTable()
    {
        var text = McpInstructions.Text;
        Assert.DoesNotContain("### Wait Statistics Tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Query Performance Tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Blocking & Deadlock Tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("| Tool | Purpose | Key Parameters |", text, StringComparison.Ordinal);
    }
}
