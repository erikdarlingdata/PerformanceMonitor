/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
using System.Text.RegularExpressions;
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
        /* #5779: the last single-row table (get_query_store_health's capture modes) and the memory-pressure
           section went too; those tools' own descriptions carry both. */
        Assert.DoesNotContain("| Tool | Note |", text, StringComparison.Ordinal);
        Assert.DoesNotContain("## Memory pressure signals", text, StringComparison.Ordinal);
    }

    /// <summary>Twin of Darling's <c>DarlingMcpInstructionsBudgetTests.ShoutedEmphasis</c> (#5779). "CRITICAL"
    /// alone stays legal: it is also a severity value some tools return.</summary>
    private static readonly Regex ShoutedEmphasis = new(@"CRITICAL( on |:| —| for )|IMPORTANT|on this fleet", RegexOptions.CultureInvariant);

    /// <summary>#5779: neither the server instructions nor any served tool description shouts CRITICAL or
    /// IMPORTANT, or says "on this fleet".</summary>
    [Fact]
    public void InstructionsAndToolDescriptions_UsePlainEmphasis()
    {
        Assert.DoesNotMatch(ShoutedEmphasis, McpInstructions.Text);

        var offenders = global::Lite.Tests.McpToolsListBudgetTests.Measure().Tools
            .Where(t => t.Description is not null && ShoutedEmphasis.IsMatch(t.Description))
            .Select(t => $"{t.Name}: \"{ShoutedEmphasis.Match(t.Description!).Value}\"")
            .ToList();
        Assert.True(offenders.Count == 0, "tool descriptions with shouted emphasis: " + string.Join("; ", offenders));
    }
}
