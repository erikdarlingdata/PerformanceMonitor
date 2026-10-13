/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
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
[Trait("Reads", "Lite")]
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
    /// either the old family headings or the old three-column table shape comes back wholesale. #5779 also
    /// dropped the last single-row "Notes" table (<c>get_query_store_health</c>'s capture modes, which its own
    /// description carries), so no tool table of any shape is left.</summary>
    [Fact]
    public void Instructions_CarryNoPerFamilyToolReferenceTable()
    {
        var text = DarlingMcpInstructions.Text;
        Assert.DoesNotContain("### Diagnostic-analysis tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Core data-read tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("### Trend data-read tools", text, StringComparison.Ordinal);
        Assert.DoesNotContain("| Tool | Purpose | Key Parameters |", text, StringComparison.Ordinal);
        Assert.DoesNotContain("| Tool | Note |", text, StringComparison.Ordinal);
    }

    /// <summary>The emphasis words the instructions and descriptions use instead of saying the thing plainly
    /// (#5779), and the phrase that cites a measurement by the place it was taken. "CRITICAL" alone stays
    /// legal: it is also a severity value some tools return.</summary>
    internal static readonly Regex ShoutedEmphasis = new(@"CRITICAL( on |:| —| for )|IMPORTANT|on this fleet", RegexOptions.CultureInvariant);

    /// <summary>#5779: neither the server instructions nor any served tool description shouts CRITICAL or
    /// IMPORTANT, or says "on this fleet".</summary>
    [Fact]
    public void InstructionsAndToolDescriptions_UsePlainEmphasis()
    {
        Assert.DoesNotMatch(ShoutedEmphasis, DarlingMcpInstructions.Text);
        Assert.DoesNotMatch(ShoutedEmphasis, DarlingCoreToolProfile.CoreNote);

        var offenders = McpToolsListBudgetTests.Measure().Tools
            .Where(t => t.Description is not null && ShoutedEmphasis.IsMatch(t.Description))
            .Select(t => $"{t.Name}: \"{ShoutedEmphasis.Match(t.Description!).Value}\"")
            .ToList();
        Assert.True(offenders.Count == 0, "tool descriptions with shouted emphasis: " + string.Join("; ", offenders));
    }

    /* #5789: initialize said "This server exposes 182 tools" while tools/list served 160, because the census was a
       fixed catalog count and the write switches narrow the served set after it. The host now renders the census
       from the names before and after DarlingMcpWritePolicy.Apply; these pin that rendering for each switch state. */

    private static List<string> Catalog() => DarlingMcpWritePolicy.ToolNames(McpToolsListBudgetTests.BuildServedTools().Tools);

    private static List<string> Served(DarlingMcpWritePolicy.Effective policy)
    {
        var all = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in McpToolsListBudgetTests.BuildServedTools().Tools)
            all.Add(tool);
        return DarlingMcpWritePolicy.ToolNames(DarlingMcpWritePolicy.FilterToolCollection(all, policy));
    }

    [Fact]
    public void ServedCensus_WritesOff_CountsTheServedTools_AndSaysTheWritesAreOff()
    {
        var catalog = Catalog();
        var served = Served(DarlingMcpWritePolicy.Effective.Off);
        var writes = catalog.Count(n => DarlingMcpWritePolicy.IsWrite(n) && !DarlingMcpWritePolicy.IsServerAdmin(n));
        var admin = catalog.Count(DarlingMcpWritePolicy.IsServerAdmin);
        var shared = served.Count(DarlingMcpInstructions.LiteToolNames.Contains);

        Assert.True(served.Count < catalog.Count, "the writes-off configuration should serve fewer tools than the catalog");
        Assert.Equal(catalog.Count - served.Count, writes + admin);

        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served);
        Assert.Contains(
            $"This server exposes {served.Count} of its {catalog.Count} tools: the write tools are off, so the {writes + admin} that change " +
            $"monitoring configuration are not served (`mcp.allowWrites` with a token adds {writes}, `mcp.allowServerAdmin` {admin} more). " +
            $"{shared} are the same names Lite exposes. The remaining {served.Count - shared} are unique to Darling: thirty-five are the " +
            "PostgreSQL reads (`get_pg_*`), plus the Custom Views and custom-alert-rule reads, and central-store-only reads",
            text, StringComparison.Ordinal);
        Assert.DoesNotContain($"This server exposes {catalog.Count} tools.", text, StringComparison.Ordinal);
        Assert.DoesNotContain("onboarding", text, StringComparison.Ordinal);
        Assert.True(text.Length <= InstructionsBudget, $"the writes-off instructions are {text.Length} characters; D2's budget is {InstructionsBudget}");
    }

    [Fact]
    public void ServedCensus_WritesOnServerAdminOff_NamesTheServerAdminSwitch()
    {
        var catalog = Catalog();
        var served = Served(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: false));
        var shared = served.Count(DarlingMcpInstructions.LiteToolNames.Contains);

        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served);
        Assert.Contains(
            $"This server exposes {served.Count} of its {catalog.Count} tools: `mcp.allowServerAdmin` is off, so `add_servers`, " +
            $"`edit_server` and `remove_server` are not served. {shared} are the same names Lite exposes. The remaining " +
            $"{served.Count - shared} are unique to Darling: thirty-five are the PostgreSQL reads (`get_pg_*`), plus Custom Views, " +
            "custom-alert-rule, alert-tuning and server-tag tools, and central-store-only reads",
            text, StringComparison.Ordinal);
        Assert.True(text.Length <= InstructionsBudget, $"the writes-on instructions are {text.Length} characters; D2's budget is {InstructionsBudget}");
    }

    /// <summary>Everything served: the rendered census is exactly the catalog census <see cref="DarlingMcpInstructions.Text"/>
    /// carries, so the constant cannot drift from the registered tools or from the Lite overlap.</summary>
    [Fact]
    public void ServedCensus_EverythingOn_IsTheCatalogCensus()
    {
        var catalog = Catalog();
        var served = Served(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: true));
        Assert.Equal(catalog.Count, served.Count);

        var census = DarlingMcpInstructions.ToolCensus(catalog, served);
        Assert.StartsWith($"This server exposes {catalog.Count} tools. ", census, StringComparison.Ordinal);
        Assert.Contains(census, DarlingMcpInstructions.Text, StringComparison.Ordinal);
        Assert.Equal(DarlingMcpInstructions.Text, DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served));
    }

    /// <summary><see cref="DarlingMcpInstructions.LiteToolNames"/> is exactly the set of tool names Lite registers,
    /// read from Lite's own <c>[McpServerTool(Name = ...)]</c> attributes.</summary>
    [Fact]
    public void LiteToolNames_AreExactlyTheNamesLiteRegisters()
    {
        var attribute = new Regex(@"\[McpServerTool\(Name = ""([a-z_]+)""", RegexOptions.CultureInvariant);
        var lite = Directory.GetFiles(Path.Combine(RepoRoot(), "Lite", "Mcp"), "*.cs")
            .SelectMany(f => attribute.Matches(File.ReadAllText(f)).Select(m => m.Groups[1].Value))
            .Distinct(StringComparer.Ordinal)
            .OrderBy(n => n, StringComparer.Ordinal)
            .ToArray();

        Assert.NotEmpty(lite);
        Assert.Equal(lite, DarlingMcpInstructions.LiteToolNames.OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        var dir = Path.GetDirectoryName(thisFile)!;
        while (dir is not null && !File.Exists(Path.Combine(dir, "PerformanceMonitor.sln")) && !Directory.Exists(Path.Combine(dir, ".git")))
        {
            dir = Path.GetDirectoryName(dir);
        }

        Assert.NotNull(dir);
        return dir!;
    }
}
