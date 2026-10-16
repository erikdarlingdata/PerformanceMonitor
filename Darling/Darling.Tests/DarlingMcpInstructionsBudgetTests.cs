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
    /* The rule for this ceiling (#5944). The instructions text is sent to every MCP client at the start of every session,
       so its length is a real cost the client pays in context before it calls a tool. 8,000 is a round figure with
       headroom, not a number set a few bytes above today's length: the tests below fail only when the text really
       crosses it. A change that needs more room either shortens something else or raises this number on purpose, in
       the same PR, with the reason. Never trim to land a few bytes under, and never raise it just to merge. */
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
        var investigations = catalog.Count(DarlingMcpWritePolicy.IsInvestigation);
        Assert.Equal(catalog.Count - served.Count, writes + admin + investigations);

        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served);
        Assert.Contains(
            $"This server exposes {served.Count} of its {catalog.Count} tools: the write tools are off, so the {writes + admin} that change " +
            $"monitoring configuration are not served (`mcp.allowWrites` with a token adds {writes}, `mcp.allowServerAdmin` {admin} more); " +
            $"`mcp.allowInvestigations` with a token adds the {investigations} investigation tools. " +
            $"{shared} are the same names Lite exposes. The remaining {served.Count - shared} are unique to Darling: thirty-five are the " +
            "PostgreSQL reads (`get_pg_*`), plus the Custom Views and custom-alert-rule reads and the investigation-wait tool, and central-store-only reads",
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
            $"This server exposes {served.Count} of its {catalog.Count} tools: `mcp.allowServerAdmin` is off, so the " +
            $"{catalog.Count(DarlingMcpWritePolicy.IsServerAdmin)} tools that add, edit and remove monitored servers are not served; " +
            $"the {catalog.Count(DarlingMcpWritePolicy.IsInvestigation)} investigation tools stay off while `mcp.allowWrites` is on (one token " +
            $"cannot tell an agent from a person). {shared} are the same names Lite exposes. The remaining " +
            $"{served.Count - shared} are unique to Darling: thirty-five are the PostgreSQL reads (`get_pg_*`), plus Custom Views, " +
            "custom-alert-rule, alert-tuning, server-tag and investigation-wait tools, and central-store-only reads",
            text, StringComparison.Ordinal);
        Assert.True(text.Length <= InstructionsBudget, $"the writes-on instructions are {text.Length} characters; D2's budget is {InstructionsBudget}");
    }

    /// <summary>Everything served: the rendered census is exactly the catalog census <see cref="DarlingMcpInstructions.Text"/>
    /// carries, so the constant cannot drift from the registered tools or from the Lite overlap.</summary>
    [Fact]
    public void ServedCensus_EverythingOn_IsTheCatalogCensus()
    {
        var catalog = Catalog();
        /* #5632 Phase A: the whole catalog. No start resolves this policy (the investigation pair and the writes exclude
           each other, ruling R2); the reference census still counts every registered tool. */
        var served = Served(new DarlingMcpWritePolicy.Effective(Writes: true, ServerAdmin: true, Investigations: true));
        Assert.Equal(catalog.Count, served.Count);

        var census = DarlingMcpInstructions.ToolCensus(catalog, served);
        Assert.StartsWith($"This server exposes {catalog.Count} tools. ", census, StringComparison.Ordinal);
        Assert.Contains(census, DarlingMcpInstructions.Text, StringComparison.Ordinal);
        Assert.Equal(DarlingMcpInstructions.Text, DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served));
    }

    /// <summary>#5789 correction: no sentence of the rendered instructions points a client at a tool this server
    /// does not serve. Every backticked name that is a Darling tool must be in the served set, for each switch state.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void RenderedInstructions_NameOnlyServedTools(bool writes, bool serverAdmin)
    {
        var catalog = Catalog();
        var served = Served(new DarlingMcpWritePolicy.Effective(writes, serverAdmin));
        var text = DarlingMcpInstructions.Build(DarlingPeerDirectory.Snapshot.Empty, catalog, served);

        var named = Regex.Matches(text, "`([a-z][a-z0-9_]*)`").Select(m => m.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
        var unserved = named.Where(n => catalog.Contains(n) && !served.Contains(n)).OrderBy(n => n, StringComparer.Ordinal).ToList();

        Assert.True(named.Count(catalog.Contains) > 10, "the scan found almost no tool names; the backtick pattern no longer matches the text");
        Assert.True(unserved.Count == 0, "the instructions name tools this configuration does not serve: " + string.Join(", ", unserved));
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
