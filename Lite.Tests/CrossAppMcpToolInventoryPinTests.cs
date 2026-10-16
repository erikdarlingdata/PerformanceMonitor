/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// PIN B (parity board §05 D3, round 2): the MCP tool-INVENTORY pin. Nothing pins Lite's and Darling's MCP
/// tool sets together — <c>McpSchemaCompatTests</c> guards the Gemini schema SHAPE, not the tool set — so a
/// tool present in one app and absent from the other is invisible to CI. This enumerates every
/// <c>[McpServerTool(Name="…")]</c> in both apps' MCP servers and asserts Darling's set is a SUPERSET of
/// Lite's, with two exceptions, <see cref="KnownDarlingMissingMcpTools"/> (the Lite tools Darling deliberately lacks) and:
/// <list type="bullet">
/// <item><see cref="KnownNamingDrift"/> — the one same-capability / different-name pair
/// (Lite <c>get_blocked_process_reports</c> &lt;-&gt; Darling <c>get_blocking</c>).</item>
/// </list>
/// A NEW Lite tool with no Darling twin fails the pin. (#5833 removed the ratchet of Darling-only tools and the doc tool-count censuses.)
///
/// <para>
/// Tool names are read from source (the regular <c>[McpServerTool(Name = "…")]</c> attribute) rather than by
/// reflecting the <c>PerformanceMonitor.Darling.Service</c> assembly — this CI-run Lite test project does not
/// (and should not) reference the headless-service / Npgsql stack. <see cref="ExtractToolNames"/> asserts every
/// method-level <c>[McpServerTool(</c> carries an explicit <c>Name =</c>, so the source scan cannot silently
/// miss a tool that relied on framework-derived naming.
/// </para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CrossAppMcpToolInventoryPinTests
{
    private const string LiteMcpDir = "Lite/Mcp";
    private const string DarlingMcpDir = "Darling/PerformanceMonitor.Darling.Service/Mcp";

    /* Same capability, different tool name — flagged for a later naming-unification pass (parity board §05 D,
       Tier 2). Left AS drift here (not renamed) because renaming a shipped MCP tool is a compatibility change;
       this pin documents the pair so it is not mistaken for a missing tool and cannot silently multiply. */
    private static readonly (string Lite, string Darling)[] KnownNamingDrift =
    {
        ("get_blocked_process_reports", "get_blocking"),
    };

    /* Lite tools Darling deliberately does not have, held to the exact set: a NEW Lite tool with no Darling twin
       still fails the pin. */
    private static readonly HashSet<string> KnownDarlingMissingMcpTools = new(StringComparer.Ordinal)
    {
        /* The undo of the analysis mute; mute_analysis_finding itself exists on both. Darling serves no MCP unmute
           on purpose: its MCP role holds an INSERT-only grant on analysis_muted, so removing a mute is a viewer
           or admin action there. Lite has no roles and no such viewer action, its only writer of that registry is
           the MCP mute tool, and the mcp_allow_writes setting that serves the mute serves its undo with it. */
        "unmute_analysis_finding",
    };

    [Fact]
    public void DarlingMcpTools_AreASupersetOfLite_ExceptKnownNamingDrift()
    {
        var lite = ExtractToolNames(LiteMcpDir);
        var darling = ExtractToolNames(DarlingMcpDir);

        /* Non-vacuous floor: a broken scan returning a handful of tools must not sail through. */
        Assert.True(lite.Count >= 40, $"Lite MCP tool scan returned only {lite.Count} tools — the scan is likely broken");
        Assert.True(darling.Count >= 40, $"Darling MCP tool scan returned only {darling.Count} tools — the scan is likely broken");

        var driftLite = KnownNamingDrift.Select(d => d.Lite).ToHashSet(StringComparer.Ordinal);

        /* Every Lite tool (minus the registered naming-drift Lite names) must exist in Darling. */
        var missingFromDarling = lite
            .Where(t => !driftLite.Contains(t) && !KnownDarlingMissingMcpTools.Contains(t) && !darling.Contains(t))
            .OrderBy(t => t, StringComparer.Ordinal)
            .ToList();
        Assert.True(missingFromDarling.Count == 0,
            "Darling is missing Lite MCP tool(s) with no registered naming-drift twin: [" +
            string.Join(", ", missingFromDarling) +
            "]. Add the tool to Darling, register a KnownNamingDrift pair, or (for a tool Darling deliberately lacks) " +
            "add it to KnownDarlingMissingMcpTools with the reason.");

        /* The naming-drift allow-list must still describe reality — both names present, and it really is a
           rename (not a resolved duplicate) — so a fix or a fresh collision forces this list to be updated. */
        foreach (var (liteName, darlingName) in KnownNamingDrift)
        {
            Assert.True(lite.Contains(liteName),
                $"KnownNamingDrift lists Lite tool '{liteName}' that no longer exists — remove the stale pair.");
            Assert.True(darling.Contains(darlingName),
                $"KnownNamingDrift lists Darling tool '{darlingName}' that no longer exists — remove the stale pair.");
            Assert.False(darling.Contains(liteName),
                $"Darling now also exposes '{liteName}', so the naming drift is resolved — remove the pair.");
        }
    }

    /// <summary>
    /// Reads every distinct MCP tool name from the <c>*.cs</c> files under a repo-relative <c>Mcp</c> directory.
    /// Asserts each method-level <c>[McpServerTool(</c> carries an explicit <c>Name = "…"</c> (the convention
    /// the scan relies on; <c>[McpServerToolType]</c> is class-level and has no <c>(</c>, so it is not matched)
    /// and that no tool name is defined twice.
    /// </summary>
    private static HashSet<string> ExtractToolNames(string relativeDir)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);

        foreach (var file in ParitySource.EnumerateCsFiles(relativeDir))
        {
            var src = File.ReadAllText(file);

            var toolAttrs = Regex.Matches(src, @"\[McpServerTool\(");
            var namedAttrs = Regex.Matches(src, @"\[McpServerTool\(Name\s*=\s*""([a-z0-9_]+)""");
            Assert.True(toolAttrs.Count == namedAttrs.Count,
                $"{Path.GetFileName(file)}: {toolAttrs.Count} [McpServerTool(] attribute(s) but {namedAttrs.Count} " +
                "carry an explicit Name = \"…\". Every MCP tool must name itself explicitly so the parity scan sees it.");

            foreach (Match m in namedAttrs)
            {
                var name = m.Groups[1].Value;
                Assert.True(names.Add(name), $"duplicate MCP tool name '{name}' under {relativeDir}");
            }
        }

        return names;
    }
}
