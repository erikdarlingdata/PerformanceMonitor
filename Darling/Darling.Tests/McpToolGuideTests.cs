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
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D1 + D3 on Darling: <c>get_tool_guide</c> serves the tails the one-place split keeps off
/// <c>tools/list</c>, generic pins any converted family relies on, and the cross-SKU lockstep (identical heads,
/// identical guide tool) that no per-family file needs to repeat. Lite's twin is
/// <c>Lite.Tests/McpToolGuideTests</c>. A family's OWN head pins (its tool roster, its guardrail facts) live in
/// their own <c>McpToolGuideHeads.&lt;Family&gt;.cs</c> next to this file (see
/// <see cref="McpToolGuideHeadsHealthParserTests"/> for the pattern) — lanes converting a new family add a file
/// there and never edit this one, so two families converting in parallel never conflict here.
/// </summary>
public sealed class McpToolGuideTests
{
    internal static McpToolsListBudgetTests.MeasuredTool Served(string tool) =>
        McpToolsListBudgetTests.Measure().Tools.Single(t => t.Name == tool);

    /* ---------------- generic D3 + D6 pins (every family, no hand-kept list) ---------------- */

    /// <summary>D3's absolute head-length target: every tool the split has actually converted (discovered from
    /// the served list, not a hand-kept roster) stays at or under 620 served characters, pointer included. The
    /// pilot pinned this per tool; this is the backstop that catches it for every family from here on,
    /// including one whose own family file forgets to check it.</summary>
    [Fact]
    public void EveryConvertedHead_StaysAtOrUnder620Characters()
    {
        var overLong = McpToolsListBudgetTests.Measure().Tools
            .Where(t => t.Tail is not null && t.Served.Length > 620)
            .Select(t => $"{t.Name}: {t.Served.Length}")
            .ToList();
        Assert.True(overLong.Count == 0, "served head(s) over the 620 target: " + string.Join(", ", overLong));
    }

    /// <summary>
    /// D6, generically: for every MCP tool name registered on BOTH SKUs, if either side's description carries
    /// the marker, both must, and the served heads must be byte-identical. Tool names and marker presence are
    /// discovered by scanning source (not a hand-kept roster), so a family converted on only one SKU fails here
    /// without either SKU needing its own pin. Darling's own description text comes from
    /// <see cref="McpToolsListBudgetTests.Measure"/> (already resolved by reflection) rather than a second,
    /// fragile source-text parse: several unconverted Darling tools build their description from string
    /// concatenation plus an embedded constant (<c>get_collection_health</c>'s
    /// <see cref="AlertReadFailureCounter.FleetScopedReads"/>), which only matters for the marker-presence scan
    /// (a plain substring search, safe regardless of how the string is built) — full resolution
    /// (<see cref="DescriptionLiteral"/>) is only ever needed for Lite's side of a tool BOTH sides already agree
    /// is converted, and every converted description is hand-authored literals a caller can resolve.
    /// </summary>
    [Fact]
    public void EverySharedToolName_CarriesTheMarkerOnBothSkus_OrNeither_WithByteIdenticalHeads()
    {
        var (darlingFiles, darlingHasMarker) = ScanToolsUnder("Darling", "PerformanceMonitor.Darling.Service", "Mcp");
        var (liteFiles, liteHasMarker) = ScanToolsUnder("Lite", "Mcp");
        var darlingMeasured = McpToolsListBudgetTests.Measure().Tools.ToDictionary(t => t.Name, StringComparer.Ordinal);
        var shared = darlingFiles.Keys.Intersect(liteFiles.Keys, StringComparer.Ordinal).OrderBy(n => n, StringComparer.Ordinal).ToList();
        var problems = new List<string>();

        foreach (var tool in shared)
        {
            var darlingConverted = darlingHasMarker[tool];
            var liteConverted = liteHasMarker[tool];
            if (darlingConverted != liteConverted)
            {
                problems.Add($"{tool}: the {McpToolGuide.Marker} marker is on {(darlingConverted ? "Darling only" : "Lite only")}.");
                continue;
            }

            if (!darlingConverted)
            {
                continue;
            }

            Assert.True(darlingMeasured.TryGetValue(tool, out var measured), $"{tool}: carries the marker but is not in Darling's measured tools/list.");
            var darlingHead = McpToolGuide.Split(measured!.Description!).Head;
            var liteHead = McpToolGuide.Split(DescriptionLiteral(File.ReadAllText(liteFiles[tool]), tool)).Head;
            if (!string.Equals(darlingHead, liteHead, StringComparison.Ordinal))
            {
                problems.Add($"{tool}: the served head differs between SKUs.");
            }
        }

        Assert.True(problems.Count == 0, string.Join("\n", problems));
        Assert.Contains(shared, tool => darlingHasMarker[tool]);
    }

    /// <summary>D6: <c>get_tool_guide</c>'s own description is byte-identical on both SKUs (it never carries the
    /// marker itself, so the generic check above never compares it).</summary>
    [Fact]
    public void GetToolGuideDescription_IsByteIdenticalOnBothSkus()
    {
        Assert.Equal(
            DescriptionLiteral(File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpToolGuideTools.cs")), "get_tool_guide"),
            DescriptionLiteral(File.ReadAllText(RepoPath("Lite", "Mcp", "McpToolGuideTools.cs")), "get_tool_guide"));
    }

    /* ---------------- topics: All aggregates every partial family array ---------------- */

    [Fact]
    public void AllTopics_HaveNoNullEntries()
    {
        Assert.All(McpToolGuideTopics.All, t => Assert.NotNull(t));
    }

    [Fact]
    public void AllTopics_HaveUniqueNames()
    {
        var names = McpToolGuideTopics.All.Select(t => t.Name).ToList();
        Assert.Equal(names.Distinct(StringComparer.Ordinal).Count(), names.Count);
    }

    /// <summary>Each family's partial file declares its own <c>private static readonly McpToolGuideTopic[]</c>
    /// field; reflected here so a family added and never wired into <c>All</c>'s static constructor fails
    /// loudly instead of silently serving no topics.</summary>
    [Fact]
    public void EveryPartialTopicArray_IsReflectedInAll()
    {
        var fields = typeof(McpToolGuideTopics)
            .GetFields(BindingFlags.NonPublic | BindingFlags.Static)
            .Where(f => f.FieldType == typeof(McpToolGuideTopic[]))
            .ToList();
        Assert.True(fields.Count == 8, $"expected 8 per-family topic arrays, found {fields.Count}: {string.Join(", ", fields.Select(f => f.Name))}");

        var totalFromFields = fields.Sum(f => ((McpToolGuideTopic[])f.GetValue(null)!).Length);
        Assert.Equal(totalFromFields, McpToolGuideTopics.All.Count);
    }

    /* ---------------- get_tool_guide ---------------- */

    [Fact]
    public void GetToolGuide_ServesTails_AndTopics_AndReportsUnknownNames()
    {
        var catalog = HostCatalog();
        using var doc = JsonDocument.Parse(DarlingMcpToolGuideTools.GetToolGuide(
            catalog,
            ["GET_HEALTH_PARSER_CPU_TASKS", "get_health_parser_cpu_tasks", "list_servers", "no_such_tool"],
            ["system_health_empty_windows", "no_such_topic"]));
        var root = doc.RootElement;

        Assert.Equal("ok", root.GetProperty("status").GetString());
        var tools = root.GetProperty("tools").EnumerateArray().ToList();
        Assert.Single(tools);
        Assert.Equal("get_health_parser_cpu_tasks", tools[0].GetProperty("name").GetString());
        Assert.Equal(Served("get_health_parser_cpu_tasks").Tail, tools[0].GetProperty("guide").GetString());
        Assert.Equal(new[] { "list_servers" }, root.GetProperty("no_separate_guide").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "no_such_tool" }, root.GetProperty("unknown_tools").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "no_such_topic" }, root.GetProperty("unknown_topics").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(McpToolGuideTopics.SystemHealthEmptyWindows, root.GetProperty("topics")[0].GetProperty("guide").GetString());
    }

    [Fact]
    public void GetToolGuide_WithNoArguments_IsTheIndex()
    {
        /* HostCatalog wires up exactly DarlingMcpHealthParserTools, DarlingMcpDataTools and
           DarlingMcpToolGuideTools, so this fixture's own guide-bearing roster is a local, self-contained fact
           about THIS test's host, not a family pin (that lives in McpToolGuideHeadsHealthParserTests). */
        var fixtureToolsWithGuides = new[]
        {
            "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
            "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
            "get_health_parser_severe_errors", "get_health_parser_significant_waits", "get_health_parser_system_health",
        };

        using var doc = JsonDocument.Parse(DarlingMcpToolGuideTools.GetToolGuide(HostCatalog(), null, null));
        var withGuides = doc.RootElement.GetProperty("tools_with_guides").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(fixtureToolsWithGuides, withGuides);
        Assert.Contains(doc.RootElement.GetProperty("topics").EnumerateArray(), t => t.GetProperty("name").GetString() == McpToolGuideTopics.SystemHealthEmptyWindowsName);
    }

    /// <summary>The answer is bounded: guides past the budget are deferred by name, not cut, and a runaway array
    /// is read only up to its cap.</summary>
    [Fact]
    public void GetToolGuide_IsBounded()
    {
        var catalog = new McpToolGuideCatalog();
        var big = new string('x', McpToolGuide.MaxGuideCharacters - 10);
        catalog.Register("tool_a", "Head a. " + McpToolGuide.Marker + " " + big);
        catalog.Register("tool_b", "Head b. " + McpToolGuide.Marker + " " + big);

        using var doc = JsonDocument.Parse(McpToolGuide.Render(catalog, McpToolGuideTopics.All, ["tool_a", "tool_b"], null));
        Assert.Single(doc.RootElement.GetProperty("tools").EnumerateArray());
        Assert.Equal(new[] { "tool_b" }, doc.RootElement.GetProperty("deferred").GetProperty("tools").EnumerateArray().Select(e => e.GetString()).ToArray());

        var many = Enumerable.Range(0, McpToolGuide.MaxNamesPerArray + 7).Select(i => "t" + i).ToArray();
        using var runaway = JsonDocument.Parse(McpToolGuide.Render(catalog, McpToolGuideTopics.All, many, null));
        Assert.Equal(McpToolGuide.MaxNamesPerArray, runaway.RootElement.GetProperty("unknown_tools").GetArrayLength());
        Assert.Equal(7, runaway.RootElement.GetProperty("names_not_read").GetInt32());
    }

    [Fact]
    public void Split_ServesTheHeadPlusPointer_AndRefusesAMalformedMarker()
    {
        Assert.Equal(("Whole.", (string?)null), McpToolGuide.Split("Whole."));
        Assert.Equal("Whole.", McpToolGuide.Served("Whole."));
        Assert.Equal(("Head.", "Tail."), McpToolGuide.Split("Head. " + McpToolGuide.Marker + " Tail."));
        Assert.Equal("Head." + McpToolGuide.GuidePointer, McpToolGuide.Served("Head. " + McpToolGuide.Marker + " Tail."));
        Assert.Throws<InvalidOperationException>(() => McpToolGuide.Split("A " + McpToolGuide.Marker + " B " + McpToolGuide.Marker + " C"));
        Assert.Throws<InvalidOperationException>(() => McpToolGuide.Split(McpToolGuide.Marker + " tail only"));
        Assert.Throws<InvalidOperationException>(() => McpToolGuide.Split("head only " + McpToolGuide.Marker));
    }

    /* ---------------- plumbing ---------------- */

    private static McpToolGuideCatalog HostCatalog()
    {
        var services = new ServiceCollection();
        services.AddMcpServer()
            .WithGeminiCompatibleTools<DarlingMcpHealthParserTools>()
            .WithGeminiCompatibleTools<DarlingMcpDataTools>()
            .WithGeminiCompatibleTools<DarlingMcpToolGuideTools>();
        return services.Single(d => d.ServiceType == typeof(McpToolGuideCatalog)).ImplementationInstance as McpToolGuideCatalog
            ?? throw new InvalidOperationException("no catalog registered");
    }

    /// <summary>The served head of a tool read from source: its Description literal, split at the marker.</summary>
    internal static string HeadOf(string source, string tool) => McpToolGuide.Split(DescriptionLiteral(source, tool)).Head;

    /// <summary>
    /// Reads a tool's <c>Description</c> attribute argument from source, handling both attribute forms
    /// (<c>[McpServerTool(Name = "x"), Description(...)]</c> and <c>[McpServerTool(Name = "x")]\n[Description(...)]</c>),
    /// a description written as several <c>+</c>-concatenated string literals across lines (the pilot's
    /// single-literal-only pattern did not anticipate this; most tools outside the pilot use it), and
    /// <c>get_collection_health</c>'s one embedded constant reference
    /// (<see cref="AlertReadFailureCounter.FleetScopedReads"/>, resolved rather than treated as a literal). Any
    /// other non-literal piece fails, naming the tool and the unrecognized fragment, rather than returning a
    /// silently truncated description.
    /// </summary>
    private static string DescriptionLiteral(string source, string tool)
    {
        var anchor = Regex.Match(source,
            @"\[McpServerTool\(Name\s*=\s*""" + Regex.Escape(tool) + @"""\)(?:\s*,\s*|\s*\]\s*\[\s*)Description\(",
            RegexOptions.Singleline);
        Assert.True(anchor.Success, $"no Description( immediately after the McpServerTool attribute for {tool}");

        var i = anchor.Index + anchor.Length;
        var sb = new StringBuilder();
        while (true)
        {
            while (i < source.Length && char.IsWhiteSpace(source[i]))
            {
                i++;
            }

            if (i < source.Length && source[i] == '"')
            {
                i++;
                var start = i;
                while (source[i] != '"' || source[i - 1] == '\\')
                {
                    i++;
                }

                sb.Append(source[start..i]);
                i++;
            }
            else
            {
                var known = Regex.Match(source[i..], @"^([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)");
                var value = known.Success ? ResolveKnownConstant(known.Groups[1].Value, known.Groups[2].Value) : null;
                Assert.True(value is not null, $"{tool}: unrecognized piece in the Description concatenation at offset {i} ('{source[i..Math.Min(i + 40, source.Length)]}...')");
                sb.Append(value);
                i += known.Length;
            }

            var afterPiece = i;
            while (i < source.Length && char.IsWhiteSpace(source[i]))
            {
                i++;
            }

            if (i < source.Length && source[i] == '+')
            {
                i++;
                continue;
            }

            i = afterPiece;
            break;
        }

        return Regex.Unescape(sb.ToString());
    }

    private static readonly Type[] KnownConstantClasses = [typeof(McpToolGuideTopics), typeof(McpToolGuide), typeof(AlertReadFailureCounter)];

    /// <summary>Resolves a <c>ClassName.MemberName</c> piece of a concatenated Description to its compile-time
    /// value, for the handful of shared constants (topic texts, the marker, the fleet-scoped-reads sentence)
    /// tool descriptions embed instead of repeating. Returns null for anything else, so the caller fails naming
    /// the tool rather than silently dropping an unrecognized piece.</summary>
    private static string? ResolveKnownConstant(string className, string memberName)
    {
        var type = KnownConstantClasses.FirstOrDefault(t => t.Name == className);
        if (type is null)
        {
            return null;
        }

        var field = type.GetField(memberName, BindingFlags.Public | BindingFlags.Static);
        if (field is not null)
        {
            return field.GetValue(null) as string;
        }

        var property = type.GetProperty(memberName, BindingFlags.Public | BindingFlags.Static);
        return property?.GetValue(null) as string;
    }

    /// <summary>Every MCP tool name defined directly under a repo-relative Mcp directory, mapped to the file
    /// that defines it (mirrors <c>CrossAppMcpToolInventoryPinTests.ExtractToolNames</c>'s scan) and to whether
    /// the marker appears anywhere between its attribute and the next tool's (or EOF) — a plain substring
    /// search, so it needs no string-concatenation resolution and cannot be fooled by one.</summary>
    private static (Dictionary<string, string> Files, Dictionary<string, bool> HasMarker) ScanToolsUnder(params string[] segments)
    {
        var files = new Dictionary<string, string>(StringComparer.Ordinal);
        var hasMarker = new Dictionary<string, bool>(StringComparer.Ordinal);
        foreach (var file in Directory.GetFiles(RepoPath(segments), "*.cs", SearchOption.TopDirectoryOnly))
        {
            var src = File.ReadAllText(file);
            var anchors = Regex.Matches(src, @"\[McpServerTool\(Name\s*=\s*""([a-z0-9_]+)""").OrderBy(m => m.Index).ToList();
            for (var idx = 0; idx < anchors.Count; idx++)
            {
                var name = anchors[idx].Groups[1].Value;
                var end = idx + 1 < anchors.Count ? anchors[idx + 1].Index : src.Length;
                Assert.True(files.TryAdd(name, file), $"duplicate MCP tool name '{name}' under {string.Join("/", segments)}");
                hasMarker[name] = src[anchors[idx].Index..end].Contains(McpToolGuide.Marker, StringComparison.Ordinal);
            }
        }

        return (files, hasMarker);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
