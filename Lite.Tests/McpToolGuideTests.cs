/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 D1 + D3 on Lite: <c>get_tool_guide</c> serves the tails the one-place split keeps off
/// <c>tools/list</c>, plus generic pins any converted family relies on. Darling's twin is
/// <c>Darling.Tests/McpToolGuideTests</c>, which also holds the cross-SKU lockstep pins (identical heads,
/// identical guide tool) because that project already reads Lite's source. A family's OWN head pins (its tool
/// roster, its guardrail facts) live in their own <c>McpToolGuideHeads.&lt;Family&gt;.cs</c> next to this file
/// (see <see cref="McpToolGuideHeadsHealthParserTests"/> for the pattern) — lanes converting a new family add a
/// file there and never edit this one, so two families converting in parallel never conflict here.
/// </summary>
public sealed class McpToolGuideTests
{
    internal static McpToolsListBudgetTests.MeasuredTool Served(string tool) =>
        McpToolsListBudgetTests.Measure().Tools.Single(t => t.Name == tool);

    /* ---------------- generic D3 pin (every family, no hand-kept list) ---------------- */

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
        using var doc = JsonDocument.Parse(McpToolGuideTools.GetToolGuide(
            catalog,
            ["GET_HEALTH_PARSER_CPU_TASKS", "get_health_parser_cpu_tasks", "no_such_tool"],
            ["system_health_empty_windows", "no_such_topic"]));
        var root = doc.RootElement;

        Assert.Equal("ok", root.GetProperty("status").GetString());
        var tools = root.GetProperty("tools").EnumerateArray().ToList();
        Assert.Single(tools);
        Assert.Equal("get_health_parser_cpu_tasks", tools[0].GetProperty("name").GetString());
        Assert.Equal(Served("get_health_parser_cpu_tasks").Tail, tools[0].GetProperty("guide").GetString());
        Assert.Equal(new[] { "no_such_tool" }, root.GetProperty("unknown_tools").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "no_such_topic" }, root.GetProperty("unknown_topics").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(McpToolGuideTopics.SystemHealthEmptyWindows, root.GetProperty("topics")[0].GetProperty("guide").GetString());
    }

    [Fact]
    public void GetToolGuide_WithNoArguments_IsTheIndex()
    {
        /* HostCatalog wires up exactly McpHealthParserTools, McpDiscoveryTools and McpToolGuideTools, so this
           fixture's own guide-bearing roster is a local, self-contained fact about THIS test's host, not a
           family pin (that lives in McpToolGuideHeadsHealthParserTests). */
        var fixtureToolsWithGuides = new[]
        {
            "get_health_parser_cpu_tasks", "get_health_parser_io_issues", "get_health_parser_memory_broker",
            "get_health_parser_memory_conditions", "get_health_parser_memory_node_oom", "get_health_parser_scheduler_issues",
            "get_health_parser_severe_errors", "get_health_parser_significant_waits", "get_health_parser_system_health",
            "list_servers",
        };

        using var doc = JsonDocument.Parse(McpToolGuideTools.GetToolGuide(HostCatalog(), null, null));
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
            .WithGeminiCompatibleTools<McpHealthParserTools>()
            .WithGeminiCompatibleTools<McpDiscoveryTools>()
            .WithGeminiCompatibleTools<McpToolGuideTools>();
        return services.Single(d => d.ServiceType == typeof(McpToolGuideCatalog)).ImplementationInstance as McpToolGuideCatalog
            ?? throw new InvalidOperationException("no catalog registered");
    }

    /// <summary>The served head of a tool read from source: its Description literal, split at the marker.</summary>
    internal static string HeadOf(string source, string tool) => McpToolGuide.Split(DescriptionLiteral(source, tool)).Head;

    private static string DescriptionLiteral(string source, string tool)
    {
        var match = Regex.Match(source, @"\[McpServerTool\(Name = """ + tool + @"""\), Description\(""((?:[^""\\]|\\.)*)""");
        Assert.True(match.Success, $"no Description literal for {tool}");
        return Regex.Unescape(match.Groups[1].Value);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
