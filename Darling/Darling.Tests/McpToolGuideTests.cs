/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 D1 + D3 on Darling: <c>get_tool_guide</c> serves the tails the one-place split keeps off
/// <c>tools/list</c>, and the head pins for the pilot family (<c>get_health_parser_*</c>): every guardrail fact
/// a caller needs to read the answer is served in the HEAD, not left in the guide. Lite's twin is
/// <c>Lite.Tests/McpToolGuideTests</c>; the cross-SKU lockstep pins (identical heads, identical guide tool)
/// live here because this project already reads Lite's source.
/// </summary>
public sealed class McpToolGuideTests
{
    private static readonly string[] HealthParserTools =
    [
        "get_health_parser_cpu_tasks",
        "get_health_parser_io_issues",
        "get_health_parser_memory_broker",
        "get_health_parser_memory_conditions",
        "get_health_parser_memory_node_oom",
        "get_health_parser_scheduler_issues",
        "get_health_parser_severe_errors",
        "get_health_parser_significant_waits",
        "get_health_parser_system_health",
    ];

    /// <summary>The per-tool guardrail fact each head must state: the significance gate (a floor under what can
    /// be listed at all) or its absence.</summary>
    private static readonly (string Tool, string Fact)[] GateFacts =
    [
        ("get_health_parser_system_health", "Ungated: every parsed event is returned."),
        ("get_health_parser_memory_node_oom", "Ungated: every recorded OOM is returned."),
        ("get_health_parser_severe_errors", "Gated: severity 19 or higher only, benign connection-reset error numbers excluded"),
        ("get_health_parser_io_issues", "Gated: WARNING-state results only."),
        ("get_health_parser_scheduler_issues", "Gated: WARNING-state results only."),
        ("get_health_parser_memory_conditions", "Gated: only snapshots whose last notification is RESOURCE_MEMPHYSICAL_LOW."),
        ("get_health_parser_cpu_tasks", "Gated: WARNING-state results with at least 10 pending tasks only."),
        ("get_health_parser_memory_broker", "Gated: RESOURCE_MEMPHYSICAL_LOW notifications only."),
        ("get_health_parser_significant_waits", "Floors: a real session, a non-BACKUP statement, at least 500 ms, and a wait type off the idle/background list; shorter waits are never listed."),
    ];

    private static McpToolsListBudgetTests.MeasuredTool Served(string tool) =>
        McpToolsListBudgetTests.Measure().Tools.Single(t => t.Name == tool);

    /* ---------------- D3 head pins ---------------- */

    [Fact]
    public void EveryPilotHead_ServesTheWindow_TheEmptyGuardrail_AndItsGate()
    {
        foreach (var tool in HealthParserTools)
        {
            var served = Served(tool);
            Assert.NotNull(served.Tail);
            /* The window is fixed on the event's own time, ends at as_of, newest first. */
            Assert.Contains("over an event_time window ending at as_of, newest first.", served.Served, StringComparison.Ordinal);
            /* Zero is not a measurement here unless the witness says so (#3541 A12). */
            Assert.Contains("An empty answer is not a clean bill: read status, source_observed and last_captured_at.", served.Served, StringComparison.Ordinal);
            Assert.EndsWith(McpToolGuide.GuidePointer, served.Served, StringComparison.Ordinal);
            Assert.True(served.Served.Length <= 600, $"{tool}: served head {served.Served.Length} is over the 600 target");
        }

        foreach (var (tool, fact) in GateFacts)
        {
            Assert.Contains(fact, Served(tool).Served, StringComparison.Ordinal);
        }

        /* No required parameter, so the head need not name one: all four are optional in the served schema. */
        Assert.All(HealthParserTools, tool => Assert.All(Served(tool).ParameterDescriptionLengths, p => Assert.True(p.Length <= 200)));
    }

    /// <summary>Nothing the old descriptions said was lost: the four-rung empty-window sentence every one of
    /// them carried is the topic, and each guide carries the topic verbatim, so one call answers it.</summary>
    [Fact]
    public void EveryPilotGuide_CarriesTheEmptyWindowTopic_AndTheTopicCarriesEveryRung()
    {
        foreach (var tool in HealthParserTools)
        {
            Assert.EndsWith(McpToolGuideTopics.SystemHealthEmptyWindows, Served(tool).Tail!, StringComparison.Ordinal);
        }

        var topic = McpToolGuideTopics.SystemHealthEmptyWindows;
        foreach (var fact in new[] { "source_observed", "EVER", "last_captured_at", "gated out", "Captured before", "Never recorded",
                     "status unavailable", "not a clean bill", "events_in_window", "last_captured_of_type_at" })
        {
            Assert.Contains(fact, topic, StringComparison.Ordinal);
        }
    }

    /// <summary>D6: the pilot heads are byte-identical on both SKUs (one shared parser, one shared gate), and so
    /// is get_tool_guide's own description. Read from source, since Lite's assembly is not referenced here.</summary>
    [Fact]
    public void PilotHeads_AndTheGuideTool_AreIdenticalOnBothSkus()
    {
        var darling = File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHealthParserTools.cs"));
        var lite = File.ReadAllText(RepoPath("Lite", "Mcp", "McpHealthParserTools.cs"));
        foreach (var tool in HealthParserTools)
        {
            var darlingHead = HeadOf(darling, tool);
            Assert.Equal(darlingHead, HeadOf(lite, tool));
            Assert.Equal(McpToolGuide.Split(Served(tool).Description!).Head, darlingHead);
        }

        Assert.Equal(
            DescriptionLiteral(File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpToolGuideTools.cs")), "get_tool_guide"),
            DescriptionLiteral(File.ReadAllText(RepoPath("Lite", "Mcp", "McpToolGuideTools.cs")), "get_tool_guide"));
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
        using var doc = JsonDocument.Parse(DarlingMcpToolGuideTools.GetToolGuide(HostCatalog(), null, null));
        var withGuides = doc.RootElement.GetProperty("tools_with_guides").EnumerateArray().Select(e => e.GetString()).ToArray();
        Assert.Equal(HealthParserTools, withGuides);
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

    private static string DescriptionLiteral(string source, string tool)
    {
        var match = Regex.Match(source, @"\[McpServerTool\(Name = """ + tool + @"""\), Description\(""((?:[^""\\]|\\.)*)""");
        Assert.True(match.Success, $"no Description literal for {tool}");
        return Regex.Unescape(match.Groups[1].Value);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}
