/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Collectors;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #3653 A5, the census: the rendered marker reaches every surface the item names, on BOTH SKUs, and the
/// roster is one list read against both. Each trend MCP tool on the roster reads the window's markers and
/// publishes them under the one key, and its description carries the one sentence; the two SKUs' rosters are
/// the same eight names; the two desktop viewers draw the marker through the one Ui helper on all four
/// Performance Trends charts; and the web page's four trend panels render the one sentence. Source-text where
/// the assembly is not referenced (Darling's tools, the viewers, the JavaScript), reflection where it is
/// (Lite's tool descriptions, which the compiler concatenated), and both on the same names so a tool that
/// leaves one SKU's roster reds the census rather than quietly narrowing "every".
/// </summary>
public sealed class BaselineDiscontinuityRenderCensusTests
{
    /// <summary>The trend tools that carry <c>discontinuities[]</c>, by SKU source file. Same eight names on each side.</summary>
    private static readonly (string Sku, string RelativePath, string[] Tools)[] Roster =
    {
        ("Darling", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs", new[]
        {
            "get_memory_trend", "get_perfmon_trend", "get_file_io_trend", "get_query_trend",
            "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend",
        }),
        ("Darling", "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpDataTools.cs", new[] { "get_wait_trend" }),
        ("Lite", "Lite/Mcp/McpMemoryTools.cs", new[] { "get_memory_trend" }),
        ("Lite", "Lite/Mcp/McpPerfmonTools.cs", new[] { "get_perfmon_trend" }),
        ("Lite", "Lite/Mcp/McpIoTools.cs", new[] { "get_file_io_trend" }),
        ("Lite", "Lite/Mcp/McpQueryTools.cs", new[]
        {
            "get_query_trend", "get_query_duration_trend", "get_procedure_duration_trend", "get_query_store_duration_trend",
        }),
        ("Lite", "Lite/Mcp/McpWaitTools.cs", new[] { "get_wait_trend" }),
    };

    public static IEnumerable<object[]> RosterRows()
    {
        foreach (var (sku, path, tools) in Roster)
        {
            foreach (var tool in tools)
            {
                yield return new object[] { sku, path, tool };
            }
        }
    }

    [Fact]
    public void TheTwoSkusCarryTheSameEightTrendTools()
    {
        var darling = Roster.Where(r => r.Sku == "Darling").SelectMany(r => r.Tools).OrderBy(t => t, StringComparer.Ordinal).ToArray();
        var lite = Roster.Where(r => r.Sku == "Lite").SelectMany(r => r.Tools).OrderBy(t => t, StringComparer.Ordinal).ToArray();

        Assert.Equal(8, darling.Length);
        Assert.Equal(darling, lite);
    }

    [Theory]
    [MemberData(nameof(RosterRows))]
    public void EveryRosteredTool_ReadsTheWindowsMarkers_PublishesTheKey_AndDescribesIt(string sku, string relativePath, string tool)
    {
        var source = File.ReadAllText(RepoPath(relativePath.Split('/')));
        var attribute = "Name = \"" + tool + "\"";
        var start = source.IndexOf(attribute, StringComparison.Ordinal);
        Assert.True(start >= 0, $"{sku} {tool}: tool attribute not found in {relativePath}");
        var end = source.IndexOf("FormatError(\"" + tool + "\"", start, StringComparison.Ordinal);
        Assert.True(end > start, $"{sku} {tool}: FormatError arm not found");
        var body = source[start..end];

        /* The description ends with the shared sentence, spelled by reference so it cannot drift. */
        var attributeLine = body[..body.IndexOf('\n', StringComparison.Ordinal)];
        Assert.Contains("+ BaselineDiscontinuities.DescriptionSentence)]", attributeLine, StringComparison.Ordinal);

        /* The body READS the window's markers and hands them to the payload — directly, or through the trio's
           shared serializer, whose own write is pinned below. */
        Assert.Contains("GetBaselineDiscontinuitiesAsync(", body, StringComparison.Ordinal);
        Assert.True(
            body.Contains("BaselineDiscontinuities.ToPayload(", StringComparison.Ordinal)
            || body.Contains("SerializeTrend(", StringComparison.Ordinal),
            $"{sku} {tool}: the body neither projects the block nor serializes through SerializeTrend");
    }

    [Theory]
    [InlineData("Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpTrendTools.cs")]
    [InlineData("Lite/Mcp/McpQueryTools.cs")]
    public void TheTriosSharedSerializer_WritesTheKeyLast(string relativePath)
    {
        var source = File.ReadAllText(RepoPath(relativePath.Split('/')));
        var start = source.IndexOf("private static string SerializeTrend(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = source[start..source.IndexOf("return JsonSerializer.Serialize(envelope", start, StringComparison.Ordinal)];

        Assert.Contains("IReadOnlyList<BaselineDiscontinuity> discontinuities", body, StringComparison.Ordinal);
        Assert.Contains("envelope[BaselineDiscontinuities.PayloadKey] = BaselineDiscontinuities.ToPayload(discontinuities);", body, StringComparison.Ordinal);
        /* Last: after the points. */
        Assert.True(
            body.LastIndexOf("envelope[\"trend\"]", StringComparison.Ordinal) < body.LastIndexOf("envelope[BaselineDiscontinuities.PayloadKey]", StringComparison.Ordinal),
            "the discontinuities block must be written after the points");
    }

    /// <summary>Lite's side by reflection: the compiled description carries the sentence, for every roster tool.</summary>
    [Fact]
    public void LiteDescriptions_CarryTheSentence_ByReflection()
    {
        var tools = LoadableTypes(typeof(McpMemoryTools).Assembly)
            .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
            .Select(m => (Method: m, Tool: m.GetCustomAttribute<McpServerToolAttribute>()))
            .Where(x => x.Tool is not null)
            .ToDictionary(x => x.Tool!.Name!, x => x.Method, StringComparer.Ordinal);

        var roster = Roster.Where(r => r.Sku == "Lite").SelectMany(r => r.Tools).ToArray();
        foreach (var name in roster)
        {
            Assert.True(tools.TryGetValue(name, out var method), $"{name} is not a Lite MCP tool");
            var description = method!.GetCustomAttribute<DescriptionAttribute>()!.Description;
            Assert.EndsWith(BaselineDiscontinuities.DescriptionSentence, description);
        }

        /* And no Lite tool OUTSIDE the roster claims the key — the roster is the whole truth of "every". */
        foreach (var (name, method) in tools)
        {
            if (roster.Contains(name, StringComparer.Ordinal))
            {
                continue;
            }

            var description = method.GetCustomAttribute<DescriptionAttribute>()?.Description ?? string.Empty;
            Assert.DoesNotContain("discontinuities[]", description, StringComparison.Ordinal);
        }
    }

    /// <summary>Both desktop viewers: the four Performance Trends charts each draw the marker through the one
    /// Ui helper with the one sentence, and the read is the shared one.</summary>
    [Theory]
    [InlineData("Lite/Controls/ServerTab.Charts.cs", "Lite/Controls/ServerTab.Refresh.cs", "_dataService.GetBaselineDiscontinuitiesAsync(")]
    [InlineData("Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.QueryTrends.cs", "Darling/PerformanceMonitor.Darling.Viewer/ViewerServerTab.QueryTrends.cs", "_dataService.GetBaselineDiscontinuitiesAsync(")]
    public void BothViewers_MarkAllFourTrendCharts_ThroughTheSharedHelper(string chartsPath, string loadPath, string read)
    {
        var charts = File.ReadAllText(RepoPath(chartsPath.Split('/')));
        var load = File.ReadAllText(RepoPath(loadPath.Split('/')));

        Assert.Contains(read, load, StringComparison.Ordinal);
        Assert.Contains("ChartStyle.AddDiscontinuityMarker(", charts, StringComparison.Ordinal);
        Assert.Contains("BaselineDiscontinuities.Sentence(", charts, StringComparison.Ordinal);

        foreach (var chart in new[] { "QueryDurationTrendChart", "ProcDurationTrendChart", "QueryStoreDurationTrendChart", "ExecutionCountTrendChart" })
        {
            Assert.Contains("MarkDiscontinuities(" + chart + ", discontinuities);", charts, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheUiHelper_DrawsADashedAccentLine_NamedInTheLegend()
    {
        var source = File.ReadAllText(RepoPath("PerformanceMonitor.Ui", "ChartStyle.cs"));
        var start = source.IndexOf("public static ScottPlot.Plottables.VerticalLine AddDiscontinuityMarker(", StringComparison.Ordinal);
        Assert.True(start >= 0);
        var body = source[start..source.IndexOf("return line;", start, StringComparison.Ordinal)];

        Assert.Contains("chart.Plot.Add.VerticalLine(x)", body, StringComparison.Ordinal);
        Assert.Contains("AccentColor(\"Discontinuity\")", body, StringComparison.Ordinal);
        Assert.Contains("LinePattern.Dashed", body, StringComparison.Ordinal);
        Assert.Contains("line.LegendText = legendText;", body, StringComparison.Ordinal);
        Assert.Equal("#E8B21E", PerformanceMonitor.Common.ChartPalette.AccentColor("Discontinuity"));
    }

    /// <summary>The web page: one sentence builder, the shared wording, applied to the four trend panels.</summary>
    [Fact]
    public void TheWebPage_RendersTheSentenceOnItsFourTrendPanels()
    {
        var js = File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Service", "wwwroot", "js", "pages", "server-tabs.js"));

        Assert.Contains("function discontinuityNotes(data)", js, StringComparison.Ordinal);
        Assert.Contains("\"baseline discontinuity at \" + localTime(d.at) + \" (\" + d.reason + \")\"", js, StringComparison.Ordinal);
        Assert.Contains("Array.isArray(data.discontinuities)", js, StringComparison.Ordinal);

        /* One definition and one use per trend-drawing function: the wait, perfmon, per-query and file-I/O panels. */
        var uses = Regex.Matches(js, @"discontinuityNotes\((trend\.data|res\.data)\)").Count;
        Assert.Equal(4, uses);
        foreach (var fn in new[] { "async function drawWaitTrend(", "async function drawPerfmonTrend(", "async function drawQueryTrend(", "export function fileIoPanel(" })
        {
            var start = js.IndexOf(fn, StringComparison.Ordinal);
            Assert.True(start >= 0, fn);
            var next = js.IndexOf("\nasync function ", start + fn.Length, StringComparison.Ordinal);
            var nextExport = js.IndexOf("\nexport function ", start + fn.Length, StringComparison.Ordinal);
            var end = new[] { next, nextExport }.Where(i => i > 0).DefaultIfEmpty(js.Length).Min();
            Assert.Contains("discontinuityNotes(", js[start..end], StringComparison.Ordinal);
        }
    }

    /* The Lite assembly hosts WPF types; on a machine without the desktop framework GetTypes() throws for
       those and hands the loadable rest through the exception. The MCP tool classes are plain statics and
       always load, which is all this census reads. */
    private static IEnumerable<Type> LoadableTypes(Assembly assembly)
    {
        try
        {
            return assembly.GetTypes();
        }
        catch (ReflectionTypeLoadException ex)
        {
            return ex.Types.Where(t => t is not null)!;
        }
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
}
