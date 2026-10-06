/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.RegularExpressions;
using ModelContextProtocol.Server;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #5231 (D5): one tool name keeps one <c>database_name</c> contract across the two SKUs. For every MCP tool both
/// Darling and Lite serve (plus Darling's <c>get_blocking</c> = Lite's <c>get_blocked_process_reports</c>), the
/// <c>database_name</c> parameter exists on both or on neither, carries the same description text, and is the LAST
/// parameter either SKU shows an agent (the injected services after it are not in the schema).
/// <para>Darling's side is reflected; Lite's side is read from <c>Lite/Mcp/*.cs</c> (the Lite assembly is not
/// referenced here), reached through <c>Path.Combine("Lite", "Mcp", ...)</c> so the <c>darling</c> CI filter, which
/// covers <c>Lite/**/*.cs</c>, runs this guard on the edit it guards. The Lite.Tests budget pin files hold only
/// description LENGTHS, so they cannot stand in for the text.</para>
/// <para>Exempt, by name: the identity parameters of <c>get_plan_xml</c> and <c>analyze_query_plan</c> (they name the
/// database a stored row came from, they do not filter). And <see cref="MiddleSince"/>: the tools whose
/// <c>database_name</c> was already a middle parameter before the filter work, on one or both SKUs. Moving it now would
/// break every positional caller, so they keep their place; the SET only shrinks (an entry that is now last fails
/// the stale-entry test), and every other tool, including every tool the filter work widens, must keep it last.</para>
/// </summary>
public sealed class DatabaseNameParamTwinTests
{
    private const string Param = "database_name";

    /// <summary>The tool name each SKU uses for the same read, where the names differ (Darling name, Lite name).</summary>
    private static readonly (string Darling, string Lite)[] Renamed = { ("get_blocking", "get_blocked_process_reports") };

    private static readonly Dictionary<string, string> Exempt = new(StringComparer.Ordinal)
    {
        ["get_plan_xml"] = "identity: names the database of one stored plan row, not a filter",
        ["analyze_query_plan"] = "identity: names the database of one stored plan row, not a filter",
    };

    /// <summary>Tools whose <c>database_name</c> predates the "append it LAST" rule and sits mid-list (see the class comment).</summary>
    private static readonly HashSet<string> MiddleSince = new(StringComparer.Ordinal)
    {
        "analyze_query_store_plan", "get_active_queries", "get_current_waits_trend", "get_index_usage", "get_query_heatmap",
        "get_query_store_regressions", "get_query_store_top", "get_query_trend", "get_top_procedures_by_cpu", "get_top_queries_by_cpu",
    };

    private sealed record Tool(string Name, string[] ParamNames, Dictionary<string, string> Descriptions);

    private static Dictionary<string, Tool> DarlingTools() => typeof(DarlingMcpObjectStatsTools).Assembly.GetTypes()
        .Where(t => t.GetCustomAttribute<McpServerToolTypeAttribute>() is not null)
        .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.Static))
        .Select(m => (Attribute: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
        .Where(x => x.Attribute?.Name is not null)
        .ToDictionary(
            x => x.Attribute!.Name!,
            x =>
            {
                var shown = x.Method.GetParameters()
                    .Where(p => p.GetCustomAttribute<DescriptionAttribute>() is not null).ToArray();
                return new Tool(
                    x.Attribute!.Name!,
                    shown.Select(p => p.Name!).ToArray(),
                    shown.ToDictionary(p => p.Name!, p => p.GetCustomAttribute<DescriptionAttribute>()!.Description));
            },
            StringComparer.Ordinal);

    private static Dictionary<string, Tool> LiteTools()
    {
        var tools = new Dictionary<string, Tool>(StringComparer.Ordinal);
        foreach (var file in Directory.GetFiles(PathTo("Lite", "Mcp"), "*.cs"))
        {
            var source = ReadRepoFile("Lite", "Mcp", Path.GetFileName(file));
            foreach (Match m in Regex.Matches(source, @"\[McpServerTool\(Name = ""([a-z_0-9]+)""\)"))
            {
                var tool = ParseLiteTool(source, m.Groups[1].Value, m.Index + m.Length);
                tools[tool.Name] = tool;
            }
        }

        return tools;
    }

    /// <summary>The tool's schema-visible parameters: the ones carrying a <c>[Description(...)]</c>, in order.</summary>
    private static Tool ParseLiteTool(string source, string name, int from)
    {
        var declaration = source.IndexOf("public static", from, StringComparison.Ordinal);
        var open = source.IndexOf('(', declaration);
        var close = MatchingParen(source, open);
        var signature = source[open..close];

        var names = new List<string>();
        var descriptions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match attribute in Regex.Matches(
            signature,
            @"\[Description\(\s*(?:""((?:[^""\\]|\\.)*)""|(\w+))\s*\)\]\s*[\w.<>?\[\]]+\s+(\w+)"))
        {
            var text = attribute.Groups[1].Success
                ? Regex.Unescape(attribute.Groups[1].Value)
                : ResolveConst(source, attribute.Groups[2].Value);
            names.Add(attribute.Groups[3].Value);
            descriptions[attribute.Groups[3].Value] = text;
        }

        return new Tool(name, names.ToArray(), descriptions);
    }

    private static string ResolveConst(string source, string constName)
    {
        var m = Regex.Match(source, @"const string " + constName + @"\s*=\s*""((?:[^""\\]|\\.)*)""");
        return m.Success ? Regex.Unescape(m.Groups[1].Value) : "<" + constName + ">";
    }

    /// <summary>The index of the <c>)</c> closing the <c>(</c> at <paramref name="open"/>, string literals skipped.</summary>
    private static int MatchingParen(string s, int open)
    {
        var depth = 0;
        for (var i = open; i < s.Length; i++)
        {
            switch (s[i])
            {
                case '"':
                    i++;
                    while (s[i] != '"')
                    {
                        if (s[i] == '\\') i++;
                        i++;
                    }

                    break;
                case '(':
                    depth++;
                    break;
                case ')':
                    depth--;
                    if (depth == 0) return i;
                    break;
            }
        }

        throw new InvalidOperationException("unbalanced parentheses in a tool signature");
    }

    /// <summary>Every (Darling name, Lite name) pair both SKUs serve.</summary>
    private static (string DarlingName, string LiteName)[] SharedTools(Dictionary<string, Tool> darling, Dictionary<string, Tool> lite) =>
        darling.Keys.Where(lite.ContainsKey).Select(n => (n, n))
            .Concat(Renamed.Where(r => darling.ContainsKey(r.Darling) && lite.ContainsKey(r.Lite)))
            .OrderBy(p => p.Item1, StringComparer.Ordinal)
            .ToArray();

    [Fact]
    public void TheParser_SeesTheSharedTools_AndThePairsItMustCover()
    {
        var darling = DarlingTools();
        var lite = LiteTools();
        var shared = SharedTools(darling, lite);

        /* A parser that silently saw nothing would pass every other test here. The 88 shared names (plus the one
           rename) are a floor, and the two tools this guard was written for must be among them. */
        Assert.True(shared.Length >= 80, $"only {shared.Length} shared tools were read; the Lite parser has gone blind");
        Assert.Contains(shared, p => p.DarlingName == "get_object_locking");
        Assert.Contains(shared, p => p.DarlingName == "get_blocking" && p.LiteName == "get_blocked_process_reports");
        Assert.Equal(new[] { "server_name", "limit", Param }, lite["get_object_locking"].ParamNames);
        Assert.Equal(new[] { "server_name", "limit", Param }, darling["get_object_locking"].ParamNames);
    }

    [Fact]
    public void DatabaseName_IsOnBothSkusOrNeither_LastOnBoth_WithTheSameDescription()
    {
        var darling = DarlingTools();
        var lite = LiteTools();
        var problems = new List<string>();

        foreach (var (darlingName, liteName) in SharedTools(darling, lite))
        {
            if (Exempt.ContainsKey(darlingName)) continue;

            var d = darling[darlingName];
            var l = lite[liteName];
            var onDarling = d.ParamNames.Contains(Param);
            var onLite = l.ParamNames.Contains(Param);

            if (onDarling != onLite)
            {
                problems.Add($"{darlingName}: {Param} is on {(onDarling ? "Darling" : "Lite")} only");
                continue;
            }

            if (!onDarling) continue;

            if (MiddleSince.Contains(darlingName)) continue;

            if (d.ParamNames[^1] != Param) problems.Add($"{darlingName}: Darling's {Param} is not the last parameter ({string.Join(", ", d.ParamNames)})");
            if (l.ParamNames[^1] != Param) problems.Add($"{liteName}: Lite's {Param} is not the last parameter ({string.Join(", ", l.ParamNames)})");
            if (!string.Equals(d.Descriptions[Param], l.Descriptions[Param], StringComparison.Ordinal))
            {
                problems.Add($"{darlingName}: the {Param} descriptions differ.\n  Darling: {d.Descriptions[Param]}\n  Lite:    {l.Descriptions[Param]}");
            }
        }

        Assert.True(problems.Count == 0, "One tool name must keep one database_name contract on both SKUs:\n" + string.Join("\n", problems));
    }

    [Fact]
    public void TheMiddleRoster_OnlyShrinks_EveryEntryIsStillMidList()
    {
        var darling = DarlingTools();
        var lite = LiteTools();
        foreach (var name in MiddleSince)
        {
            var onDarling = darling[name].ParamNames;
            var liteName = Renamed.FirstOrDefault(r => r.Darling == name).Lite ?? name;
            var onLite = lite.TryGetValue(liteName, out var l) ? l.ParamNames : null;

            /* Mid-list on at least one SKU, or the entry is stale and must go (the rule then applies to the tool). */
            var middleSomewhere = onDarling[^1] != Param || (onLite is not null && onLite.Contains(Param) && onLite[^1] != Param);
            Assert.True(middleSomewhere, $"{name}: database_name is last on both SKUs now; drop it from MiddleSince");
        }
    }

    [Fact]
    public void TheExemptions_NameToolsThatStillExist_AndThatStillHaveTheParameter()
    {
        var darling = DarlingTools();
        foreach (var (name, reason) in Exempt)
        {
            Assert.True(darling.TryGetValue(name, out var tool), $"{name} (exempt: {reason}) is no longer a Darling tool: drop the exemption");
            Assert.True(tool!.ParamNames.Contains(Param), $"{name} (exempt: {reason}) no longer has {Param}: drop the exemption");
        }
    }
}
