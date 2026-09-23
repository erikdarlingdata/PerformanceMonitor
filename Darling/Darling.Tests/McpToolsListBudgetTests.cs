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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #3898 Phase 0: the <c>tools/list</c> budget ratchet for Darling. It measures what a client actually
/// receives, the serialized <c>ListToolsResult</c> (served descriptions and input schemas), built through the
/// same <c>WithGeminiCompatibleTools</c> path the host registers every tool with, over the tool types the host
/// source registers. Lite's twin is <c>Lite.Tests/McpToolsListBudgetTests</c> (D6 lockstep).
///
/// <para><b>The pins are ceilings that only go down.</b> <c>McpToolsListBudget.txt</c> holds one line per tool
/// (its served description's length) and per advertised parameter (its description's length). A value over its
/// ceiling fails: put the new text after the tool's <see cref="McpToolGuide.Marker"/> instead, where
/// <c>get_tool_guide</c> serves it. A value under its ceiling fails too, so a saving is banked the moment it
/// lands and cannot be spent later: lower the line. A new tool or parameter adds its line deliberately, with
/// the reason in the PR. The total's ceiling is below, with the reason for each change.</para>
///
/// <para><b>D2's absolute caps apply to converted tools</b> (a description carrying the marker): the served head
/// at most 1,000 characters (the target is 600), every parameter description at most 200, and every tail and
/// topic small enough to be served whole by one <c>get_tool_guide</c> answer.</para>
/// </summary>
public sealed class McpToolsListBudgetTests
{
    /// <summary>
    /// The serialized tools/list ceiling, in UTF-8 bytes. Change log (a ceiling only goes down, except for a
    /// deliberate new tool or parameter, named here):
    /// <list type="bullet">
    /// <item>#3898 Phase 0 + seam: pinned at the measured value, which includes the new get_tool_guide and the
    /// converted get_health_parser_* family.</item>
    /// </list>
    /// </summary>
    private const int TotalCeilingBytes = 328_331;

    /// <summary>How far under the total ceiling the measured size may sit before the ceiling must be lowered.
    /// The per-tool and per-parameter lines are exact; this slack only spares every small edit a conflict on
    /// one shared line.</summary>
    private const int TotalBankingSlackBytes = 4_096;

    private const int ConvertedHeadCap = 1_000;
    private const int ConvertedParameterCap = 200;
    private const string BudgetFile = "McpToolsListBudget.txt";

    [Fact]
    public void ToolsList_TotalBytes_IsAtOrUnderTheCeiling_AndSavingsAreBanked()
    {
        var measured = Measure();
        Assert.True(measured.TotalBytes <= TotalCeilingBytes,
            $"tools/list is {measured.TotalBytes:N0} bytes, over its {TotalCeilingBytes:N0}-byte ceiling. Move reading guidance after the tool's {McpToolGuide.Marker} marker instead of growing the head.");
        Assert.True(TotalCeilingBytes - measured.TotalBytes <= TotalBankingSlackBytes,
            $"tools/list is {measured.TotalBytes:N0} bytes, {TotalCeilingBytes - measured.TotalBytes:N0} under its {TotalCeilingBytes:N0}-byte ceiling. Bank the saving: lower {nameof(TotalCeilingBytes)} to {measured.TotalBytes} and say why.");
    }

    [Fact]
    public void EveryServedDescription_AndEveryParameterDescription_MatchesItsCeiling()
    {
        var measured = Measure();
        var pinned = ReadBudget();
        var problems = new List<string>();

        foreach (var (key, value) in measured.Lines)
        {
            if (!pinned.TryGetValue(key, out var ceiling))
            {
                problems.Add($"{key}: {value} is not pinned. Add the line '{key} {value}' deliberately, with the reason in the PR.");
            }
            else if (value > ceiling)
            {
                problems.Add($"{key}: {value} is over its ceiling {ceiling}. Put the new text after the tool's {McpToolGuide.Marker} marker (get_tool_guide serves it) instead of raising the pin.");
            }
            else if (value < ceiling)
            {
                problems.Add($"{key}: {value} is under its ceiling {ceiling}. Bank the saving: lower the line to '{key} {value}'.");
            }
        }

        foreach (var key in pinned.Keys.Where(k => !measured.Lines.ContainsKey(k)))
        {
            problems.Add($"{key}: pinned, but no such tool or parameter is served. Remove the line.");
        }

        if (problems.Count > 0)
        {
            var path = Path.Combine(Path.GetTempPath(), "darling-" + BudgetFile);
            File.WriteAllText(path, Render(measured));
            Assert.Fail($"{problems.Count} tools/list budget line(s) moved (measured file written to {path}):\n"
                + string.Join("\n", problems.Take(40)));
        }
    }

    [Fact]
    public void ConvertedTools_HonorD2sAbsoluteCaps_AndNoMarkerReachesTheWire()
    {
        var measured = Measure();
        var problems = new List<string>();

        foreach (var tool in measured.Tools)
        {
            Assert.DoesNotContain(McpToolGuide.Marker, tool.Served, StringComparison.Ordinal);
            if (tool.Tail is null)
            {
                continue;
            }

            if (tool.Served.Length > ConvertedHeadCap)
            {
                problems.Add($"{tool.Name}: served head {tool.Served.Length} > {ConvertedHeadCap} (target 600).");
            }

            if (tool.Tail.Length > McpToolGuide.MaxGuideCharacters)
            {
                problems.Add($"{tool.Name}: guide {tool.Tail.Length} > {McpToolGuide.MaxGuideCharacters}, so get_tool_guide could never serve it whole.");
            }

            foreach (var (parameter, length) in tool.ParameterDescriptionLengths)
            {
                if (length > ConvertedParameterCap)
                {
                    problems.Add($"{tool.Name}.{parameter}: parameter description {length} > {ConvertedParameterCap}.");
                }
            }
        }

        foreach (var topic in McpToolGuideTopics.All.Where(t => t.Guide.Length > McpToolGuide.MaxGuideCharacters))
        {
            problems.Add($"topic {topic.Name}: {topic.Guide.Length} > {McpToolGuide.MaxGuideCharacters}.");
        }

        Assert.True(problems.Count == 0, string.Join("\n", problems));
        Assert.Contains(measured.Tools, t => t.Tail is not null);
    }

    /// <summary>A near-miss marker (a typo, a stray half) would leave a tool unsplit and silently serve its
    /// whole description; every description source is scanned for one.</summary>
    [Fact]
    public void NoDescriptionCarriesANearMissMarker()
    {
        var offenders = Measure().Tools
            .Where(t => t.Description is not null)
            .Where(t => Regex.Replace(t.Description!, Regex.Escape(McpToolGuide.Marker), string.Empty)
                .Contains("GUIDE>>", StringComparison.OrdinalIgnoreCase)
                || Regex.Replace(t.Description!, Regex.Escape(McpToolGuide.Marker), string.Empty)
                .Contains("<<GUIDE", StringComparison.OrdinalIgnoreCase))
            .Select(t => t.Name)
            .ToList();
        Assert.True(offenders.Count == 0, "near-miss guide marker in: " + string.Join(", ", offenders));
    }

    /* ---------------- measurement ---------------- */

    internal sealed record MeasuredTool(
        string Name,
        string? Description,
        string Served,
        string? Tail,
        IReadOnlyList<(string Parameter, int Length)> ParameterDescriptionLengths);

    internal sealed record Measurement(int TotalBytes, IReadOnlyList<MeasuredTool> Tools, IReadOnlyDictionary<string, int> Lines);

    private static Measurement? _cached;

    internal static Measurement Measure()
    {
        if (_cached is not null)
        {
            return _cached;
        }

        var (tools, types) = BuildServedTools();
        var protocolTools = tools.Select(t => t.ProtocolTool).OrderBy(t => t.Name, StringComparer.Ordinal).ToList();
        var json = JsonSerializer.Serialize(new ListToolsResult { Tools = protocolTools }, McpJsonUtilities.DefaultOptions);
        var totalBytes = Encoding.UTF8.GetByteCount(json);

        var descriptions = types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Select(m => (Attr: m.GetCustomAttribute<McpServerToolAttribute>(), Method: m))
            .Where(x => x.Attr is not null)
            .ToDictionary(x => x.Attr!.Name!, x => x.Method.GetCustomAttribute<DescriptionAttribute>()?.Description, StringComparer.Ordinal);

        var measuredTools = new List<MeasuredTool>();
        var lines = new SortedDictionary<string, int>(StringComparer.Ordinal);
        foreach (var tool in protocolTools)
        {
            var description = descriptions[tool.Name];
            var served = tool.Description ?? string.Empty;
            var tail = description is null ? null : McpToolGuide.Split(description).Tail;
            var parameters = new List<(string, int)>();
            if (tool.InputSchema.TryGetProperty("properties", out var properties))
            {
                foreach (var property in properties.EnumerateObject())
                {
                    var length = property.Value.TryGetProperty("description", out var d) ? d.GetString()!.Length : 0;
                    parameters.Add((property.Name, length));
                    lines[$"param {tool.Name}.{property.Name}"] = length;
                }
            }

            lines[$"tool {tool.Name}"] = served.Length;
            measuredTools.Add(new MeasuredTool(tool.Name, description, served, tail, parameters));
        }

        _cached = new Measurement(totalBytes, measuredTools, lines);
        return _cached;
    }

    private static (List<McpServerTool> Tools, List<Type> Types) BuildServedTools()
    {
        var registered = Regex
            .Matches(File.ReadAllText(RepoPath("Darling", "PerformanceMonitor.Darling.Service", "Mcp", "DarlingMcpHostService.cs")),
                @"WithGeminiCompatibleTools<(\w+)>")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);
        var types = typeof(DarlingMcpHostService).Assembly.GetTypes()
            .Where(t => registered.Contains(t.Name))
            .OrderBy(t => t.FullName, StringComparer.Ordinal)
            .ToList();
        Assert.Equal(registered.Count, types.Count);

        var services = new ServiceCollection();
        foreach (var serviceType in types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static))
            .Where(m => m.GetCustomAttribute<McpServerToolAttribute>() is not null)
            .SelectMany(m => m.GetParameters())
            .Select(p => p.ParameterType)
            .Where(McpServedSchema.IsServiceParameter)
            .Where(t => t != typeof(McpToolGuideCatalog))
            .Distinct())
        {
            services.AddSingleton(serviceType, _ => null!);
        }

        var builder = services.AddMcpServer();
        var register = typeof(McpSchemaCompat).GetMethod(nameof(McpSchemaCompat.WithGeminiCompatibleTools), BindingFlags.Public | BindingFlags.Static)!;
        foreach (var type in types)
        {
            register.MakeGenericMethod(type).Invoke(null, new object?[] { builder });
        }

        var provider = services.BuildServiceProvider();
        return (provider.GetServices<McpServerTool>().ToList(), types);
    }

    private static Dictionary<string, int> ReadBudget()
    {
        var result = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var raw in File.ReadAllLines(RepoPath("Darling", "Darling.Tests", BudgetFile)))
        {
            var line = raw.Trim();
            if (line.Length == 0 || line.StartsWith('#'))
            {
                continue;
            }

            var at = line.LastIndexOf(' ');
            result.Add(line[..at], int.Parse(line[(at + 1)..], CultureInfo.InvariantCulture));
        }

        return result;
    }

    private static string Render(Measurement measured)
    {
        var sb = new StringBuilder();
        sb.AppendLine("# #3898 Phase 0: Darling's tools/list budget. Ceilings only go down; see McpToolsListBudgetTests.");
        sb.AppendLine("# 'tool <name> <chars>' is the served description; 'param <tool>.<param> <chars>' is a parameter description.");
        foreach (var (key, value) in measured.Lines)
        {
            sb.Append(key).Append(' ').Append(value.ToString(CultureInfo.InvariantCulture)).AppendLine();
        }

        return sb.ToString();
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, "..", ".."));
}

/// <summary>
/// Which tool parameters are DI services (excluded from the advertised schema) and which the model fills in.
/// The older censuses' predicate called every non-primitive a service, which silently dropped nullable value
/// types (<c>int?</c>) and arrays (<c>get_tool_guide</c>'s <c>string[]</c>) from the schemas they check.
/// </summary>
internal static class McpServedSchema
{
    internal static bool IsServiceParameter(Type t)
    {
        var underlying = Nullable.GetUnderlyingType(t) ?? t;
        if (underlying.IsArray)
        {
            underlying = underlying.GetElementType()!;
        }

        return !(underlying.IsPrimitive || underlying.IsEnum || underlying == typeof(string) || underlying == typeof(decimal)
            || underlying == typeof(DateTime) || underlying == typeof(DateTimeOffset) || underlying == typeof(Guid)
            || underlying == typeof(TimeSpan));
    }
}
