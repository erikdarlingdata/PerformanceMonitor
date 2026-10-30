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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Mcp;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// #3898 Phase 0: the <c>tools/list</c> served-shape checks for Lite. It measures what a client actually
/// receives, the serialized <c>ListToolsResult</c> (served descriptions and input schemas), built through the
/// same <c>WithGeminiCompatibleTools</c> path the host registers every tool with, over the tool types the host
/// source registers. Darling's twin is <c>Darling.Tests/McpToolsListBudgetTests</c> (D6 lockstep).
///
/// <para><b>D2's absolute caps apply to converted tools</b> (a description carrying the marker): the served head
/// at most 1,000 characters (the target is 600), every parameter description at most 230, and every tail and
/// topic small enough to be served whole by one <c>get_tool_guide</c> answer.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class McpToolsListBudgetTests
{
    private const int ConvertedHeadCap = 1_000;
    private const int ConvertedParameterCap = 230; // longest served parameter description is 200 (Darling) and 195 (Lite); current plus about 15%

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
                /* Unconverted: served exactly as before the seam, the whole [Description]. */
                Assert.Equal(tool.Description ?? string.Empty, tool.Served);
                continue;
            }

            Assert.Equal(McpToolGuide.Split(tool.Description!).Head + McpToolGuide.GuidePointer, tool.Served);

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

    /// <summary>
    /// #5820 L5c: the four investigation tools are in the served <c>tools/list</c> only with the gate on (the setting on and
    /// the writes off) and absent otherwise, and what they add stays inside a byte budget. Measured through the real policies
    /// over the real tool classes, so a fifth tool, a longer head or a gate that stops filtering fails here.
    /// </summary>
    [Fact]
    public void InvestigationTools_AreInTheServedList_OnlyWithTheGateOn_AndAddWithinTheBudget()
    {
        var names = McpInvestigationPolicy.InvestigationTools;
        var off = ServedList(allowWrites: false, allowInvestigations: false);
        var on = ServedList(allowWrites: false, allowInvestigations: true);

        Assert.DoesNotContain(off.Names, names.Contains);
        Assert.Equal(names.OrderBy(n => n, StringComparer.Ordinal), on.Names.Where(names.Contains).OrderBy(n => n, StringComparer.Ordinal));
        Assert.Equal(off.Names.Count + names.Count, on.Names.Count);
        Assert.DoesNotContain(ServedList(allowWrites: true, allowInvestigations: true).Names, names.Contains);

        var added = on.Bytes - off.Bytes;
        Assert.True(added > 0 && added <= InvestigationToolsBytesBudget,
            $"the four investigation tools add {added} bytes to tools/list; the budget is {InvestigationToolsBytesBudget}");
    }

    /// <summary>What the four investigation tools may add to the served <c>tools/list</c> (heads, parameter descriptions and
    /// schemas), measured at 3,280 bytes when L5c landed and 3,922 at the 3.11.0 release (the ceiling is that plus about 15%).</summary>
    private const int InvestigationToolsBytesBudget = 4_500;

    private static (IReadOnlyList<string> Names, int Bytes) ServedList(bool allowWrites, bool allowInvestigations)
    {
        var collection = new McpServerPrimitiveCollection<McpServerTool>();
        foreach (var tool in BuildServedTools().Tools)
        {
            collection.Add(tool);
        }

        McpInvestigationPolicy.Apply(collection, McpInvestigationPolicy.Served(allowInvestigations, allowWrites));
        McpWritePolicy.Apply(collection, allowWrites);
        var protocolTools = collection.Select(t => t.ProtocolTool).OrderBy(t => t.Name, StringComparer.Ordinal).ToList();
        var bytes = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(new ListToolsResult { Tools = protocolTools }, McpJsonUtilities.DefaultOptions));
        return (protocolTools.Select(t => t.Name).ToList(), bytes);
    }

    /* ---------------- measurement ---------------- */

    internal sealed record MeasuredTool(
        string Name,
        string ClassName,
        string? Description,
        string Served,
        string? Tail,
        IReadOnlyList<(string Parameter, int Length)> ParameterDescriptionLengths);

    internal sealed record Measurement(int TotalBytes, IReadOnlyList<MeasuredTool> Tools, IReadOnlyList<string> ClassNames);

    /// <summary>How many times <see cref="Build"/> actually ran (should be exactly 1 per test process; more
    /// than 1 would mean the <see cref="Lazy{T}"/> re-entered or was replaced, which the diagnostic below
    /// reports if it ever happens again).</summary>
    private static int _measureBuildCount;

    /// <summary>The managed thread that ran <see cref="Build"/>, for the "0 is not pinned" diagnostic below.</summary>
    private static int _measureBuildThreadId;

    /// <summary>The <c>IServiceProviderIsService</c> from the provider <see cref="BuildServedTools"/> built,
    /// captured so a later assertion failure can ask it directly whether a given CLR type reads as a DI
    /// service to that same provider (see #4075's "0 is not pinned" diagnostic below).</summary>
    private static IServiceProviderIsService? _measureServiceProviderIsService;

    private static readonly Lazy<Measurement> _measured = new(Build, LazyThreadSafetyMode.ExecutionAndPublication);

    internal static Measurement Measure() => _measured.Value;

    private static Measurement Build()
    {
        Interlocked.Increment(ref _measureBuildCount);
        _measureBuildThreadId = Environment.CurrentManagedThreadId;
        var (tools, types, isService) = BuildServedTools();
        _measureServiceProviderIsService = isService;
        var protocolTools = tools.Select(t => t.ProtocolTool).OrderBy(t => t.Name, StringComparer.Ordinal).ToList();
        var json = JsonSerializer.Serialize(new ListToolsResult { Tools = protocolTools }, McpJsonUtilities.DefaultOptions);
        var totalBytes = Encoding.UTF8.GetByteCount(json);

        var toolInfo = types
            .SelectMany(t => t.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static).Select(m => (Type: t, Method: m)))
            .Select(x => (Attr: x.Method.GetCustomAttribute<McpServerToolAttribute>(), x.Method, x.Type))
            .Where(x => x.Attr is not null)
            .ToDictionary(
                x => x.Attr!.Name!,
                x => (ClassName: x.Type.Name, Description: x.Method.GetCustomAttribute<DescriptionAttribute>()?.Description, Method: x.Method),
                StringComparer.Ordinal);

        var measuredTools = new List<MeasuredTool>();
        var parameterTypes = new Dictionary<string, Type>(StringComparer.Ordinal);
        foreach (var tool in protocolTools)
        {
            var (className, description, method) = toolInfo[tool.Name];
            var served = tool.Description ?? string.Empty;
            var tail = description is null ? null : McpToolGuide.Split(description).Tail;
            var parameters = new List<(string, int)>();
            if (tool.InputSchema.TryGetProperty("properties", out var properties))
            {
                foreach (var property in properties.EnumerateObject())
                {
                    var length = property.Value.TryGetProperty("description", out var d) ? d.GetString()!.Length : 0;
                    parameters.Add((property.Name, length));
                    var parameterInfo = method.GetParameters().FirstOrDefault(p => string.Equals(p.Name, property.Name, StringComparison.Ordinal));
                    if (parameterInfo is not null)
                    {
                        parameterTypes[$"{tool.Name}.{property.Name}"] = parameterInfo.ParameterType;
                    }
                }
            }

            measuredTools.Add(new MeasuredTool(tool.Name, className, description, served, tail, parameters));
        }

        _measureParameterTypes = parameterTypes;
        var classNames = types.Select(t => t.Name).OrderBy(n => n, StringComparer.Ordinal).ToList();
        return new Measurement(totalBytes, measuredTools, classNames);
    }

    /// <summary>The CLR parameter type behind every served <c>tool.parameter</c> key measured in the most
    /// recent <see cref="Build"/>, for the "0 is not pinned" diagnostic below.</summary>
    private static Dictionary<string, Type>? _measureParameterTypes;

    internal static (List<McpServerTool> Tools, List<Type> Types, IServiceProviderIsService IsService) BuildServedTools()
    {
        var registered = Regex
            .Matches(File.ReadAllText(RepoPath("Lite", "Mcp", "McpHostService.cs")),
                @"WithGeminiCompatibleTools<(\w+)>")
            .Select(m => m.Groups[1].Value)
            .ToHashSet(StringComparer.Ordinal);
        var types = typeof(McpHostService).Assembly.GetTypes()
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
        var isService = provider.GetRequiredService<IServiceProviderIsService>();
        return (provider.GetServices<McpServerTool>().ToList(), types, isService);
    }

    private static string RepoPath(params string[] segments) => Path.Combine(new[] { RepoRoot() }.Concat(segments).ToArray());

    private static string RepoRoot([CallerFilePath] string thisFile = "")
        => Path.GetFullPath(Path.Combine(Path.GetDirectoryName(thisFile)!, ".."));
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
