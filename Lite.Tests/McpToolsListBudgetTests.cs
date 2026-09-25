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
/// #3898 Phase 0: the <c>tools/list</c> budget ratchet for Lite. It measures what a client actually
/// receives, the serialized <c>ListToolsResult</c> (served descriptions and input schemas), built through the
/// same <c>WithGeminiCompatibleTools</c> path the host registers every tool with, over the tool types the host
/// source registers. Darling's twin is <c>Darling.Tests/McpToolsListBudgetTests</c> (D6 lockstep).
///
/// <para><b>The pins live one file per tool class</b> (#3898 step 0), under <c>McpToolsListBudget/</c>, named
/// for the class that declares the tools (e.g. <c>McpWaitTools.txt</c>). Inside a file: one block per
/// tool (a <c>tool &lt;name&gt; &lt;chars&gt;</c> line followed by that tool's sorted
/// <c>param &lt;tool&gt;.&lt;param&gt; &lt;chars&gt;</c> lines), blocks sorted by tool name, exactly one blank
/// line between blocks. Splitting the old single file this way means two content PRs that each touch a
/// different tool class never conflict, and a blank line between every two tools in the SAME class means two
/// PRs that each touch an adjacent tool in that class don't conflict either.</para>
///
/// <para><b>The pins are ceilings that only go down.</b> A value over its ceiling fails: put the new text after
/// the tool's <see cref="McpToolGuide.Marker"/> instead, where <c>get_tool_guide</c> serves it. A value under
/// its ceiling fails too, so a saving is banked the moment it lands and cannot be spent later: lower the line.
/// A new tool or parameter adds its line deliberately, with the reason in the PR. Every failure names the file
/// and line and gives the exact replacement line. The directory is also checked for layout: every served tool
/// has exactly one block, in the file for the class that declares it; every file belongs to a registered class;
/// and every file's bytes match its own canonical re-serialization (order, blank lines, no duplicates).</para>
///
/// <para><b>The total</b> (<see cref="TotalCeilingBytes"/>) is a growth-only ceiling; content PRs never lower
/// it (that happens once, in the #3898 PR that ships the last converted family). The measured total is also
/// written to the test output, so a run's log states it without re-deriving it.</para>
///
/// <para><b>D2's absolute caps apply to converted tools</b> (a description carrying the marker): the served head
/// at most 1,000 characters (the target is 600), every parameter description at most 200, and every tail and
/// topic small enough to be served whole by one <c>get_tool_guide</c> answer.</para>
/// </summary>
public sealed class McpToolsListBudgetTests
{
    private readonly ITestOutputHelper _output;

    public McpToolsListBudgetTests(ITestOutputHelper output)
    {
        _output = output;
    }

    /// <summary>
    /// The serialized tools/list ceiling, in UTF-8 bytes. Growth-only; a content PR never lowers this (the
    /// final #3898 PR does, once, after every family converts). Change log:
    /// <list type="bullet">
    /// <item>#3898 Phase 0 + seam: pinned at the measured value, which includes the new get_tool_guide and the
    /// converted get_health_parser_* family.</item>
    /// <item>#4048 D9 round: the nine pilot heads' empty-answer sentence now ties to each tool's own gate (or
    /// floors, or the absence of one) instead of repeating one identical sentence, so a reader given only the
    /// head is not left to guess which of the four empty-window rungs applies. Raised deliberately.</item>
    /// <item>#3898 close-out: every content PR and both feature PRs have merged. Lowered once, to the measured
    /// total, banking the accumulated saving; a future PR only raises it again.</item>
    /// </list>
    /// </summary>
    /* #4199 (M2b): +162 bytes for get_collection_log's fleet-form server_name/limit descriptions, after
       trimming both to the D2 200-char parameter cap and moving the rest to the tool's tail (get_tool_guide),
       which is not served in tools/list and so is not counted here. Matches Darling's twin change exactly. */
/* #4198 (get_plan_corrections): +137 bytes, matching Darling's twin change exactly — the new full_text
       opt-in parameter only (the head is unchanged; the preview explanation lives in the tail get_tool_guide
       serves). Default row limit dropped 50 -> 25 and the preview 2,000 chars -> 150; neither is a served
       description, so neither counts here. */
    /* #4198 (lane TB): +253 bytes for get_deadlock_detail's default-preview note in its served description
       and its new full_graph opt-in parameter (deadlock_graph_xml, the wide field, is now a 2000-char
       preview by default). Darling's twin grew by a different amount (+364): Darling's description also
       covers the dedup_key exemption, which Lite's get_deadlock_detail has no dedup_key parameter to need. */
    /* #4198: get_collection_log's per-server form gained full_text (76 bytes), matching Darling's twin;
       limit's own description banked 1 byte. +116 net (Lite's server_name description is shorter than
       Darling's, since it has no fleet-maintenance sentinel to warn about). */
    private const int TotalCeilingBytes = 90_387;

    private const int ConvertedHeadCap = 1_000;
    private const int ConvertedParameterCap = 200;

    /// <summary>The directory holding one budget file per tool class, next to this test.</summary>
    private const string BudgetDir = "McpToolsListBudget";

    private const string HeaderSuffix =
        ": tools/list budget for #3898. Ceilings only go down; see McpToolsListBudgetTests. One block per tool, blank line between blocks.";

    private const string DebugPrefix = "lite-budget";

    [Fact]
    public void ToolsList_TotalBytes_IsAtOrUnderTheCeiling()
    {
        var measured = Measure();
        _output.WriteLine($"tools/list total: {measured.TotalBytes:N0} bytes (ceiling {TotalCeilingBytes:N0}).");
        Assert.True(measured.TotalBytes <= TotalCeilingBytes,
            $"tools/list is {measured.TotalBytes:N0} bytes, over its {TotalCeilingBytes:N0}-byte ceiling. Move reading guidance after the tool's {McpToolGuide.Marker} marker instead of growing the head.");
    }

    [Fact]
    public void EveryServedDescription_AndEveryParameterDescription_MatchesItsCeiling()
    {
        var measured = Measure();
        var files = ReadBudgetFiles();
        var problems = new List<string>();

        var pinned = new Dictionary<string, (int Value, string FileName, int LineNumber)>(StringComparer.Ordinal);
        foreach (var file in files)
        {
            foreach (var entry in file.Entries)
            {
                if (!pinned.TryAdd(entry.Key, (entry.Value, file.FileName, entry.LineNumber)))
                {
                    var first = pinned[entry.Key];
                    problems.Add($"{file.FileName}:{entry.LineNumber}: '{entry.Key}' duplicates the pin already at {first.FileName}:{first.LineNumber}. Remove one.");
                }
            }
        }

        var measuredKeys = new SortedDictionary<string, (int Value, string ClassName)>(StringComparer.Ordinal);
        foreach (var tool in measured.Tools)
        {
            measuredKeys[$"tool {tool.Name}"] = (tool.Served.Length, tool.ClassName);
            foreach (var (parameter, length) in tool.ParameterDescriptionLengths)
            {
                measuredKeys[$"param {tool.Name}.{parameter}"] = (length, tool.ClassName);
            }
        }

        foreach (var (key, measuredValue) in measuredKeys)
        {
            if (!pinned.TryGetValue(key, out var ceiling))
            {
                var diagnostic = DescribeUnpinnedIfServiceLeak(key);
                problems.Add($"{key}: {measuredValue.Value} is not pinned. Add the line '{key} {measuredValue.Value}' to {BudgetDir}/{measuredValue.ClassName}.txt.{diagnostic}");
            }
            else if (measuredValue.Value > ceiling.Value)
            {
                problems.Add($"{ceiling.FileName}:{ceiling.LineNumber}: '{key}' is {measuredValue.Value}, over its ceiling {ceiling.Value}. Put the new text after the tool's {McpToolGuide.Marker} marker (get_tool_guide serves it) instead of raising the pin; if the growth is deliberate, replace the line with '{key} {measuredValue.Value}'.");
            }
            else if (measuredValue.Value < ceiling.Value)
            {
                problems.Add($"{ceiling.FileName}:{ceiling.LineNumber}: '{key}' is {measuredValue.Value}, under its ceiling {ceiling.Value}. Bank the saving: replace the line with '{key} {measuredValue.Value}'.");
            }
        }

        foreach (var (key, ceiling) in pinned)
        {
            if (!measuredKeys.ContainsKey(key))
            {
                problems.Add($"{ceiling.FileName}:{ceiling.LineNumber}: '{key}' is pinned, but no such tool or parameter is served. Remove the line.");
            }
        }

        if (problems.Count > 0)
        {
            Assert.Fail($"{problems.Count} tools/list budget line(s) moved:\n"
                + string.Join("\n", problems.OrderBy(p => p, StringComparer.Ordinal).Take(40)));
        }
    }

    /// <summary>
    /// The directory's layout, independent of the values inside it: every served tool has exactly one block,
    /// filed under the class that declares it; every file belongs to a class that is actually registered; and
    /// every file's bytes are its own canonical re-serialization (blocks sorted by tool name, exactly one blank
    /// line between them, no duplicates). A file that fails the last check gets its corrected form written to a
    /// temp path named in the failure.
    /// </summary>
    [Fact]
    public void EveryBudgetFile_MatchesItsRegisteredClass_AndIsInCanonicalForm()
    {
        var measured = Measure();
        var files = ReadBudgetFiles();
        var registeredClassNames = measured.ClassNames.ToHashSet(StringComparer.Ordinal);
        var problems = new List<string>();

        var blockOwner = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var file in files)
        {
            foreach (var entry in file.Entries)
            {
                var (tool, parameter) = ParseKey(entry.Key);
                if (parameter is null)
                {
                    blockOwner[tool] = file.ClassName;
                }
            }
        }

        foreach (var tool in measured.Tools)
        {
            if (!blockOwner.TryGetValue(tool.Name, out var actualClass))
            {
                problems.Add($"{tool.Name}: no block in any {BudgetDir} file. Add 'tool {tool.Name} {tool.Served.Length}' (and its param lines) to {BudgetDir}/{tool.ClassName}.txt.");
            }
            else if (!string.Equals(actualClass, tool.ClassName, StringComparison.Ordinal))
            {
                problems.Add($"{tool.Name}: blocked in {BudgetDir}/{actualClass}.txt, but is declared on {tool.ClassName}. Move its block to {BudgetDir}/{tool.ClassName}.txt.");
            }
        }

        var measuredToolNames = measured.Tools.Select(t => t.Name).ToHashSet(StringComparer.Ordinal);
        foreach (var (toolName, className) in blockOwner)
        {
            if (!measuredToolNames.Contains(toolName))
            {
                problems.Add($"{BudgetDir}/{className}.txt: block for '{toolName}', which is not a served tool. Remove it.");
            }
        }

        foreach (var file in files)
        {
            if (!registeredClassNames.Contains(file.ClassName))
            {
                problems.Add($"{BudgetDir}/{file.FileName}: '{file.ClassName}' is not a registered tool class. Delete the file.");
            }
        }

        foreach (var file in files)
        {
            List<string> canonical;
            try
            {
                canonical = CanonicalLinesFromEntries(file.ClassName, file.Entries);
            }
            catch (InvalidOperationException ex)
            {
                problems.Add($"{BudgetDir}/{file.FileName}: {ex.Message}");
                continue;
            }

            if (canonical.SequenceEqual(file.RawLines, StringComparer.Ordinal))
            {
                continue;
            }

            var firstDiff = 0;
            while (firstDiff < canonical.Count && firstDiff < file.RawLines.Count
                && string.Equals(canonical[firstDiff], file.RawLines[firstDiff], StringComparison.Ordinal))
            {
                firstDiff++;
            }

            var tempPath = Path.Combine(Path.GetTempPath(), $"{DebugPrefix}-{file.FileName}");
            File.WriteAllText(tempPath, JoinForDisk(canonical));
            var expected = firstDiff < canonical.Count ? canonical[firstDiff] : "(end of file)";
            var actual = firstDiff < file.RawLines.Count ? file.RawLines[firstDiff] : "(end of file)";
            problems.Add($"{BudgetDir}/{file.FileName}:{firstDiff + 1}: not in canonical form (blocks sorted by tool name, exactly one blank line between blocks, no duplicates). Expected '{expected}', found '{actual}'. Canonical file written to {tempPath}.");
        }

        if (problems.Count > 0)
        {
            Assert.Fail($"{problems.Count} budget-layout problem(s):\n"
                + string.Join("\n", problems.OrderBy(p => p, StringComparer.Ordinal).Take(40)));
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

    /// <summary>
    /// #4075: an unpinned "param X.Y: 0" almost always means a plain new parameter (add the pin line and
    /// move on). But if the key's CLR type reads as a DI service (<see cref="McpServedSchema.IsServiceParameter"/>),
    /// that would mean a DI service leaked into the served schema, which the null-singleton registration in
    /// <see cref="BuildServedTools"/> is supposed to make impossible. Root cause is still open (see issue
    /// #4075); this appends everything the next sighting needs to diagnose it, without changing pass/fail.
    /// </summary>
    private static string DescribeUnpinnedIfServiceLeak(string key)
    {
        if (!key.StartsWith("param ", StringComparison.Ordinal))
        {
            return string.Empty;
        }

        var parameterKey = key["param ".Length..];
        if (_measureParameterTypes is null || !_measureParameterTypes.TryGetValue(parameterKey, out var parameterType)
            || !McpServedSchema.IsServiceParameter(parameterType))
        {
            return string.Empty;
        }

        var isServiceResult = _measureServiceProviderIsService is null
            ? "(no provider captured)"
            : _measureServiceProviderIsService.IsService(parameterType).ToString();

        return $" #4075 (DI service leaked into served schema; root cause still open): parameter CLR type is "
            + $"'{parameterType.FullName}'; IServiceProviderIsService.IsService(type) on this build's provider = {isServiceResult}; "
            + $"Measure() built on managed thread {_measureBuildThreadId}, build count = {_measureBuildCount}.";
    }

    private static (List<McpServerTool> Tools, List<Type> Types, IServiceProviderIsService IsService) BuildServedTools()
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

    /* ---------------- per-class budget files ---------------- */

    internal sealed record BudgetEntry(string Key, int Value, int LineNumber);

    internal sealed record BudgetFileContents(string ClassName, string FileName, IReadOnlyList<string> RawLines, IReadOnlyList<BudgetEntry> Entries);

    private static List<BudgetFileContents> ReadBudgetFiles()
    {
        var dir = RepoPath("Lite.Tests", BudgetDir);
        var files = new List<BudgetFileContents>();
        foreach (var filePath in Directory.GetFiles(dir, "*.txt").OrderBy(p => p, StringComparer.Ordinal))
        {
            var className = Path.GetFileNameWithoutExtension(filePath);
            var rawLines = File.ReadAllLines(filePath);
            var entries = new List<BudgetEntry>();
            for (var i = 0; i < rawLines.Length; i++)
            {
                var trimmed = rawLines[i].Trim();
                if (trimmed.Length == 0 || trimmed.StartsWith('#'))
                {
                    continue;
                }

                var at = trimmed.LastIndexOf(' ');
                entries.Add(new BudgetEntry(trimmed[..at], int.Parse(trimmed[(at + 1)..], CultureInfo.InvariantCulture), i + 1));
            }

            files.Add(new BudgetFileContents(className, Path.GetFileName(filePath), rawLines, entries));
        }

        return files;
    }

    /// <summary>Splits a budget key into its tool name and, for a parameter line, the parameter name.</summary>
    private static (string Tool, string? Parameter) ParseKey(string key)
    {
        if (key.StartsWith("tool ", StringComparison.Ordinal))
        {
            return (key["tool ".Length..], null);
        }

        if (key.StartsWith("param ", StringComparison.Ordinal))
        {
            var rest = key["param ".Length..];
            var dot = rest.IndexOf('.', StringComparison.Ordinal);
            return (rest[..dot], rest[(dot + 1)..]);
        }

        throw new InvalidOperationException($"unrecognized budget key '{key}'");
    }

    private sealed record Block(string Tool, int ToolValue, IReadOnlyList<(string Parameter, int Length)> Parameters);

    /// <summary>The one formatter both the live-measurement renderer and the on-disk canonical-form checker
    /// use, so a file this test generates always reads back as canonical.</summary>
    private static List<string> CanonicalLines(string className, IEnumerable<Block> blocks)
    {
        var lines = new List<string> { "# " + className + HeaderSuffix };
        var first = true;
        foreach (var block in blocks.OrderBy(b => b.Tool, StringComparer.Ordinal))
        {
            if (!first)
            {
                lines.Add(string.Empty);
            }

            first = false;
            lines.Add($"tool {block.Tool} {block.ToolValue.ToString(CultureInfo.InvariantCulture)}");
            foreach (var (parameter, length) in block.Parameters.OrderBy(p => p.Parameter, StringComparer.Ordinal))
            {
                lines.Add($"param {block.Tool}.{parameter} {length.ToString(CultureInfo.InvariantCulture)}");
            }
        }

        return lines;
    }

    internal static List<string> CanonicalLinesFromMeasurement(string className, IEnumerable<MeasuredTool> tools) =>
        CanonicalLines(className, tools.Select(t => new Block(t.Name, t.Served.Length, t.ParameterDescriptionLengths)));

    private static List<string> CanonicalLinesFromEntries(string className, IReadOnlyList<BudgetEntry> entries)
    {
        var toolValues = new Dictionary<string, int>(StringComparer.Ordinal);
        var paramsByTool = new Dictionary<string, List<(string Parameter, int Length)>>(StringComparer.Ordinal);
        foreach (var entry in entries)
        {
            var (tool, parameter) = ParseKey(entry.Key);
            if (parameter is null)
            {
                toolValues[tool] = entry.Value;
            }
            else
            {
                if (!paramsByTool.TryGetValue(tool, out var list))
                {
                    list = new List<(string, int)>();
                    paramsByTool[tool] = list;
                }

                list.Add((parameter, entry.Value));
            }
        }

        var orphans = paramsByTool.Keys.Where(t => !toolValues.ContainsKey(t)).OrderBy(t => t, StringComparer.Ordinal).ToList();
        if (orphans.Count > 0)
        {
            throw new InvalidOperationException($"parameter line(s) for tool(s) with no 'tool' line in this file: {string.Join(", ", orphans)}.");
        }

        return CanonicalLines(className, toolValues.Select(kv =>
            new Block(kv.Key, kv.Value, paramsByTool.TryGetValue(kv.Key, out var ps) ? ps : new List<(string, int)>())));
    }

    private static string JoinForDisk(IEnumerable<string> lines) => string.Join("\r\n", lines) + "\r\n";

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
