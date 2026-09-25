/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace PerformanceMonitor.Common;

/// <summary>
/// Two-tier tool descriptions (#3898 D1 + D3). A tool's <c>[Description]</c> literal may carry ONE
/// <see cref="Marker"/>: the text before it is the HEAD, which <c>tools/list</c> serves; the text after it is the
/// TAIL, the tool's reading guide, which only <see cref="ToolName"/> serves. A description with no marker is
/// served whole, exactly as before, until its family is converted.
///
/// <para>The split happens in ONE place, <c>McpSchemaCompat.WithGeminiCompatibleTools</c>, which both SKUs'
/// hosts register every tool through, so no tool carries its own split. Whatever a caller needs in order to
/// choose a tool, call it, or read its answer without being misled (the guardrail facts: which fields are
/// floors, which zeros are measurements, which windows are fixed, required parameters, read-only-ness) stays
/// in the head, and each SKU pins that per converted tool. The tail holds the long-form reading guidance.</para>
/// </summary>
public static class McpToolGuide
{
    /// <summary>
    /// The head/tail split marker, written inside a tool's existing <c>Description</c> literal. Chosen so it
    /// cannot occur in prose and greps cleanly: <c>grep -rn "&lt;&lt;GUIDE&gt;&gt;"</c> lists every converted
    /// tool.
    /// </summary>
    public const string Marker = "<<GUIDE>>";

    /// <summary>The name of the tool that serves the tails and the topic guides, on both SKUs.</summary>
    public const string ToolName = "get_tool_guide";

    /// <summary>
    /// Appended to every served head that has a tail, so a caller knows a guide exists and where it is. One
    /// sentence for every converted tool, added here rather than hand-written into each head.
    /// </summary>
    public const string GuidePointer = " Reading guide: get_tool_guide.";

    /// <summary>
    /// The most guide text (tails plus topics) one <see cref="ToolName"/> answer carries. Items that do not fit
    /// are listed under <c>deferred</c> for a second call rather than cut mid-sentence. Every single tail and
    /// topic must fit on its own (pinned by each SKU's budget test), so each is always reachable.
    /// </summary>
    public const int MaxGuideCharacters = 24_000;

    /// <summary>
    /// The most names read from each of the two arrays in one call. The rest are counted, not echoed, so a
    /// runaway argument cannot make the answer unbounded.
    /// </summary>
    public const int MaxNamesPerArray = 50;

    /// <summary>
    /// Splits a description at <see cref="Marker"/>. No marker: the whole description is the head and there is
    /// no tail. A marker with nothing on one side, or a second marker, is a malformed conversion and throws,
    /// which fails the host at startup and every schema test, rather than serving a half-split description.
    /// </summary>
    public static (string Head, string? Tail) Split(string description)
    {
        ArgumentNullException.ThrowIfNull(description);

        var at = description.IndexOf(Marker, StringComparison.Ordinal);
        if (at < 0)
        {
            return (description, null);
        }

        if (description.IndexOf(Marker, at + Marker.Length, StringComparison.Ordinal) >= 0)
        {
            throw new InvalidOperationException($"A tool description carries {Marker} more than once; it takes exactly one.");
        }

        var head = description[..at].TrimEnd();
        var tail = description[(at + Marker.Length)..].Trim();
        if (head.Length == 0 || tail.Length == 0)
        {
            throw new InvalidOperationException($"A tool description's {Marker} must have text on both sides: a head to choose the tool by, and a guide.");
        }

        return (head, tail);
    }

    /// <summary>
    /// The description <c>tools/list</c> serves: the head, plus <see cref="GuidePointer"/> when there is a tail; the
    /// unchanged description when there is no marker.
    /// </summary>
    public static string Served(string description)
    {
        var (head, tail) = Split(description);
        return tail is null ? description : head + GuidePointer;
    }

    /// <summary>
    /// The <see cref="ToolName"/> answer. Unknown names are reported in the result, never thrown; the answer is
    /// bounded by <see cref="MaxGuideCharacters"/> and <see cref="MaxNamesPerArray"/>. Called with neither array,
    /// it is the index: the tools that have a guide and the topics, each topic with its one-line summary.
    /// </summary>
    public static string Render(
        McpToolGuideCatalog catalog,
        IReadOnlyList<McpToolGuideTopic> topics,
        IReadOnlyList<string>? tools,
        IReadOnlyList<string>? topicNames)
    {
        ArgumentNullException.ThrowIfNull(catalog);
        ArgumentNullException.ThrowIfNull(topics);

        var (requestedTools, droppedTools) = Normalize(tools);
        var (requestedTopics, droppedTopics) = Normalize(topicNames);

        if (requestedTools.Count == 0 && requestedTopics.Count == 0)
        {
            return JsonSerializer.Serialize(new
            {
                status = "ok",
                message = "Pass tool names in tools, or topic names in topics, to read their guides.",
                tools_with_guides = catalog.ToolsWithGuides,
                topics = topics.Select(t => new { name = t.Name, summary = t.Summary }).ToArray(),
            }, McpHelpers.JsonOptions);
        }

        var budget = MaxGuideCharacters;
        var toolGuides = new List<object>();
        var noGuide = new List<string>();
        var unknownTools = new List<string>();
        var deferredTools = new List<string>();
        foreach (var name in requestedTools)
        {
            if (!catalog.TryGetTool(name, out var canonical, out var guide))
            {
                unknownTools.Add(name);
            }
            else if (guide is null)
            {
                noGuide.Add(canonical);
            }
            else if (guide.Length <= budget)
            {
                toolGuides.Add(new { name = canonical, guide });
                budget -= guide.Length;
            }
            else
            {
                deferredTools.Add(canonical);
            }
        }

        var topicGuides = new List<object>();
        var unknownTopics = new List<string>();
        var deferredTopics = new List<string>();
        foreach (var name in requestedTopics)
        {
            var topic = topics.FirstOrDefault(t => string.Equals(t.Name, name, StringComparison.OrdinalIgnoreCase));
            if (topic is null)
            {
                unknownTopics.Add(name);
            }
            else if (topic.Guide.Length <= budget)
            {
                topicGuides.Add(new { name = topic.Name, guide = topic.Guide });
                budget -= topic.Guide.Length;
            }
            else
            {
                deferredTopics.Add(topic.Name);
            }
        }

        var answer = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["status"] = "ok",
            ["tools"] = toolGuides,
            ["topics"] = topicGuides,
        };
        if (noGuide.Count > 0)
        {
            /* A known tool with no tail: its tools/list description is already all there is. */
            answer["no_separate_guide"] = noGuide;
        }
        if (unknownTools.Count > 0)
        {
            answer["unknown_tools"] = unknownTools;
        }
        if (unknownTopics.Count > 0)
        {
            answer["unknown_topics"] = unknownTopics;
            answer["available_topics"] = topics.Select(t => t.Name).ToArray();
        }
        if (deferredTools.Count > 0 || deferredTopics.Count > 0)
        {
            answer["deferred"] = new { tools = deferredTools, topics = deferredTopics };
            answer["deferred_reason"] = $"Over this answer's {MaxGuideCharacters:N0}-character guide budget. Ask for the deferred names in another call.";
        }
        if (droppedTools + droppedTopics > 0)
        {
            answer["names_not_read"] = droppedTools + droppedTopics;
            answer["names_not_read_reason"] = $"Only the first {MaxNamesPerArray} names of each array are read.";
        }

        return JsonSerializer.Serialize(answer, McpHelpers.JsonOptions);
    }

    /// <summary>Trims, drops blanks, de-duplicates case-insensitively (first spelling wins), keeps the caller's
    /// order, and reads at most <see cref="MaxNamesPerArray"/> names; returns how many were not read.</summary>
    private static (List<string> Names, int Dropped) Normalize(IReadOnlyList<string>? names)
    {
        var result = new List<string>();
        if (names is null)
        {
            return (result, 0);
        }

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var dropped = 0;
        foreach (var raw in names)
        {
            var name = raw?.Trim();
            if (string.IsNullOrEmpty(name) || !seen.Add(name))
            {
                continue;
            }

            if (result.Count < MaxNamesPerArray)
            {
                result.Add(name);
            }
            else
            {
                dropped++;
            }
        }

        return (result, dropped);
    }
}

/// <summary>
/// The two parameter descriptions of <see cref="McpToolGuide.ToolName"/>, shared so both SKUs advertise the
/// same schema (D6). The tool's own description is a literal on each SKU, pinned byte-identical.
/// </summary>
public static class McpToolGuideToolText
{
    /// <summary>The <c>tools</c> parameter's description.</summary>
    public const string ToolsParameter =
        "Tool names whose reading guides to return, e.g. [\"get_health_parser_cpu_tasks\"]. Case-insensitive.";

    /// <summary>The <c>topics</c> parameter's description.</summary>
    public const string TopicsParameter =
        "Cross-tool topic names, e.g. [\"system_health_empty_windows\"]. Call with no arguments to list them.";
}

/// <summary>A named cross-tool reading guide served by <see cref="McpToolGuide.ToolName"/>.</summary>
public sealed record McpToolGuideTopic(string Name, string Summary, string Guide);

/// <summary>
/// Every tool one host registered, with its tail when it has one. Filled by
/// <c>McpSchemaCompat.WithGeminiCompatibleTools</c> as tools are registered (one instance per service
/// collection), and injected into <see cref="McpToolGuide.ToolName"/> as a DI service, so the guide can only
/// ever describe the tools this host actually serves.
/// </summary>
public sealed class McpToolGuideCatalog
{
    private readonly Dictionary<string, string?> _tails = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, string> _canonical = new(StringComparer.OrdinalIgnoreCase);

    /// <summary>Records one registered tool. A second registration of the same name is a host defect and throws.</summary>
    public void Register(string name, string? description)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        var tail = description is null ? null : McpToolGuide.Split(description).Tail;
        if (!_tails.TryAdd(name, tail))
        {
            throw new InvalidOperationException($"MCP tool '{name}' is registered twice.");
        }

        _canonical[name] = name;
    }

    /// <summary>Every registered tool name, ordinal order.</summary>
    public IReadOnlyList<string> Tools => _canonical.Values.OrderBy(n => n, StringComparer.Ordinal).ToArray();

    /// <summary>The registered tools that have a tail, ordinal order.</summary>
    public IReadOnlyList<string> ToolsWithGuides => _tails
        .Where(kv => kv.Value is not null)
        .Select(kv => _canonical[kv.Key])
        .OrderBy(n => n, StringComparer.Ordinal)
        .ToArray();

    /// <summary>Case-insensitive lookup: whether the tool is registered, its registered spelling, and its tail
    /// (null when it has none).</summary>
    public bool TryGetTool(string name, out string canonical, out string? tail)
    {
        if (_tails.TryGetValue(name, out tail))
        {
            canonical = _canonical[name];
            return true;
        }

        canonical = name;
        return false;
    }
}
