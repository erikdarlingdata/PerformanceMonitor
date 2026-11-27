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
using System.Text;
using System.Text.Json;
using ModelContextProtocol.Server;

namespace PerformanceMonitor.Common;

/// <summary>
/// <c>find_tools</c> (#9388): a search over the tools a caller can call, so a client that loads every
/// <c>tools/list</c> definition up front can ask for the two or three a question needs. Shared by both SKUs'
/// twin tool classes, like <see cref="McpToolGuide"/>.
///
/// <para>The search runs over the tools the CALLER'S request serves, never over the registration catalog. The
/// caller passes the collection the request's own <c>tools/list</c> serves and <c>tools/call</c> dispatches
/// over (the SDK's <c>McpServer.ServerOptions.ToolCollection</c> after the host's per-request options hook ran),
/// so a tool that a token's policy, a write switch or a path profile leaves out is never named. It reads no
/// store data and no monitored server.</para>
/// </summary>
public static class McpToolSearch
{
    /// <summary>The name of the search tool, on both SKUs.</summary>
    public const string ToolName = "find_tools";

    /// <summary>The most results at <c>names</c> and <c>summaries</c>. A limit above it is refused. The count used when the caller
    /// gives none is <see cref="DefaultLimit"/>.</summary>
    public const int MaxLimit = 50;

    /// <summary>The count a caller gets when it gives no limit at <c>names</c> or <c>summaries</c>.</summary>
    public const int DefaultLimit = 20;

    /// <summary>The most results at <c>full</c>, and the count used when the caller gives none there.</summary>
    public const int MaxFullLimit = 10;

    /// <summary>The longest first sentence a <c>summaries</c> entry carries.</summary>
    public const int MaxSummaryCharacters = 240;

    /// <summary>The detail levels, in the order the parameter description lists them.</summary>
    public static readonly IReadOnlyList<string> Details = new[] { "names", "summaries", "full" };

    /// <summary>The note every answer that names tools, or says none match, carries (#9584). Both SKUs serve MCP over stateless HTTP, so
    /// the service has no session to push <c>tools/list_changed</c> on: a client reads its tool list once, when it connects, and a tool
    /// the service gained since then is in this answer and not in the client's list. A SKU adds its own way round it through
    /// <c>skuNote</c> at <see cref="Render"/>.</summary>
    public const string ReconnectNote =
        "This search covers the tools this service serves as it runs now. A client reads its own tool list when it connects and is not told when the service's list changes, " +
        "so a tool named here that the client cannot call needs the client to reconnect.";

    /// <summary>Room kept for the answer's own fields when <c>full</c> entries are packed into the response budget.</summary>
    private const int EnvelopeBytes = 2_048;

    /// <summary>
    /// The <see cref="ToolName"/> answer over <paramref name="served"/>. Never throws for a caller's input: an empty
    /// query, an unknown <paramref name="detail"/> and an out-of-range <paramref name="limit"/> are answered in the result.
    /// Every answer with results or with no match ends its notes with <see cref="ReconnectNote"/>, followed by
    /// <paramref name="skuNote"/> when the SKU has a way round a stale client tool list. The empty-query error names no tool and carries no note.
    /// </summary>
    public static string Render(IEnumerable<McpServerTool>? served, string? query, string? detail, int? limit, string? skuNote = null)
    {
        var words = Words(query);
        if (words.Count == 0)
        {
            return JsonSerializer.Serialize(new
            {
                status = "error",
                message = "Give at least one word to search for, for example query=\"wait stats\" or query=\"deadlock\". Words are matched against tool names and descriptions, case-insensitively.",
            }, McpHelpers.JsonOptions);
        }

        var notes = new List<string>();
        var level = "summaries";
        if (!string.IsNullOrWhiteSpace(detail))
        {
            var found = Details.FirstOrDefault(d => string.Equals(d, detail.Trim(), StringComparison.OrdinalIgnoreCase));
            if (found is null)
            {
                notes.Add($"detail \"{detail.Trim()}\" is not one of {string.Join(", ", Details)}, so summaries was used.");
            }
            else
            {
                level = found;
            }
        }

        var max = level == "full" ? MaxFullLimit : MaxLimit;
        var take = limit ?? (level == "full" ? MaxFullLimit : DefaultLimit);
        if (take < 1)
        {
            notes.Add($"limit {take} is below 1, so 1 was used.");
            take = 1;
        }
        else if (take > max)
        {
            notes.Add($"limit {take} is above the most {level} returns, so {max} was used.");
            take = max;
        }

        var tools = (served ?? Array.Empty<McpServerTool>())
            .Where(t => !string.Equals(t.ProtocolTool.Name, ToolName, StringComparison.Ordinal))
            .ToList();
        var hasGuideTool = tools.Any(t => string.Equals(t.ProtocolTool.Name, McpToolGuide.ToolName, StringComparison.Ordinal));

        var ranked = tools
            .Select(t => Candidate.Of(t, words))
            .Where(c => c.NameHits + c.HeadHits > 0)
            .OrderByDescending(c => c.NameHits)
            .ThenByDescending(c => c.HeadHits)
            .ThenBy(c => c.Name, StringComparer.Ordinal)
            .ToList();

        var reconnect = string.IsNullOrWhiteSpace(skuNote) ? ReconnectNote : ReconnectNote + " " + skuNote.Trim();

        if (ranked.Count == 0)
        {
            notes.Add(reconnect);
            return JsonSerializer.Serialize(new
            {
                status = "no_match",
                query = query!.Trim(),
                matched = 0,
                tools_searched = tools.Count,
                message = "No tool you can call matches those words. Try fewer or broader words, for example one metric name such as \"wait\", \"deadlock\" or \"memory\", or a part of a tool name such as \"get_\".",
                notes,
            }, McpHelpers.JsonOptions);
        }

        var picked = ranked.Take(take).ToList();
        var answer = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["status"] = "ok",
            ["query"] = query!.Trim(),
            ["detail"] = level,
            ["limit"] = take,
            ["matched"] = ranked.Count,
        };

        List<string> deferred = new();
        switch (level)
        {
            case "names":
                answer["tools"] = picked.Select(c => c.Name).ToArray();
                break;
            case "summaries":
                answer["tools"] = picked.Select(c => new { name = c.Name, summary = FirstSentence(c.Head) }).ToArray();
                break;
            default:
                var entries = new List<object>();
                var used = EnvelopeBytes;
                foreach (var c in picked)
                {
                    var entry = FullEntry(c, hasGuideTool);
                    var bytes = Encoding.UTF8.GetByteCount(JsonSerializer.Serialize(entry, McpHelpers.JsonOptions));
                    /* The first entry always goes in, so a call makes progress even when one tool is large. */
                    if (entries.Count > 0 && used + bytes > McpResponseBudget.DefaultBytes)
                    {
                        deferred.Add(c.Name);
                        continue;
                    }

                    entries.Add(entry);
                    used += bytes;
                }

                answer["tools"] = entries;
                break;
        }

        answer["returned"] = picked.Count - deferred.Count;
        var leftOver = ranked.Count - picked.Count;
        if (leftOver > 0)
        {
            notes.Add($"{leftOver} more tool(s) match. Add words to narrow the search, or raise limit (up to {max} at {level}).");
        }

        if (deferred.Count > 0)
        {
            answer["deferred"] = deferred;
            notes.Add($"{deferred.Count} matching tool(s) did not fit this answer's size budget and are named under deferred. Ask for them again with narrower words.");
        }

        notes.Add(reconnect);
        answer["notes"] = notes;

        return JsonSerializer.Serialize(answer, McpHelpers.JsonOptions);
    }

    /// <summary>The distinct, lower-cased query words. Words split on spaces and commas only, so a part of a tool name such as
    /// <c>wait_stats</c> stays one word.</summary>
    private static List<string> Words(string? query) =>
        (query ?? string.Empty)
            .Split(new[] { ' ', '\t', '\r', '\n', ',', ';' }, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(w => w.ToLowerInvariant())
            .Distinct(StringComparer.Ordinal)
            .ToList();

    /// <summary>The first sentence of a description head, cut at <see cref="MaxSummaryCharacters"/> when it runs longer.</summary>
    internal static string FirstSentence(string head)
    {
        var text = head.Trim();
        var end = -1;
        for (var i = 0; i < text.Length; i++)
        {
            if (text[i] == '.' && (i + 1 == text.Length || char.IsWhiteSpace(text[i + 1])))
            {
                end = i + 1;
                break;
            }
        }

        var sentence = end < 0 ? text : text[..end];
        return sentence.Length <= MaxSummaryCharacters ? sentence : McpHelpers.Truncate(sentence, MaxSummaryCharacters) ?? sentence;
    }

    private static Dictionary<string, object?> FullEntry(Candidate c, bool hasGuideTool)
    {
        var entry = new Dictionary<string, object?>(StringComparer.Ordinal)
        {
            ["name"] = c.Name,
            ["description"] = c.Head,
            ["input_schema"] = c.Tool.ProtocolTool.InputSchema,
        };

        var annotations = c.Tool.ProtocolTool.Annotations;
        if (annotations is not null)
        {
            var hints = new Dictionary<string, object?>(StringComparer.Ordinal);
            if (!string.IsNullOrEmpty(annotations.Title)) hints["title"] = annotations.Title;
            if (annotations.ReadOnlyHint is { } readOnly) hints["readOnlyHint"] = readOnly;
            if (annotations.DestructiveHint is { } destructive) hints["destructiveHint"] = destructive;
            if (annotations.IdempotentHint is { } idempotent) hints["idempotentHint"] = idempotent;
            if (annotations.OpenWorldHint is { } openWorld) hints["openWorldHint"] = openWorld;
            entry["annotations"] = hints;
        }

        if (c.HasGuide)
        {
            entry["has_reading_guide"] = true;
            if (hasGuideTool)
            {
                entry["reading_guide"] = $"Call {McpToolGuide.ToolName} with tools=[\"{c.Name}\"] for what its fields mean and its edge cases.";
            }
        }

        return entry;
    }

    /// <summary>One served tool with its search scores.</summary>
    private sealed record Candidate(McpServerTool Tool, string Name, string Head, bool HasGuide, int NameHits, int HeadHits)
    {
        public static Candidate Of(McpServerTool tool, IReadOnlyList<string> words)
        {
            var name = tool.ProtocolTool.Name;
            var description = tool.ProtocolTool.Description ?? string.Empty;

            /* tools/list serves the head plus the guide pointer (McpToolGuide.Served). Search and show the head. */
            var hasGuide = description.EndsWith(McpToolGuide.GuidePointer, StringComparison.Ordinal);
            var head = hasGuide ? description[..^McpToolGuide.GuidePointer.Length] : description;

            return new Candidate(
                tool,
                name,
                head,
                hasGuide,
                words.Count(w => name.Contains(w, StringComparison.OrdinalIgnoreCase)),
                words.Count(w => head.Contains(w, StringComparison.OrdinalIgnoreCase)));
        }
    }
}

/// <summary>
/// The parameter descriptions of <see cref="McpToolSearch.ToolName"/>, shared so both SKUs advertise the same schema
/// (D6). The tool's own description is a literal on each SKU.
/// </summary>
public static class McpToolSearchToolText
{
    /// <summary>The tool's description, one literal both SKUs use, so the twins cannot drift.</summary>
    public const string Description =
        "Searches the tools you can call and returns the ones a question needs, so you need not load every definition first. " +
        "Pass words in query: a tool matches when any word is in its name or description, and a tool with more of the words in its name comes first. " +
        "detail is names, summaries (the default) or full, which adds the input schema and hints so you can call the tool. " +
        "It names only tools this connection can call: a tool that a token's settings, the write switches or the path leave out is never listed. " +
        "Read-only: reads no collected data and no monitored server.";

    /// <summary>The <c>query</c> parameter's description.</summary>
    public const string QueryParameter =
        "Words to look for in tool names and descriptions, case-insensitive, e.g. \"deadlock\" or \"wait stats\". A tool matches when any word is in its name or description.";

    /// <summary>The <c>detail</c> parameter's description.</summary>
    public const string DetailParameter =
        "names (just the tool names), summaries (the default: each name and the first sentence of its description) or full (name, description, input schema and hints).";

    /// <summary>The <c>limit</c> parameter's description.</summary>
    public const string LimitParameter =
        "Most tools to return. Default 20 (10 at full). At most 50 (10 at full): a larger value, or one below 1, is refused.";
}
