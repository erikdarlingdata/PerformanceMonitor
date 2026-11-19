/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using ModelContextProtocol.Protocol;

namespace PerformanceMonitor.Common;

/// <summary>How a <c>database_name</c> a caller gave stands against the names a server is known to have.</summary>
public enum McpDatabaseNameMatch
{
    /// <summary>No name was given (null, empty or whitespace only): the read covers every database.</summary>
    NotAsked,

    /// <summary>A known name is the given one, character for character.</summary>
    Exact,

    /// <summary>No known name is the given one exactly and one known name is it ignoring letter case.</summary>
    Resolved,

    /// <summary>No known name is the given one exactly and two or more are it ignoring letter case.</summary>
    Ambiguous,

    /// <summary>No known name is the given one, with or without its letter case.</summary>
    NotSeen,
}

/// <summary>
/// The answer of <see cref="McpDatabaseNameResolver.Resolve"/>. <see cref="Name"/> is the name the read uses: the given one for
/// <see cref="McpDatabaseNameMatch.Exact"/> and <see cref="McpDatabaseNameMatch.NotSeen"/>, the stored spelling for
/// <see cref="McpDatabaseNameMatch.Resolved"/>, null otherwise. <see cref="Candidates"/> holds the stored spellings of an
/// <see cref="McpDatabaseNameMatch.Ambiguous"/> name in ordinal order, and is empty otherwise.
/// </summary>
public readonly record struct McpDatabaseNameResolution(McpDatabaseNameMatch Match, string? Name, IReadOnlyList<string> Candidates);

/// <summary>
/// #9065: the one rule for the <c>database_name</c> an MCP caller gives, on both SKUs. The reads compare a name with <c>=</c>, which
/// is case-sensitive in the store, while the database names of a SQL Server follow a collation that almost always ignores case. So the
/// name is resolved to its stored spelling before the read runs, by <see cref="McpDatabaseNameFilter"/>, and the read keeps its exact
/// comparison.
///
/// <para>An exact match wins, so two names that differ only by case stay two databases to a caller who spells either one as stored. A
/// name is never trimmed: a leading or trailing space is part of it. Only when nothing matches exactly is letter case ignored, and
/// then only a single match is used; two or more are refused with the list, never guessed between.</para>
///
/// <para>The names a host hands in are the newest list's, plus the given spelling itself when the host finds it on an earlier list
/// in its look-back (Darling reads the hourly lists, Lite every list): a database renamed or recreated in another letter case keeps
/// its history reachable under the spelling that history was stored with. A tool that changes state (<see cref="ChangesState"/>) is never handed another spelling: a case-only mismatch is refused
/// with the stored one.</para>
/// </summary>
public static class McpDatabaseNameResolver
{
    /// <summary>The tool parameter this resolves.</summary>
    public const string Parameter = "database_name";

    /// <summary>The tool parameter that names the server whose databases the name is resolved against.</summary>
    public const string ServerParameter = "server_name";

    /// <summary>The answer field that carries the stored spelling when the read used one that differs from the name given.</summary>
    public const string ResolvedField = "resolved_database_name";

    /// <summary>The answer field that says the server's database list holds no such name.</summary>
    public const string NoteField = "database_name_note";

    /// <summary>The answer field beside <see cref="ResolvedField"/> that says when the list the name was resolved against was taken.</summary>
    public const string ResolvedAsOfField = "resolved_database_name_as_of";

    /// <summary>What the served <c>database_name</c> parameter description of a resolving tool ends with.</summary>
    public const string MatchingSentence = "Exact match first, then a unique case-insensitive one.";

    /// <summary>What the served <c>database_name</c> parameter description of a resolving tool that changes state ends with. No longer
    /// than <see cref="MatchingSentence"/>, the sentence a tool is registered with, so a description that fits its length cap when
    /// registered still fits once the write policy restates it.</summary>
    public const string ExactOnlySentence = "Exact only: another case of a listed name is refused.";

    /// <summary>
    /// Resolves <paramref name="given"/> against <paramref name="known"/>, the names the server is known to have (the whole list, or
    /// only the ones that equal the given name ignoring case: the answer is the same).
    /// </summary>
    public static McpDatabaseNameResolution Resolve(string? given, IEnumerable<string?>? known)
    {
        if (string.IsNullOrWhiteSpace(given))
        {
            return new McpDatabaseNameResolution(McpDatabaseNameMatch.NotAsked, null, Array.Empty<string>());
        }

        var folded = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var name in known ?? Array.Empty<string?>())
        {
            if (name is null)
            {
                continue;
            }

            if (string.Equals(name, given, StringComparison.Ordinal))
            {
                return new McpDatabaseNameResolution(McpDatabaseNameMatch.Exact, given, Array.Empty<string>());
            }

            if (string.Equals(name, given, StringComparison.OrdinalIgnoreCase) && seen.Add(name))
            {
                folded.Add(name);
            }
        }

        return folded.Count switch
        {
            0 => new McpDatabaseNameResolution(McpDatabaseNameMatch.NotSeen, given, Array.Empty<string>()),
            1 => new McpDatabaseNameResolution(McpDatabaseNameMatch.Resolved, folded[0], Array.Empty<string>()),
            _ => new McpDatabaseNameResolution(
                McpDatabaseNameMatch.Ambiguous, null, folded.OrderBy(name => name, StringComparer.Ordinal).ToArray()),
        };
    }

    /// <summary>The refusal sentence for a name that two or more stored names equal ignoring case: it lists them.</summary>
    public static string AmbiguousMessage(string given, IReadOnlyList<string> candidates, DateTime? listTakenUtc = null) =>
        $"database_name '{given}' matches {candidates.Count} databases on this server that differ only by letter case: "
        + string.Join(", ", candidates.Select(name => "'" + name + "'"))
        + ". Pass one of them spelled exactly as listed. The call was refused before it ran." + ListTaken(listTakenUtc);

    /// <summary>
    /// What an answer says when the server's database list holds no such name. It states only what the lookup compared and that the
    /// name was not changed, so it is true beside rows too (a dropped database's history) and for a tool that trims the name itself.
    /// The comparison is the store's <c>lower()</c> and then ordinal-ignore-case here, so the message claims no more than a simple
    /// letter-case comparison.
    /// </summary>
    public static string NotSeenMessage(string given, DateTime? listTakenUtc = null) =>
        $"database_name '{given}' matched no name on this server's newest database list, exactly or by a simple letter-case comparison "
        + "(no accent or language-specific casing rules), so the name was left as given. "
        + "A database dropped before that list was taken, or created after it, is not on it." + ListTaken(listTakenUtc);

    /// <summary>The refusal sentence for a tool that changes state and a name that is another letter case of one stored name.</summary>
    public static string ExactRequiredMessage(string given, string stored, DateTime? listTakenUtc = null) =>
        $"database_name '{given}' is not a database on this server's database list; '{stored}' is, in another letter case. "
        + $"This tool changes state, so the name is not corrected for you: pass '{stored}' exactly. The call was refused before it ran."
        + ListTaken(listTakenUtc);

    /// <summary>
    /// A list's collection time as an answer carries it: UTC in the ISO-8601 round-trip form, the format <c>find_database</c> gives
    /// its <c>newest_snapshot_at</c>.
    /// </summary>
    public static string AsOf(DateTime listTakenUtc) =>
        (listTakenUtc.Kind == DateTimeKind.Local ? listTakenUtc.ToUniversalTime() : DateTime.SpecifyKind(listTakenUtc, DateTimeKind.Utc))
        .ToString("o", CultureInfo.InvariantCulture);

    /// <summary>
    /// The sentence that dates the newest list a message speaks of, so a list taken days ago is not read as the server's present
    /// state; empty when the host gave no time.
    /// </summary>
    private static string ListTaken(DateTime? listTakenUtc) =>
        listTakenUtc is { } taken ? $" This server's newest database list was taken at {AsOf(taken)}." : string.Empty;

    /// <summary>
    /// True when the tool changes state: the host's write policy stamped <c>destructiveHint</c> on it, the one marker both SKUs set
    /// for every tool behind their write gate.
    /// </summary>
    public static bool ChangesState(Tool? tool) => tool?.Annotations?.DestructiveHint == true;

    /// <summary>
    /// True when the tool takes a <c>database_name</c> that is resolved: its served schema declares both <c>database_name</c> and
    /// <c>server_name</c>. A tool with no <c>server_name</c> has no one server whose list the name could be resolved against.
    /// </summary>
    public static bool Resolves(Tool? tool)
    {
        if (tool is null || tool.InputSchema.ValueKind != JsonValueKind.Object
            || !tool.InputSchema.TryGetProperty("properties", out var properties) || properties.ValueKind != JsonValueKind.Object)
        {
            return false;
        }

        return properties.TryGetProperty(Parameter, out _) && properties.TryGetProperty(ServerParameter, out _);
    }

    /// <summary>
    /// Ends the served <c>database_name</c> description of a resolving tool with <see cref="MatchingSentence"/>, or with
    /// <see cref="ExactOnlySentence"/> when the tool changes state, so the description a caller reads and what the filter does are
    /// decided by the same two predicates, <see cref="Resolves"/> and <see cref="ChangesState"/>. Any other tool is left as it is.
    /// Called where a tool is registered and again where a host's write policy stamps its annotations: the second call swaps the
    /// sentence of a tool the stamp marked.
    /// </summary>
    public static void DescribeMatching(Tool tool)
    {
        ArgumentNullException.ThrowIfNull(tool);
        if (!Resolves(tool) || JsonNode.Parse(tool.InputSchema.GetRawText()) is not JsonObject schema
            || schema["properties"]?[Parameter] is not JsonObject parameter)
        {
            return;
        }

        var described = parameter["description"] is JsonValue value && value.TryGetValue<string>(out var text) ? text.TrimEnd() : string.Empty;
        var sentence = ChangesState(tool) ? ExactOnlySentence : MatchingSentence;
        if (described.EndsWith(sentence, StringComparison.Ordinal))
        {
            return;
        }

        foreach (var other in new[] { MatchingSentence, ExactOnlySentence })
        {
            if (described.EndsWith(other, StringComparison.Ordinal))
            {
                described = described[..^other.Length].TrimEnd();
            }
        }

        parameter["description"] = described.Length == 0 ? sentence : described + " " + sentence;
        tool.InputSchema = JsonSerializer.SerializeToElement(schema);
    }

    /// <summary>
    /// <paramref name="json"/> with one more string field at the end of its top-level object, every byte it already held kept as it
    /// is. Null when the text is not a JSON object or already has the field, so the caller returns the answer unchanged.
    /// </summary>
    public static string? WithField(string? json, string field, string value)
    {
        if (json is null)
        {
            return null;
        }

        var close = json.AsSpan().TrimEnd().Length - 1;
        if (close < 1 || json[close] != '}')
        {
            return null;
        }

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Object || root.TryGetProperty(field, out _))
            {
                return null;
            }

            var separator = root.EnumerateObject().Any() ? "," : string.Empty;
            return string.Concat(
                json.AsSpan(0, close),
                separator + JsonSerializer.Serialize(field, McpHelpers.JsonOptions) + ":" + JsonSerializer.Serialize(value, McpHelpers.JsonOptions),
                json.AsSpan(close));
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
