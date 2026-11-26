/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>One key of an add entry that the add core does not read, with the key it does read that the caller most likely meant.</summary>
internal sealed record UnreadServerKey(string Key, string? DidYouMean);

/// <summary>
/// #9780: names the keys of an <c>add_servers</c> / <c>--add-server</c> entry that the add core does not read. An unread key is dropped
/// by the parse, so <c>"name"</c> for <c>display_name</c> adds the server under its host name and a misspelt
/// <c>trustServerCertifcate</c> adds it with the default, each with no word to the caller. The answer carries the key names so
/// the caller sees what was not applied; the entry is still processed (a script that sends extra keys keeps working).
///
/// <para>Only key names are produced. No value is read, so a password, a client secret or an external ID cannot reach the answer.</para>
/// </summary>
internal static class DarlingMcpAddServerKeys
{
    /// <summary>Every key <see cref="DarlingMcpServerAdminTools.ParseRequest(string)"/> reads from an entry. A key the parse starts to read
    /// is added here, or the answer names it as unread.</summary>
    internal static readonly string[] Read =
    [
        "host", "display_name", "database", "engine", "auth", "username", "password", "port",
        "encrypt_mode", "trust_server_certificate", "read_only_intent", "multi_subnet_failover",
        "aws_role_arn", "aws_external_id", "query_store_replica", "capture_plans", "alert_delivery_mode_override",
    ];

    /// <summary>Keys that are not a spelling of a key the core reads but plainly mean one: the registered name is <c>display_name</c>.</summary>
    private static readonly Dictionary<string, string> s_aliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["name"] = "display_name",
    };

    private static readonly HashSet<string> s_readSet = new(Read, StringComparer.Ordinal);

    /// <summary>The unread keys of every entry in <paramref name="serversJson"/>, by the entry's index in the array. An entry that is not
    /// an object has none here: the parse already answers for it.</summary>
    internal static Dictionary<int, IReadOnlyList<UnreadServerKey>> ByEntry(string serversJson)
    {
        var byEntry = new Dictionary<int, IReadOnlyList<UnreadServerKey>>();
        if (JsonNode.Parse(serversJson) is not JsonArray array)
        {
            return byEntry;
        }

        for (var i = 0; i < array.Count; i++)
        {
            if (array[i] is JsonObject entry && UnreadIn(entry) is { Count: > 0 } unread)
            {
                byEntry[i] = unread;
            }
        }

        return byEntry;
    }

    /// <summary>The keys of <paramref name="entry"/> the core does not read, in the entry's order. A key that starts with <c>// </c> is a note
    /// (the sample files carry them beside a setting) and is never reported.</summary>
    internal static IReadOnlyList<UnreadServerKey> UnreadIn(JsonObject entry)
    {
        var unread = new List<UnreadServerKey>();
        foreach (var (key, _) in entry)
        {
            if (s_readSet.Contains(key) || key.StartsWith(DarlingUnknownKeys.CommentKeyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            unread.Add(new UnreadServerKey(key, Suggest(key)));
        }

        return unread;
    }

    /// <summary>The key the core reads that <paramref name="key"/> is a spelling of, or null. Case, underscores and hyphens are not
    /// part of the spelling (<c>displayName</c>, <c>Display-Name</c>), then one edit is (<c>trustServerCertifcate</c>).</summary>
    internal static string? Suggest(string key)
    {
        if (s_aliases.TryGetValue(key, out var alias))
        {
            return alias;
        }

        var folded = Fold(key);
        string? nearest = null;
        foreach (var candidate in Read)
        {
            var candidateFolded = Fold(candidate);
            if (folded == candidateFolded)
            {
                return candidate;
            }

            if (nearest is null && ComposeSpec.EditDistance(folded, candidateFolded) <= 1)
            {
                nearest = candidate;
            }
        }

        return nearest;
    }

    private static string Fold(string key) =>
        string.Concat(key.Where(c => c is not ('_' or '-' or ' '))).ToLowerInvariant();

    /// <summary>The list as the verb prints it: <c>name (did you mean display_name?), foo</c>.</summary>
    internal static string Describe(IEnumerable<(string Key, string? DidYouMean)> keys) =>
        string.Join(", ", keys.Select(k => k.DidYouMean is null ? k.Key : $"{k.Key} (did you mean {k.DidYouMean}?)"));
}
