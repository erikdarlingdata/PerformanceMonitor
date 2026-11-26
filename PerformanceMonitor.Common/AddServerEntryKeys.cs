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

namespace PerformanceMonitor.Common;

/// <summary>One key of an add entry that the add parse does not read, with the key it does read that the caller most likely meant.</summary>
internal sealed record UnreadServerKey(string Key, string? DidYouMean);

/// <summary>
/// #9780: names the keys of an <c>add_servers</c> / <c>--add-server</c> entry that the add parse does not read. An unread key is dropped by
/// the parse, so <c>"name"</c> for <c>display_name</c> adds the server under its host name and a misspelt <c>trustServerCertifcate</c>
/// adds it with the default, each with no word to the caller. The answer carries the key names so the caller sees what was not applied;
/// the entry is still processed (a script that sends extra keys keeps working).
///
/// <para>Darling and Lite each build one over the keys their own parse reads, with the comparer their own parse matches names by
/// (Darling is case-sensitive, Lite is not), so the two never report a key the other one reads.</para>
///
/// <para>Only key names are produced. No value is read, so a password, a client secret or an external ID cannot reach the answer.</para>
/// </summary>
internal sealed class AddServerEntryKeys
{
    /// <summary>A key that starts with this is a note (the sample files carry them beside a setting) and is never reported.</summary>
    internal const string CommentKeyPrefix = "// ";

    /// <summary>Keys that are not a spelling of a key the parse reads but plainly mean one: the registered name is <c>display_name</c>.</summary>
    private static readonly Dictionary<string, string> s_aliases = new(StringComparer.OrdinalIgnoreCase)
    {
        ["name"] = "display_name",
    };

    private readonly string[] _read;
    private readonly HashSet<string> _readSet;

    /// <param name="read">Every key the parse reads. A key the parse starts to read is added here, or the answer names it as unread.</param>
    /// <param name="comparer">How the parse matches a key name against <paramref name="read"/>.</param>
    internal AddServerEntryKeys(IEnumerable<string> read, StringComparer comparer)
    {
        _read = read.ToArray();
        _readSet = new HashSet<string>(_read, comparer);
    }

    /// <summary>The keys of an entry the parse does not read, in the entry's order.</summary>
    internal IReadOnlyList<UnreadServerKey> Unread(IEnumerable<string> entryKeys)
    {
        var unread = new List<UnreadServerKey>();
        foreach (var key in entryKeys)
        {
            if (_readSet.Contains(key) || key.StartsWith(CommentKeyPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            unread.Add(new UnreadServerKey(key, Suggest(key)));
        }

        return unread;
    }

    /// <summary>The key the parse reads that <paramref name="key"/> is a spelling of, or null. Case, underscores and hyphens are not
    /// part of the spelling (<c>displayName</c>, <c>Display-Name</c>), then one edit is (<c>trustServerCertifcate</c>).</summary>
    internal string? Suggest(string key)
    {
        if (s_aliases.TryGetValue(key, out var alias) && _readSet.Contains(alias))
        {
            return alias;
        }

        var folded = Fold(key);
        string? nearest = null;
        foreach (var candidate in _read)
        {
            var candidateFolded = Fold(candidate);
            if (folded == candidateFolded)
            {
                return candidate;
            }

            if (nearest is null && WithinOneEdit(folded, candidateFolded))
            {
                nearest = candidate;
            }
        }

        return nearest;
    }

    private static string Fold(string key) =>
        string.Concat(key.Where(c => c is not ('_' or '-' or ' '))).ToLowerInvariant();

    /// <summary>True when one inserted, deleted or substituted character turns <paramref name="a"/> into <paramref name="b"/> (or they are equal).</summary>
    private static bool WithinOneEdit(string a, string b)
    {
        if (Math.Abs(a.Length - b.Length) > 1)
        {
            return false;
        }

        var i = 0;
        while (i < a.Length && i < b.Length && a[i] == b[i])
        {
            i++;
        }

        if (i == a.Length && i == b.Length)
        {
            return true;
        }

        if (a.Length == b.Length)
        {
            return a.AsSpan(i + 1).SequenceEqual(b.AsSpan(i + 1));
        }

        var (shorter, longer) = a.Length < b.Length ? (a, b) : (b, a);
        return shorter.AsSpan(i).SequenceEqual(longer.AsSpan(i + 1));
    }

    /// <summary>The list as the verb prints it: <c>name (did you mean display_name?), foo</c>.</summary>
    internal static string Describe(IEnumerable<(string Key, string? DidYouMean)> keys) =>
        string.Join(", ", keys.Select(k => k.DidYouMean is null ? k.Key : $"{k.Key} (did you mean {k.DidYouMean}?)"));

    /// <summary>The same list from the keys a parse found.</summary>
    internal static string Describe(IEnumerable<UnreadServerKey> keys) => Describe(keys.Select(k => (k.Key, k.DidYouMean)));
}
