/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What a query read's <c>tag</c> parameter resolved to, plus the server's tag map (#5992 item 3, the twin of Darling's
/// <c>QueryTagReadJoin</c>, #5984). The map is the materialised <c>query_tag_map</c>: nothing here evaluates a rule. A row's tag is its
/// path name (<c>Parent / Child</c>) or null. A filter is a semi-join on the map inside the read's own statement, so it applies before the top-N cut.
/// </summary>
public sealed class QueryTagReadScope
{
    /// <summary>The word that matches rows with no tag (when no tag is itself named so).</summary>
    public const string UntaggedWord = "untagged";

    /// <summary>The separator between a tag and its parent in a path name.</summary>
    public const string PathSeparator = " / ";

    /// <summary>The description every query read gives its optional <c>tag</c> parameter.</summary>
    public const string ParameterDescription =
        "Query tag name or path ('Parent / Child'), any case. Keeps rows with that tag or any tag beneath it. 'untagged' keeps rows with no tag. Applied before the top-N cut.";

    private readonly Dictionary<string, int> _hash;
    private readonly Dictionary<string, int> _object;
    private readonly Dictionary<int, string> _paths;
    private readonly HashSet<int> _wanted;

    internal QueryTagReadScope(
        Dictionary<string, int> hash, Dictionary<string, int> objects, Dictionary<int, string> paths,
        HashSet<int> wanted, bool untagged, bool unknown, string? requested)
    {
        _hash = hash;
        _object = objects;
        _paths = paths;
        _wanted = wanted;
        Untagged = untagged;
        Unknown = unknown;
        Requested = requested;
    }

    /// <summary>The word the caller asked for, or null.</summary>
    public string? Requested { get; }

    /// <summary>True when the caller named a tag nobody carries.</summary>
    public bool Unknown { get; }

    /// <summary>The tag that shaped the read: the name as the caller gave it, or <c>untagged</c>. Null when no tag shaped it.</summary>
    public string? Named => !Active ? null : Untagged ? UntaggedWord : Requested;

    /// <summary>True when the filter is the word <c>untagged</c>.</summary>
    public bool Untagged { get; }

    /// <summary>True when the read must narrow its population.</summary>
    public bool Active => Untagged || _wanted.Count > 0;

    /// <summary>The tag of a query hash (any stored form), as its path name, or null.</summary>
    public string? TagOfHash(string? hash)
    {
        var key = QueryHashIdentity.CanonicalKey(hash);
        return key is not null && _hash.TryGetValue(key, out var id) && _paths.TryGetValue(id, out var path) ? path : null;
    }

    /// <summary>The tag of a database, schema, object name, as its path name, or null.</summary>
    public string? TagOfObject(string? database, string? schema, string? name)
    {
        if (string.IsNullOrEmpty(name))
        {
            return null;
        }

        return _object.TryGetValue(new QueryTagObject(database, schema, name).Qualified, out var id) && _paths.TryGetValue(id, out var path) ? path : null;
    }

    /// <summary>The hash token a statement carries at the place its database filter sits: the filter's column holds the stored hash, in the 0x or the decimal form.</summary>
    public static string HashToken(string hashColumn) => $"/*TAG:hash:{hashColumn}*/";

    /// <summary>The object token: the three columns that build the qualified <c>database.schema.name</c>.</summary>
    public static string ObjectToken(string databaseColumn, string schemaColumn, string nameColumn) => $"/*TAG:object:{databaseColumn},{schemaColumn},{nameColumn}*/";

    private static readonly System.Text.RegularExpressions.Regex TokenPattern =
        new(@"/\*TAG:(?<kind>hash|object):(?<cols>[^*]*)\*/", System.Text.RegularExpressions.RegexOptions.CultureInvariant);

    /// <summary>The tag ids a filtered read binds, in order, from the parameter number <see cref="Apply"/> was given. Empty when the filter is <c>untagged</c> or none.</summary>
    public IReadOnlyList<object> ParameterValues => Untagged ? [] : _wanted.OrderBy(i => i).Select(i => (object)i).ToList();

    /// <summary>
    /// Replaces each token of <paramref name="sql"/> (<see cref="HashToken"/>, <see cref="ObjectToken"/>) with an <c>AND EXISTS</c> (or
    /// <c>AND NOT EXISTS</c> for <c>untagged</c>) semi-join on the server's <c>query_tag_map</c> rows, which is <c>$1</c>. The tag ids are the
    /// <see cref="ParameterValues"/> the caller adds after its own parameters, from <paramref name="firstParameter"/>. A hash is put in
    /// the map's canonical form in SQL and compared by exact text: nothing folds case, and both sides carry the collected text. With no filter the
    /// text is returned as it came. A requested tag on a statement with no token would serve unfiltered rows under the tag the caller
    /// asked for, so it throws <see cref="InvalidOperationException"/> naming the statement (the twin of Darling's <c>QueryTagReadJoin.Apply</c>).
    /// </summary>
    public string Apply(string sql, int firstParameter)
    {
        if (!Active)
        {
            return sql;
        }

        if (!sql.Contains("/*TAG:", StringComparison.Ordinal))
        {
            var head = sql.TrimStart();
            throw new InvalidOperationException(
                "A query tag was requested but the statement carries no /*TAG:...*/ token to apply it at: "
                + (head.Length > 80 ? head[..80] + "..." : head));
        }

        return TokenPattern.Replace(sql, match =>
        {
            var cols = match.Groups["cols"].Value.Split(',', StringSplitOptions.TrimEntries);
            string kind;
            string expression;
            if (match.Groups["kind"].Value == "object")
            {
                kind = "object";
                expression = $"(COALESCE({cols[0]}, '') || '.' || COALESCE({cols[1]}, '') || '.' || COALESCE({cols[2]}, ''))";
            }
            else
            {
                kind = "hash";
                var trimmed = $"trim({cols[0]})";
                expression = $"(CASE WHEN {trimmed} LIKE '0x%' THEN {trimmed} "
                    + $"WHEN TRY_CAST({trimmed} AS UBIGINT) IS NOT NULL THEN '0x' || upper(lpad(to_hex(TRY_CAST({trimmed} AS UBIGINT)), 16, '0')) "
                    + $"ELSE {trimmed} END)";
            }

            var map = $"SELECT 1 FROM query_tag_map m WHERE m.server_id = $1 AND m.key_kind = '{kind}' AND m.key = {expression}";
            if (Untagged)
            {
                return $" AND NOT EXISTS ({map})";
            }

            var marks = Enumerable.Range(0, _wanted.Count).Select(i => "$" + (firstParameter + i));
            return $" AND EXISTS ({map} AND m.tag_id IN ({string.Join(", ", marks)}))";
        });
    }

    /// <summary>The message a read answers with when its <c>tag</c> names no tag.</summary>
    public static string UnknownTagMessage(string requested) =>
        $"No query tag is named '{requested}', so no row can match it. Use the tag's name or its path (Parent / Child); the word 'untagged' matches rows with no tag.";
}

public partial class LocalDataService
{
    /// <summary>
    /// Loads the server's tag map and tag tree once and resolves <paramref name="tag"/> (blank is no filter): a name or path, any case,
    /// matches every tag so named with every tag beneath it. The word <c>untagged</c> matches rows with no tag unless a tag is named so.
    /// </summary>
    public async Task<QueryTagReadScope> GetQueryTagReadScopeAsync(int serverId, string? tag)
    {
        var tags = await GetQueryTagsAsync();
        var byId = tags.ToDictionary(t => t.Id);
        var paths = new Dictionary<int, string>();
        foreach (var node in tags)
        {
            var parts = new List<string> { node.Name };
            var parent = node.ParentId;
            for (var steps = 0; parent is int pid && byId.TryGetValue(pid, out var up) && steps < TagTreeRules.MaxDepth + 2; steps++)
            {
                parts.Add(up.Name);
                parent = up.ParentId;
            }

            parts.Reverse();
            paths[node.Id] = string.Join(QueryTagReadScope.PathSeparator, parts);
        }

        var hash = new Dictionary<string, int>(StringComparer.Ordinal);
        var objects = new Dictionary<string, int>(StringComparer.Ordinal);
        using (var connection = await OpenConnectionAsync())
        {
            await ReadRowsAsync(connection, "SELECT key_kind, key, tag_id FROM query_tag_map WHERE server_id = $1 AND key_kind IN ('hash', 'object')", [serverId],
                r => (r.GetString(0) == "hash" ? hash : objects)[r.GetString(1)] = r.GetInt32(2));
        }

        var wanted = new HashSet<int>();
        if (string.IsNullOrWhiteSpace(tag))
        {
            return new QueryTagReadScope(hash, objects, paths, wanted, false, false, null);
        }

        var requested = tag.Trim();
        var normal = string.Join(QueryTagReadScope.PathSeparator, requested.Split('/').Select(p => p.Trim()));
        var nodes = tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();
        var roots = tags.Where(t => string.Equals(t.Name, requested, StringComparison.OrdinalIgnoreCase)
            || string.Equals(paths[t.Id], normal, StringComparison.OrdinalIgnoreCase)).Select(t => t.Id).ToList();
        if (roots.Count == 0 && int.TryParse(requested, NumberStyles.None, CultureInfo.InvariantCulture, out var asId) && byId.ContainsKey(asId))
        {
            roots.Add(asId);
        }

        if (roots.Count == 0)
        {
            var untagged = string.Equals(requested, QueryTagReadScope.UntaggedWord, StringComparison.OrdinalIgnoreCase);
            return new QueryTagReadScope(hash, objects, paths, wanted, untagged, !untagged, requested);
        }

        foreach (var root in roots)
        {
            wanted.Add(root);
            wanted.UnionWith(TagTreeRules.Descendants(nodes, root));
        }

        return new QueryTagReadScope(hash, objects, paths, wanted, false, false, requested);
    }
}
