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
using System.Text;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What a query read's <c>tag</c> parameter resolved to, plus the server's tag map (#5992 item 3, the twin of Darling's
/// <c>QueryTagReadJoin</c>, #5984). The map is the materialised <c>query_tag_map</c>: nothing here evaluates a rule. A row's tag is its
/// path name (<c>Parent / Child</c>) or null. A filter is a set of keys written into the statement, so it applies before the top-N cut.
/// </summary>
public sealed class QueryTagReadScope
{
    /// <summary>The word that matches rows with no tag (when no tag is itself named so).</summary>
    public const string UntaggedWord = "untagged";

    /// <summary>The separator between a tag and its parent in a path name.</summary>
    public const string PathSeparator = " / ";

    /// <summary>The description every query read gives its optional <c>tag</c> parameter.</summary>
    public const string ParameterDescription =
        "Keep rows whose query has this tag or one beneath it (name or Parent / Child path), applied before the row cap. 'untagged' keeps untagged rows. Each row carries its tag.";

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

    /// <summary>
    /// An <c>AND ...</c> predicate over <paramref name="hashExpression"/> (a column holding the stored hash), or empty when there is no
    /// filter. A key is listed in its stored <c>0x</c> form and in the decimal form the long-query trace stores.
    /// </summary>
    public string HashClause(string hashExpression) => Clause(hashExpression, _hash, includeDecimal: true);

    /// <summary>The same over the qualified object name built from the three columns.</summary>
    public string ObjectClause(string databaseColumn, string schemaColumn, string nameColumn) =>
        Clause($"(COALESCE({databaseColumn}, '') || '.' || COALESCE({schemaColumn}, '') || '.' || COALESCE({nameColumn}, ''))", _object, includeDecimal: false);

    private string Clause(string expression, Dictionary<string, int> map, bool includeDecimal)
    {
        if (!Active)
        {
            return string.Empty;
        }

        var keys = new List<string>();
        foreach (var (key, tag) in map)
        {
            if (Untagged || _wanted.Contains(tag))
            {
                keys.Add(key);
                if (includeDecimal && key.Length == 18 && key.StartsWith("0x", StringComparison.OrdinalIgnoreCase)
                    && ulong.TryParse(key.AsSpan(2), NumberStyles.HexNumber, CultureInfo.InvariantCulture, out var value))
                {
                    keys.Add(value.ToString(CultureInfo.InvariantCulture));
                }
            }
        }

        if (keys.Count == 0)
        {
            /* A tag nothing is mapped to matches nothing; with the word untagged and an empty map every row matches. */
            return Untagged ? string.Empty : " AND 1 = 0";
        }

        var list = new StringBuilder();
        foreach (var key in keys)
        {
            list.Append(list.Length == 0 ? "" : ", ").Append('\'').Append(key.Replace("'", "''", StringComparison.Ordinal)).Append('\'');
        }

        return Untagged
            ? $" AND ({expression} IS NULL OR {expression} NOT IN ({list}))"
            : $" AND {expression} IN ({list})";
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
