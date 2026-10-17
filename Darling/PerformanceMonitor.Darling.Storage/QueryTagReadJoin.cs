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
using System.Text;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// What a read's <c>tag</c> parameter resolved to (#5984): no filter, a set of tag ids (the named tag and every tag beneath it),
/// the word <c>untagged</c>, or a name no tag carries.
/// </summary>
public sealed record QueryTagFilter(IReadOnlyList<int> TagIds, bool Untagged, bool Unknown, string? Requested)
{
    /// <summary>No <c>tag</c> parameter: the read is unchanged.</summary>
    public static QueryTagFilter None { get; } = new(Array.Empty<int>(), false, false, null);

    /// <summary>True when the read must narrow its population.</summary>
    public bool Active => Untagged || TagIds.Count > 0;
}

/// <summary>
/// The one join the Darling query reads use to carry a query tag and to filter by it (#5984, wave 2 of #5971). The tag of a row
/// is read from <c>config.query_tag_map</c> by (server, key kind, key): <c>query_hash</c> for a <c>hash</c> row, the qualified
/// <c>database.schema.name</c> for an <c>object</c> row. The map holds at most one tag per key (the builder's precedence,
/// #5985), so a row has at most one tag and an unmapped key has none. A read never builds the map and never writes it.
///
/// <para><b>The filter is in the statement, before the cap.</b> A SQL text carries the token
/// <c>/*TAG:hash:query_hash*/</c> (or <c>/*TAG:object:database_name,schema_name,object_name*/</c>) as a comment at the place its
/// database filter sits; <see cref="Apply"/> turns it into an <c>EXISTS</c> (or <c>NOT EXISTS</c> for <c>untagged</c>) test
/// against the map, so a filtered read returns that tag's top rows, not the tag's share of the unfiltered top rows. With no filter
/// the text is returned unchanged (the tokens are comments). The ids are integers read from the store and written in
/// invariant form; no caller text reaches the statement.</para>
///
/// <para><b>The row's tag is a second, page-sized read</b> (<see cref="LookupAsync"/>), a left join in effect: a key with no map row
/// has no entry and the row simply carries no <c>tag</c> key.</para>
/// </summary>
public static class QueryTagReadJoin
{
    /// <summary>The word that matches rows with no tag (when no tag is itself named so).</summary>
    public const string UntaggedWord = "untagged";

    /// <summary>The page lookup: an exact-key probe of the map's primary key (<c>server_id, key_kind, key</c>), with no function on the key.</summary>
    internal const string LookupSql = "SELECT key, tag_id FROM config.query_tag_map WHERE server_id = $1 AND key_kind = $2 AND key = ANY($3)";

    /// <summary>The description every query read gives its optional <c>tag</c> parameter.</summary>
    public const string TagParameterDescription =
        "Query tag name or path ('Parent / Child'), any case. Keeps rows with that tag or any tag beneath it. 'untagged' keeps rows with no tag. Applied before the top-N cut.";

    /// <summary>The separator between a tag and its parent in a path name.</summary>
    public const string PathSeparator = " / ";

    /// <summary>The key kind of a query_hash row in <c>config.query_tag_map</c>.</summary>
    public const string HashKind = "hash";

    /// <summary>The key kind of a procedure (database, schema, object) row in <c>config.query_tag_map</c>.</summary>
    public const string ObjectKind = "object";

    private const int MaxWalkSteps = 64;

    private static readonly Regex TokenPattern = new(
        @"/\*TAG:(?<kind>hash|object):(?<cols>[^*]*)\*/", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private const string TagsSql = "SELECT id, name, parent_id FROM config.query_tags ORDER BY id";

    private sealed record Node(int Id, string Name, int? ParentId);

    /// <summary>The map key of an object row: <c>database.schema.name</c>, a missing part an empty string.</summary>
    public static string ObjectKey(string? database, string? schema, string? name) =>
        (database ?? string.Empty) + "." + (schema ?? string.Empty) + "." + (name ?? string.Empty);

    private static async Task<List<Node>> ReadTagsAsync(NpgsqlDataSource postgres, CancellationToken cancellationToken)
    {
        var nodes = new List<Node>();
        await using var command = postgres.CreateCommand(TagsSql);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            nodes.Add(new Node(reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetInt32(2)));
        }

        return nodes;
    }

    private static Dictionary<int, string> PathsOf(List<Node> nodes)
    {
        var byId = nodes.ToDictionary(n => n.Id);
        var paths = new Dictionary<int, string>();
        foreach (var node in nodes)
        {
            var parts = new List<string> { node.Name };
            var parent = node.ParentId;
            var steps = 0;
            while (parent is int pid && byId.TryGetValue(pid, out var up) && steps++ < MaxWalkSteps)
            {
                parts.Add(up.Name);
                parent = up.ParentId;
            }

            parts.Reverse();
            paths[node.Id] = string.Join(PathSeparator, parts);
        }

        return paths;
    }

    /// <summary>Collapses the spacing around slashes so <c>Prod/Orders</c> and <c>Prod / Orders</c> name one path.</summary>
    private static string NormalisePath(string text) =>
        string.Join(PathSeparator, text.Split('/').Select(p => p.Trim()));

    /// <summary>
    /// Resolves a read's <c>tag</c> argument. Blank is no filter. A name matches every tag so named (a name can repeat under
    /// different parents) or a tag whose path name (<c>Parent / Child</c>) equals it, each with every tag beneath it, ignoring case.
    /// The word <c>untagged</c> matches rows with no tag unless a tag is named so. A name nobody carries is <c>Unknown</c>.
    /// </summary>
    public static async Task<QueryTagFilter> ResolveAsync(NpgsqlDataSource postgres, string? tag, CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(tag))
        {
            return QueryTagFilter.None;
        }

        var requested = tag.Trim();
        var nodes = await ReadTagsAsync(postgres, cancellationToken);
        var paths = PathsOf(nodes);
        var path = NormalisePath(requested);
        var roots = nodes
            .Where(n => string.Equals(n.Name, requested, StringComparison.OrdinalIgnoreCase)
                || string.Equals(paths[n.Id], path, StringComparison.OrdinalIgnoreCase))
            .Select(n => n.Id)
            .ToHashSet();
        if (roots.Count == 0)
        {
            return string.Equals(requested, UntaggedWord, StringComparison.OrdinalIgnoreCase)
                ? new QueryTagFilter(Array.Empty<int>(), true, false, requested)
                : new QueryTagFilter(Array.Empty<int>(), false, true, requested);
        }

        var ids = new HashSet<int>(roots);
        var frontier = roots.ToList();
        for (var step = 0; step < MaxWalkSteps && frontier.Count > 0; step++)
        {
            var next = nodes.Where(n => n.ParentId is int p && frontier.Contains(p) && ids.Add(n.Id)).Select(n => n.Id).ToList();
            frontier = next;
        }

        return new QueryTagFilter(ids.OrderBy(i => i).ToList(), false, false, requested);
    }

    /// <summary>
    /// Replaces each <c>/*TAG:...*/</c> token of <paramref name="sql"/>: with an active filter, an <c>AND (EXISTS ...)</c> or
    /// <c>AND (NOT EXISTS ...)</c> test against the server's rows of <c>config.query_tag_map</c>; otherwise nothing.
    /// </summary>
    public static string Apply(string sql, int serverId, QueryTagFilter? filter)
    {
        /* With no active filter the text is returned as it came, so a read without a tag sends the very statement it always sent
           (the tokens are comments), and the statement hashes the diagnostics and tests key on do not move. */
        if (filter is null || !filter.Active || !sql.Contains("/*TAG:", StringComparison.Ordinal))
        {
            return sql;
        }

        return TokenPattern.Replace(sql, match =>
        {
            var cols = match.Groups["cols"].Value.Split(',', StringSplitOptions.TrimEntries);
            var isObject = match.Groups["kind"].Value == ObjectKind;
            var kind = isObject ? ObjectKind : HashKind;
            var join = isObject
                ? $"m.key = COALESCE({cols[0]}, '') || '.' || COALESCE({cols[1]}, '') || '.' || COALESCE({cols[2]}, '')"
                : $"m.key = {cols[0]}";
            var scope = string.Create(CultureInfo.InvariantCulture,
                $"FROM config.query_tag_map AS m WHERE m.server_id = {serverId} AND m.key_kind = '{kind}' AND {join}");
            if (filter.Untagged)
            {
                return $"AND NOT EXISTS (SELECT 1 {scope})";
            }

            var ids = string.Join(",", filter.TagIds.Select(i => i.ToString(CultureInfo.InvariantCulture)));
            return $"AND EXISTS (SELECT 1 {scope} AND m.tag_id IN ({ids}))";
        });
    }

    /// <summary>
    /// The tag of each key, as its path name (<c>Parent / Child</c>), for a page of rows. A key with no map row is not in the
    /// result. Every key compares exactly (#5984): the map's primary key is (server_id, key_kind, key), so a lower() on either side would
    /// defeat it. Case is folded once, when the map is built (#5985 writes the sighted key text), never at read time.
    /// </summary>
    public static async Task<Dictionary<string, string>> LookupAsync(
        NpgsqlDataSource postgres, int serverId, string keyKind, IEnumerable<string?> keys, CancellationToken cancellationToken)
    {
        var wanted = keys.Where(k => !string.IsNullOrEmpty(k)).Select(k => k!).Distinct(StringComparer.Ordinal).ToArray();
        var found = new Dictionary<string, string>(StringComparer.Ordinal);
        if (wanted.Length == 0)
        {
            return found;
        }

        var tagByKey = new Dictionary<string, int>(StringComparer.Ordinal);
        await using (var command = postgres.CreateCommand(LookupSql))
        {
            command.Parameters.Add(new NpgsqlParameter { Value = serverId, NpgsqlDbType = NpgsqlDbType.Integer });
            command.Parameters.Add(new NpgsqlParameter { Value = keyKind, NpgsqlDbType = NpgsqlDbType.Text });
            command.Parameters.Add(new NpgsqlParameter { Value = wanted, NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                tagByKey[reader.GetString(0)] = reader.GetInt32(1);
            }
        }

        if (tagByKey.Count == 0)
        {
            return found;
        }

        var paths = PathsOf(await ReadTagsAsync(postgres, cancellationToken));
        foreach (var (key, tagId) in tagByKey)
        {
            if (paths.TryGetValue(tagId, out var path))
            {
                found[key] = path;
            }
        }

        return found;
    }

    /// <summary>
    /// Puts <c>tag</c> (the path name) on each row of a page whose key has a map row, and leaves every other row exactly as it was: an
    /// unmapped key has no <c>tag</c> member at all. <paramref name="keyOf"/> reads the row's key (the stored <c>query_hash</c>, or the
    /// qualified object name).
    /// </summary>
    public static async Task AttachAsync(
        NpgsqlDataSource postgres, int serverId, string keyKind, IEnumerable<JsonObject> rows, Func<JsonObject, string?> keyOf,
        CancellationToken cancellationToken)
    {
        var page = rows.ToList();
        await AttachByKeysAsync(postgres, serverId, keyKind, page, page.Select(keyOf).ToList(), cancellationToken);
    }

    /// <summary>The same, for a page whose keys are known beside the rows (<paramref name="keys"/>[i] is the key of <paramref name="rows"/>[i]).</summary>
    public static async Task AttachByKeysAsync(
        NpgsqlDataSource postgres, int serverId, string keyKind, IReadOnlyList<JsonObject> rows, IReadOnlyList<string?> keys,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        var tags = await LookupAsync(postgres, serverId, keyKind, keys, cancellationToken);
        for (var i = 0; i < rows.Count && i < keys.Count; i++)
        {
            var key = keys[i];
            if (!string.IsNullOrEmpty(key) && tags.TryGetValue(key, out var path))
            {
                rows[i]["tag"] = path;
            }
        }
    }

    /// <summary>The status message a read answers with when its <c>tag</c> names no tag.</summary>
    public static string UnknownTagMessage(string requested) =>
        new StringBuilder("No query tag is named '").Append(requested)
            .Append("', so no row can match it. Use the tag's name or its path (Parent / Child); the word 'untagged' matches rows with no tag.").ToString();
}
