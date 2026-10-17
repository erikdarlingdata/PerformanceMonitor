/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>What one map pass did: whether it rebuilt, how many keys it resolved and how many of them got a tag.</summary>
public sealed record QueryTagMapPass(bool Rebuilt, int KeysResolved, int KeysTagged);

public partial class LocalDataService
{
    /// <summary>
    /// #5990: the tag map pass. Reads the tags, hand assignments, rules and sightings and gives every sighted key its one
    /// tag with the shared <see cref="QueryTagResolver"/> (Lite has no matching of its own). When <c>rules_version</c> is
    /// ahead of <c>built_version</c> the pass REBUILDS: it replaces every map row in one transaction and sets
    /// <c>built_version</c> to the version it built from, so a rule written meanwhile makes the next pass rebuild again.
    /// Otherwise it EXTENDS: it resolves only the keys whose sighting was last seen at or after
    /// <paramref name="changedSince"/> (null = every key) and upserts or removes their map rows. Each server resolves at
    /// most <see cref="QueryTagLimits.MaxKeysPerServerPerPass"/> keys, the most recently seen first.
    /// A key that resolves to no tag has no map row (it reads as untagged). The write takes effect in this pass, never
    /// inside the rule or assignment write.
    /// </summary>
    public async Task<QueryTagMapPass> BuildQueryTagMapAsync(DateTime? changedSince = null)
    {
        using var connection = await OpenWriteConnectionAsync();

        long rulesVersion = 1;
        long builtVersion = 0;
        using (var state = connection.CreateCommand())
        {
            state.CommandText = "SELECT rules_version, built_version FROM query_tag_state WHERE id = 1";
            using var reader = await state.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                rulesVersion = reader.GetInt64(0);
                builtVersion = reader.GetInt64(1);
            }
        }

        var rebuild = rulesVersion > builtVersion;
        var since = rebuild ? null : changedSince;

        var tags = await ReadQueryTagsAsync(connection);
        var tagIds = tags.Select(t => (long)t.Id).ToHashSet();
        var names = new TagNameList(tags.Select(t => new KeyValuePair<string, long>(t.Name, t.Id)));
        var assignments = (await GetQueryTagAssignmentsAsync()).Select(ToHandAssignment).ToList();
        var rules = (await GetQueryTagRulesAsync()).Select(ToRule).ToList();
        var sightings = await ReadQuerySightingsAsync(connection, since);

        var resolved = new List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagResolution Result)>();
        var removed = new List<(int ServerId, string Kind, string Key)>();
        var byKey = sightings.GroupBy(x => (x.ServerId, x.Kind, x.Key)).ToList();
        foreach (var group in byKey)
        {
            var (serverId, kind, key) = group.Key;
            var result = QueryTagResolver.Resolve(serverId, group.Select(g => g.Sighting).ToList(), assignments, rules, names);
            if (result.HasTag && tagIds.Contains(result.TagId!.Value))
            {
                resolved.Add((serverId, kind, key, result));
            }
            else
            {
                removed.Add((serverId, KeyKindText(kind), key));
            }
        }

        using (var begin = connection.CreateCommand())
        {
            begin.CommandText = "BEGIN TRANSACTION";
            await begin.ExecuteNonQueryAsync();
        }

        try
        {
            if (rebuild)
            {
                using var clear = connection.CreateCommand();
                clear.CommandText = "DELETE FROM query_tag_map";
                await clear.ExecuteNonQueryAsync();
            }
            else
            {
                await DeleteMapRowsAsync(connection, removed);
            }

            await UpsertMapRowsAsync(connection, resolved);

            if (rebuild)
            {
                using var built = connection.CreateCommand();
                built.CommandText = @"
INSERT INTO query_tag_state (id, rules_version, built_version, updated_at)
VALUES (1, $1, $1, $2)
ON CONFLICT (id) DO UPDATE SET built_version = excluded.built_version";
                built.Parameters.Add(new DuckDBParameter { Value = rulesVersion });
                built.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
                await built.ExecuteNonQueryAsync();
            }

            using var commit = connection.CreateCommand();
            commit.CommandText = "COMMIT";
            await commit.ExecuteNonQueryAsync();
        }
        catch
        {
            try
            {
                using var rollback = connection.CreateCommand();
                rollback.CommandText = "ROLLBACK";
                await rollback.ExecuteNonQueryAsync();
            }
            catch (Exception)
            {
                /* the original failure is the one to report */
            }

            throw;
        }

        return new QueryTagMapPass(rebuild, byKey.Count, resolved.Count);
    }

    private static async Task<List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagSighting Sighting)>> ReadQuerySightingsAsync(
        LockedConnection connection, DateTime? since)
    {
        using var command = connection.CreateCommand();
        /* Per server, the newest keys first, up to the shared cap. */
        command.CommandText = @"
SELECT server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY server_id ORDER BY last_seen DESC, key, application_name, login_name, host_name) AS rn
    FROM query_tag_sightings
    WHERE $1 IS NULL OR (server_id, key_kind, key) IN
        (SELECT server_id, key_kind, key FROM query_tag_sightings WHERE last_seen >= $1)
)
WHERE rn <= $2";
        command.Parameters.Add(new DuckDBParameter { Value = (object?)since ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = QueryTagLimits.MaxKeysPerServerPerPass });

        var rows = new List<(int, QueryTagKeyKind, string, QueryTagSighting)>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            string? Text(int i) => reader.IsDBNull(i) || reader.GetString(i).Length == 0 ? null : reader.GetString(i);
            var serverId = reader.GetInt32(0);
            var kind = reader.GetString(1) switch
            {
                "hash" => QueryTagKeyKind.Hash,
                "object" => QueryTagKeyKind.Object,
                _ => QueryTagKeyKind.Caller,
            };
            var key = reader.GetString(2);
            rows.Add((serverId, kind, key, new QueryTagSighting(
                kind, key, Text(3), ParseObject(Text(4)), Text(5), Text(6), Text(7), Text(8))));
        }

        return rows;
    }

    /// <summary>Splits <c>database.schema.name</c> (<see cref="QueryTagObject.Qualified"/>) back into an object. The name is
    /// everything after the second dot, so a dot inside a name survives; a dot inside a database or schema name does not.</summary>
    private static QueryTagObject? ParseObject(string? qualified)
    {
        if (string.IsNullOrEmpty(qualified))
        {
            return null;
        }

        var first = qualified.IndexOf('.', StringComparison.Ordinal);
        var second = first < 0 ? -1 : qualified.IndexOf('.', first + 1);
        if (second < 0)
        {
            return new QueryTagObject(null, null, qualified);
        }

        static string? Part(string s) => s.Length == 0 ? null : s;
        return new QueryTagObject(Part(qualified[..first]), Part(qualified[(first + 1)..second]), Part(qualified[(second + 1)..]));
    }

    private static QueryTagHandAssignment ToHandAssignment(QueryTagAssignmentRow a) =>
        a.Kind == "hash"
            ? new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Hash, QueryHashIdentity.CanonicalKey(a.QueryHash) ?? a.QueryHash, null, a.ServerId)
            : new QueryTagHandAssignment(a.TagId, QueryTagKeyKind.Object, null,
                new QueryTagObject(a.DatabaseName, a.SchemaName, a.ObjectName), a.ServerId);

    private static QueryTagRule ToRule(QueryTagRuleRow r) =>
        new(r.Id, r.MatchField switch
        {
            "application" => QueryTagRuleField.Application,
            "login" => QueryTagRuleField.Login,
            "host" => QueryTagRuleField.Host,
            "database" => QueryTagRuleField.Database,
            "object" => QueryTagRuleField.Object,
            "text" => QueryTagRuleField.Text,
            _ => QueryTagRuleField.TextTag,
        }, r.Pattern, r.Priority, r.Enabled, r.TagId, r.ServerId);

    private static string KeyKindText(QueryTagKeyKind kind) => kind switch
    {
        QueryTagKeyKind.Hash => "hash",
        QueryTagKeyKind.Object => "object",
        _ => "caller",
    };

    private static async Task DeleteMapRowsAsync(LockedConnection connection, List<(int ServerId, string Kind, string Key)> keys)
    {
        if (keys.Count == 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = @"
DELETE FROM query_tag_map
WHERE (server_id, key_kind, key) IN (SELECT UNNEST($1), UNNEST($2), UNNEST($3))";
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.ServerId).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.Kind).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = keys.Select(k => k.Key).ToList() });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task UpsertMapRowsAsync(
        LockedConnection connection, List<(int ServerId, QueryTagKeyKind Kind, string Key, QueryTagResolution Result)> rows)
    {
        if (rows.Count == 0)
        {
            return;
        }

        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_tag_map (server_id, key_kind, key, tag_id, source, rule_id, matched_field, matched_value, built_at)
SELECT s, k, c_key, t, src, NULLIF(r, 0), NULLIF(f, ''), NULLIF(v, ''), $9
FROM (SELECT UNNEST($1) AS s, UNNEST($2) AS k, UNNEST($3) AS c_key, UNNEST($4) AS t, UNNEST($5) AS src,
             UNNEST($6) AS r, UNNEST($7) AS f, UNNEST($8) AS v)
ON CONFLICT (server_id, key_kind, key) DO UPDATE SET
    tag_id = excluded.tag_id, source = excluded.source, rule_id = excluded.rule_id,
    matched_field = excluded.matched_field, matched_value = excluded.matched_value, built_at = excluded.built_at";
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.ServerId).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => KeyKindText(r.Kind)).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Key).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => (int)r.Result.TagId!.Value).ToList() });
        command.Parameters.Add(new DuckDBParameter
        {
            Value = rows.Select(r => r.Result.Source switch
            {
                QueryTagSource.HandHash => "hand_hash",
                QueryTagSource.HandObject => "hand_object",
                _ => "rule",
            }).ToList(),
        });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => (int)(r.Result.RuleId ?? 0)).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Result.MatchedField is { } f ? QueryTagFieldText(f) : string.Empty).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = rows.Select(r => r.Result.MatchedValue ?? string.Empty).ToList() });
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>The tag names as a list of pairs, so two tags with the same name under different parents both reach the
    /// resolver, which then treats the name as ambiguous. A <see cref="Dictionary{TKey,TValue}"/> would drop one.</summary>
    private sealed class TagNameList(IEnumerable<KeyValuePair<string, long>> pairs) : IReadOnlyDictionary<string, long>
    {
        private readonly List<KeyValuePair<string, long>> _pairs = pairs.ToList();

        public long this[string key] => _pairs.First(p => string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase)).Value;
        public IEnumerable<string> Keys => _pairs.Select(p => p.Key);
        public IEnumerable<long> Values => _pairs.Select(p => p.Value);
        public int Count => _pairs.Count;
        public bool ContainsKey(string key) => _pairs.Any(p => string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase));

        public bool TryGetValue(string key, out long value)
        {
            foreach (var p in _pairs)
            {
                if (string.Equals(p.Key, key, StringComparison.OrdinalIgnoreCase))
                {
                    value = p.Value;
                    return true;
                }
            }

            value = 0;
            return false;
        }

        public IEnumerator<KeyValuePair<string, long>> GetEnumerator() => _pairs.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }
}
