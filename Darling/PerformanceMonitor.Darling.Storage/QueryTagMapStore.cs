/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One row of <c>config.query_tag_map</c>: the one tag a key resolved to, and why.</summary>
public sealed record QueryTagMapRow(
    string KeyKind,
    string Key,
    int TagId,
    string Source,
    int? RuleId,
    string? MatchedField,
    string? MatchedValue);

/// <summary>A key whose sightings changed, with the newest <c>last_seen</c> among its sighting rows.</summary>
public sealed record QueryTagChangedKey(string KeyKind, string Key, DateTime LastSeen);

/// <summary>Everything the resolver needs, read in one repeatable-read snapshot so the version matches the rules.</summary>
public sealed record QueryTagMapSnapshot(
    long RulesVersion,
    long BuiltVersion,
    IReadOnlyList<QueryTagRule> Rules,
    IReadOnlyList<QueryTagHandAssignment> Assignments,
    IReadOnlyDictionary<string, long> TagIdsByName);

/// <summary>
/// The SQL behind <see cref="QueryTagMapBuilder"/> (#5985, part of #5971): reading the rules, the sightings and the
/// version state, and writing <c>config.query_tag_map</c>. It evaluates no rule; the resolver does.
/// </summary>
public static class QueryTagMapStore
{
    public const int CommandTimeoutSeconds = 60;

    /// <summary>The version and the tag names, read together with the rules and assignments in one snapshot.</summary>
    internal const string ReadStateSql = "SELECT rules_version, built_version FROM config.query_tag_state WHERE id = 1";

    internal const string ReadTagsSql = "SELECT id, name FROM config.query_tags";

    internal const string ReadRulesSql =
        "SELECT id, match_field, pattern, priority, enabled, tag_id, server_id FROM config.query_tag_rules WHERE enabled";

    internal const string ReadAssignmentsSql =
        "SELECT tag_id, kind, server_id, query_hash, database_name, schema_name, object_name FROM config.query_tag_assignments";

    /// <summary>
    /// The keys of one server whose sightings changed after the cursor, oldest first, at most $5. $1 server_id,
    /// $2 the cursor's time, $3 and $4 the cursor's kind and key (the tie-break among keys with the same time: one
    /// batch stamps every key it touches with one <c>now()</c>, so a time alone could not step past a full page).
    /// </summary>
    internal const string ChangedKeysSql = """
SELECT key_kind, key, last_seen
FROM (
    SELECT key_kind, key, max(last_seen) AS last_seen
    FROM collect.query_tag_sightings
    WHERE server_id = $1 AND last_seen >= $2
    GROUP BY key_kind, key
) AS s
WHERE (last_seen, key_kind, key) > ($2, $3, $4)
ORDER BY last_seen, key_kind, key
LIMIT $5
""";

    /// <summary>
    /// The sighting rows of the listed keys. $1 server_id, $2 kinds, $3 keys (parallel arrays). The order is total (key, then
    /// database, application, login, host, as the unique index reads them) because the resolver takes the first matching
    /// sighting: a tie broken by whatever order the planner returned would let a rebuild change a key's tag (#5985).
    /// </summary>
    internal const string SightingsOfKeysSql = """
SELECT s.key_kind, s.key, s.database_name, s.object_name, s.application_name, s.login_name, s.host_name, s.text_sample
FROM collect.query_tag_sightings AS s
JOIN unnest($2::text[], $3::text[]) AS k(key_kind, key) ON k.key_kind = s.key_kind AND k.key = s.key
WHERE s.server_id = $1
ORDER BY s.key_kind, s.key, COALESCE(s.database_name, ''), COALESCE(s.application_name, ''), COALESCE(s.login_name, ''), COALESCE(s.host_name, '')
""";

    /// <summary>Every sighting row of one server, in the same total order as <see cref="SightingsOfKeysSql"/> so a reader can resolve one key at a time. $1 server_id.</summary>
    internal const string AllSightingsSql = """
SELECT key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample
FROM collect.query_tag_sightings
WHERE server_id = $1
ORDER BY key_kind, key, COALESCE(database_name, ''), COALESCE(application_name, ''), COALESCE(login_name, ''), COALESCE(host_name, '')
""";

    internal const string DeleteServerMapSql = "DELETE FROM config.query_tag_map WHERE server_id = $1";

    /// <summary>Inserts the resolved rows. $1 server_id, $2..$8 the seven columns as parallel arrays.</summary>
    internal const string InsertMapSql = """
INSERT INTO config.query_tag_map (server_id, key_kind, key, tag_id, source, rule_id, matched_field, matched_value)
SELECT $1, u.key_kind, u.key, u.tag_id, u.source, u.rule_id, u.matched_field, u.matched_value
FROM unnest($2::text[], $3::text[], $4::int[], $5::text[], $6::int[], $7::text[], $8::text[])
    AS u(key_kind, key, tag_id, source, rule_id, matched_field, matched_value)
""";

    /// <summary>
    /// The extend path's write: inserts new rows and updates a changed one, and skips a row that already says the
    /// same (so a key re-sighted every minute does not rewrite its map row). Never deletes.
    /// </summary>
    internal const string UpsertMapSql = InsertMapSql + """

ON CONFLICT (server_id, key_kind, key) DO UPDATE SET
    tag_id = EXCLUDED.tag_id,
    source = EXCLUDED.source,
    rule_id = EXCLUDED.rule_id,
    matched_field = EXCLUDED.matched_field,
    matched_value = EXCLUDED.matched_value,
    built_at = now()
WHERE (config.query_tag_map.tag_id, config.query_tag_map.source, config.query_tag_map.rule_id,
       config.query_tag_map.matched_field, config.query_tag_map.matched_value)
    IS DISTINCT FROM (EXCLUDED.tag_id, EXCLUDED.source, EXCLUDED.rule_id, EXCLUDED.matched_field, EXCLUDED.matched_value)
""";

    /// <summary>Stamps the version a rebuild read at its start. Never moves it backwards. $1 version.</summary>
    internal const string StampBuiltVersionSql =
        "UPDATE config.query_tag_state SET built_version = $1, updated_at = now() WHERE id = 1 AND built_version < $1";

    /// <summary>The database's own clock, so a cursor and a sighting's <c>last_seen</c> are on one clock.</summary>
    public static async Task<DateTime> ClockAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand("SELECT clock_timestamp()", connection) { CommandTimeout = CommandTimeoutSeconds };
        return (DateTime)(await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false))!;
    }

    public static async Task<QueryTagMapSnapshot> ReadSnapshotAsync(NpgsqlConnection connection, CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(System.Data.IsolationLevel.RepeatableRead, cancellationToken).ConfigureAwait(false);

        long rulesVersion = 1;
        long builtVersion = 0;
        await using (var command = new NpgsqlCommand(ReadStateSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            if (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                rulesVersion = reader.GetInt64(0);
                builtVersion = reader.GetInt64(1);
            }
        }

        /* Two tags that fold to one name are ambiguous to a tag comment, which then matches neither: leave the name out. */
        var byFolded = new Dictionary<string, (long Id, int Count)>(StringComparer.OrdinalIgnoreCase);
        await using (var command = new NpgsqlCommand(ReadTagsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                string name = reader.GetString(1).Trim();
                byFolded[name] = byFolded.TryGetValue(name, out var seen) ? (seen.Id, seen.Count + 1) : (reader.GetInt32(0), 1);
            }
        }

        var tagIdsByName = new Dictionary<string, long>(StringComparer.OrdinalIgnoreCase);
        foreach (var (name, (id, count)) in byFolded)
        {
            if (count == 1)
            {
                tagIdsByName[name] = id;
            }
        }

        var rules = new List<QueryTagRule>();
        await using (var command = new NpgsqlCommand(ReadRulesSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                if (TryParseField(reader.GetString(1), out var field))
                {
                    rules.Add(new QueryTagRule(
                        reader.GetInt32(0),
                        field,
                        reader.GetString(2),
                        reader.GetInt32(3),
                        reader.GetBoolean(4),
                        reader.IsDBNull(5) ? null : reader.GetInt32(5),
                        reader.IsDBNull(6) ? null : reader.GetInt32(6)));
                }
            }
        }

        var assignments = new List<QueryTagHandAssignment>();
        await using (var command = new NpgsqlCommand(ReadAssignmentsSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        await using (var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false))
        {
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                int tagId = reader.GetInt32(0);
                string kind = reader.GetString(1);
                int? serverId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
                if (kind == "hash")
                {
                    string raw = reader.GetString(3);
                    assignments.Add(new QueryTagHandAssignment(tagId, QueryTagKeyKind.Hash, QueryHashIdentity.CanonicalKey(raw) ?? raw, null, serverId));
                }
                else if (kind == "object")
                {
                    var objectRef = new QueryTagObject(reader.GetString(4), reader.GetString(5), reader.GetString(6));
                    assignments.Add(new QueryTagHandAssignment(tagId, QueryTagKeyKind.Object, null, objectRef, serverId));
                }
            }
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
        return new QueryTagMapSnapshot(rulesVersion, builtVersion, rules, assignments, tagIdsByName);
    }

    public static async Task<List<QueryTagChangedKey>> ReadChangedKeysAsync(
        NpgsqlConnection connection,
        int serverId,
        DateTime sinceUtc,
        string afterKind,
        string afterKey,
        int limit,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(ChangedKeysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.TimestampTz, Value = DateTime.SpecifyKind(sinceUtc, DateTimeKind.Utc) });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = afterKind });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = afterKey });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = limit });

        var keys = new List<QueryTagChangedKey>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            keys.Add(new QueryTagChangedKey(reader.GetString(0), reader.GetString(1), reader.GetFieldValue<DateTime>(2)));
        }

        return keys;
    }

    /// <summary>The sightings of the listed keys, grouped by (kind, key).</summary>
    public static async Task<Dictionary<(string Kind, string Key), List<QueryTagSighting>>> ReadSightingsOfKeysAsync(
        NpgsqlConnection connection,
        int serverId,
        IReadOnlyList<QueryTagChangedKey> keys,
        CancellationToken cancellationToken)
    {
        var grouped = new Dictionary<(string, string), List<QueryTagSighting>>();
        if (keys.Count == 0)
        {
            return grouped;
        }

        var kinds = new string[keys.Count];
        var names = new string[keys.Count];
        for (int i = 0; i < keys.Count; i++)
        {
            kinds[i] = keys[i].KeyKind;
            names[i] = keys[i].Key;
        }

        await using var command = new NpgsqlCommand(SightingsOfKeysSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = kinds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = names });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            var sighting = ReadSighting(reader);
            var id = (reader.GetString(0), reader.GetString(1));
            if (!grouped.TryGetValue(id, out var list))
            {
                grouped[id] = list = new List<QueryTagSighting>();
            }

            list.Add(sighting);
        }

        return grouped;
    }

    /// <summary>
    /// Streams every sighting of one server, one key's rows at a time, so a rebuild never holds more than one key's
    /// sightings plus the (small) rows it resolved.
    /// </summary>
    public static async IAsyncEnumerable<List<QueryTagSighting>> ReadAllSightingsByKeyAsync(
        NpgsqlConnection connection,
        int serverId,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(AllSightingsSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);

        List<QueryTagSighting>? current = null;
        string? kind = null;
        string? key = null;
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            string rowKind = reader.GetString(0);
            string rowKey = reader.GetString(1);
            if (current is not null && (kind != rowKind || key != rowKey))
            {
                yield return current;
                current = null;
            }

            current ??= new List<QueryTagSighting>();
            kind = rowKind;
            key = rowKey;
            current.Add(ReadSighting(reader));
        }

        if (current is not null)
        {
            yield return current;
        }
    }

    /// <summary>
    /// Replaces one server's map rows with <paramref name="rows"/> in one transaction: a reader sees the old map or
    /// the new one, and a failure part way rolls back to the old one.
    /// </summary>
    public static async Task ReplaceMapAsync(
        NpgsqlConnection connection,
        int serverId,
        IReadOnlyList<QueryTagMapRow> rows,
        CancellationToken cancellationToken)
    {
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken).ConfigureAwait(false);
        await using (var delete = new NpgsqlCommand(DeleteServerMapSql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds })
        {
            delete.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
            await delete.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        if (rows.Count > 0)
        {
            await using var insert = MapCommand(InsertMapSql, connection, transaction, serverId, rows);
            await insert.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The extend path's write: adds and changes rows, never deletes one.</summary>
    public static async Task UpsertMapAsync(
        NpgsqlConnection connection,
        int serverId,
        IReadOnlyList<QueryTagMapRow> rows,
        CancellationToken cancellationToken)
    {
        if (rows.Count == 0)
        {
            return;
        }

        await using var command = MapCommand(UpsertMapSql, connection, null, serverId, rows);
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    public static async Task StampBuiltVersionAsync(NpgsqlConnection connection, long version, CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(StampBuiltVersionSql, connection) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = version });
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    /// <summary>The store's text for a resolver source, matching the map's CHECK.</summary>
    public static string SourceText(QueryTagSource source) => source switch
    {
        QueryTagSource.HandHash => "hand_hash",
        QueryTagSource.HandObject => "hand_object",
        _ => "rule",
    };

    /// <summary>The store's text for a rule field, matching the rules table's CHECK.</summary>
    public static string FieldText(QueryTagRuleField field) => field switch
    {
        QueryTagRuleField.Application => "application",
        QueryTagRuleField.Login => "login",
        QueryTagRuleField.Host => "host",
        QueryTagRuleField.Database => "database",
        QueryTagRuleField.Object => "object",
        QueryTagRuleField.Text => "text",
        _ => "text_tag",
    };

    internal static bool TryParseField(string text, out QueryTagRuleField field)
    {
        switch (text)
        {
            case "application": field = QueryTagRuleField.Application; return true;
            case "login": field = QueryTagRuleField.Login; return true;
            case "host": field = QueryTagRuleField.Host; return true;
            case "database": field = QueryTagRuleField.Database; return true;
            case "object": field = QueryTagRuleField.Object; return true;
            case "text": field = QueryTagRuleField.Text; return true;
            case "text_tag": field = QueryTagRuleField.TextTag; return true;
            default: field = default; return false;
        }
    }

    private static NpgsqlCommand MapCommand(
        string sql, NpgsqlConnection connection, NpgsqlTransaction? transaction, int serverId, IReadOnlyList<QueryTagMapRow> rows)
    {
        var kinds = new string[rows.Count];
        var keys = new string[rows.Count];
        var tagIds = new int[rows.Count];
        var sources = new string[rows.Count];
        var ruleIds = new int?[rows.Count];
        var fields = new string?[rows.Count];
        var values = new string?[rows.Count];
        for (int i = 0; i < rows.Count; i++)
        {
            kinds[i] = rows[i].KeyKind;
            keys[i] = rows[i].Key;
            tagIds[i] = rows[i].TagId;
            sources[i] = rows[i].Source;
            ruleIds[i] = rows[i].RuleId;
            fields[i] = rows[i].MatchedField;
            values[i] = rows[i].MatchedValue;
        }

        var command = new NpgsqlCommand(sql, connection, transaction) { CommandTimeout = CommandTimeoutSeconds };
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = kinds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = keys });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = tagIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = sources });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Integer, Value = ruleIds });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = fields });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Text, Value = values });
        return command;
    }

    private static QueryTagSighting ReadSighting(NpgsqlDataReader reader)
    {
        string kind = reader.GetString(0);
        string key = reader.GetString(1);
        string? database = reader.IsDBNull(2) ? null : reader.GetString(2);
        string? objectName = reader.IsDBNull(3) ? null : reader.GetString(3);
        string? application = reader.IsDBNull(4) ? null : reader.GetString(4);
        string? login = reader.IsDBNull(5) ? null : reader.GetString(5);
        string? host = reader.IsDBNull(6) ? null : reader.GetString(6);
        string? sample = reader.IsDBNull(7) ? null : reader.GetString(7);
        var keyKind = kind switch
        {
            "hash" => QueryTagKeyKind.Hash,
            "object" => QueryTagKeyKind.Object,
            _ => QueryTagKeyKind.Caller,
        };

        /* An object key's own text is the qualified name, so it carries the object even if the column was empty. */
        var objectRef = ParseQualified(objectName ?? (keyKind == QueryTagKeyKind.Object ? key : null), database);
        return new QueryTagSighting(keyKind, key, database, objectRef, application, login, host, sample);
    }

    /// <summary>
    /// Reads a <c>database.schema.name</c> text (<see cref="QueryTagObject.Qualified"/>, the only form the sightings
    /// table keeps) back into its parts. The database is the text before the first dot, or the sighting's own database
    /// when the text starts with it (a database name may carry dots); a schema is the text up to the next dot. A name
    /// with dots in its schema is read at the first dot, which is the only ambiguity the one column leaves.
    /// </summary>
    internal static QueryTagObject? ParseQualified(string? qualified, string? database)
    {
        if (string.IsNullOrEmpty(qualified))
        {
            return null;
        }

        string rest;
        string? db;
        if (!string.IsNullOrEmpty(database) && qualified.StartsWith(database + ".", StringComparison.Ordinal))
        {
            db = database;
            rest = qualified[(database.Length + 1)..];
        }
        else
        {
            int first = qualified.IndexOf('.', StringComparison.Ordinal);
            if (first < 0)
            {
                return new QueryTagObject(null, null, qualified);
            }

            db = first == 0 ? null : qualified[..first];
            rest = qualified[(first + 1)..];
        }

        int second = rest.IndexOf('.', StringComparison.Ordinal);
        return second < 0
            ? new QueryTagObject(db, null, rest)
            : new QueryTagObject(db, second == 0 ? null : rest[..second], rest[(second + 1)..]);
    }
}
