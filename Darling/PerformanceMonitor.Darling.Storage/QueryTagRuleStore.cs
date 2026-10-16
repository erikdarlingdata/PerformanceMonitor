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
using PerformanceMonitor.Common.QueryTags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>A stored query tag rule, as <c>config.query_tag_rules</c> holds it.</summary>
public sealed record QueryTagRuleRow(
    long Id,
    QueryTagRuleField Field,
    string Pattern,
    int Priority,
    bool Enabled,
    int? TagId,
    int? ServerId,
    string CreatedBy,
    DateTimeOffset CreatedAt)
{
    /// <summary>The shared matcher's view of the row.</summary>
    public QueryTagRule ToRule() => new(Id, Field, Pattern, Priority, Enabled, TagId, ServerId);
}

/// <summary>A query tag (<c>config.query_tags</c>): id, name and parent.</summary>
public sealed record QueryTagNameRow(int Id, string Name, int? ParentId);

/// <summary>The version row (<c>config.query_tag_state</c>): a rule write bumps <see cref="RulesVersion"/>; the map is stale until
/// <see cref="BuiltVersion"/> catches up on a collector pass.</summary>
public sealed record QueryTagState(long RulesVersion, long BuiltVersion);

/// <summary>One stored sighting row (<c>collect.query_tag_sightings</c>), read for the rule test.</summary>
public sealed record QueryTagSightingRow(
    int ServerId,
    string KeyKind,
    string Key,
    string? DatabaseName,
    string? ObjectName,
    string? ApplicationName,
    string? LoginName,
    string? HostName,
    string? TextSample,
    DateTimeOffset LastSeen);

/// <summary>
/// Reads and writes of the query tag rules (#5982, wave 2 of #5971) over <c>config.query_tag_rules</c>, plus the reads the
/// rule tools need beside them: the tags (to refuse a rule for an unknown tag), the registered servers, the hand assignments, the
/// sightings and the version row. The matching and the validation live in <c>PerformanceMonitor.Common.QueryTags</c>; this class
/// only moves rows. A rule write changes no map row: the statement trigger of V192 bumps <c>rules_version</c> and the next
/// collector pass rebuilds the map.
/// </summary>
public sealed class QueryTagRuleStore
{
    private const string RuleColumns = "id, match_field, pattern, priority, enabled, tag_id, server_id, created_by, created_at";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    public QueryTagRuleStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds = 30)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <summary>The stored name of a field, the value <c>ck_query_tag_rules_field</c> allows.</summary>
    public static string FieldName(QueryTagRuleField field) => field switch
    {
        QueryTagRuleField.Application => "application",
        QueryTagRuleField.Login => "login",
        QueryTagRuleField.Host => "host",
        QueryTagRuleField.Database => "database",
        QueryTagRuleField.Object => "object",
        QueryTagRuleField.Text => "text",
        QueryTagRuleField.TextTag => "text_tag",
        _ => throw new ArgumentOutOfRangeException(nameof(field)),
    };

    /// <summary>The field a stored (or tool-supplied) name means; false for a name that is none of the seven.</summary>
    public static bool TryParseField(string? name, out QueryTagRuleField field)
    {
        switch ((name ?? string.Empty).Trim().ToLowerInvariant())
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

    /// <summary>Every rule, in the order the resolver applies them: priority, then id.</summary>
    public async Task<IReadOnlyList<QueryTagRuleRow>> ListRulesAsync(CancellationToken ct = default)
    {
        await using var command = Command($"SELECT {RuleColumns} FROM config.query_tag_rules ORDER BY priority, id");
        return await ReadRulesAsync(command, ct);
    }

    /// <summary>One rule by id, or null.</summary>
    public async Task<QueryTagRuleRow?> GetRuleAsync(long id, CancellationToken ct = default)
    {
        await using var command = Command($"SELECT {RuleColumns} FROM config.query_tag_rules WHERE id = $1");
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var rows = await ReadRulesAsync(command, ct);
        return rows.Count == 0 ? null : rows[0];
    }

    /// <summary>Inserts a rule and returns the row as stored. The rule is already validated by the caller.</summary>
    public async Task<QueryTagRuleRow> InsertRuleAsync(
        QueryTagRuleField field, string pattern, int priority, bool enabled, int? tagId, int? serverId, string createdBy, CancellationToken ct = default)
    {
        await using var command = Command(
            $"INSERT INTO config.query_tag_rules (match_field, pattern, priority, enabled, tag_id, server_id, created_by) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING {RuleColumns}");
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = FieldName(field) });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = pattern });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = priority });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        command.Parameters.Add(Nullable(tagId));
        command.Parameters.Add(Nullable(serverId));
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = createdBy });
        var rows = await ReadRulesAsync(command, ct);
        return rows[0];
    }

    /// <summary>Writes the four editable columns of one rule and returns the row as stored, or null when the rule is gone.</summary>
    public async Task<QueryTagRuleRow?> UpdateRuleAsync(
        long id, string pattern, int priority, bool enabled, int? tagId, int? serverId, CancellationToken ct = default)
    {
        await using var command = Command(
            $"UPDATE config.query_tag_rules SET pattern = $2, priority = $3, enabled = $4, tag_id = $5, server_id = $6 WHERE id = $1 RETURNING {RuleColumns}");
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = pattern });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = priority });
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });
        command.Parameters.Add(Nullable(tagId));
        command.Parameters.Add(Nullable(serverId));
        var rows = await ReadRulesAsync(command, ct);
        return rows.Count == 0 ? null : rows[0];
    }

    /// <summary>Deletes one rule; false when there was none.</summary>
    public async Task<bool> DeleteRuleAsync(long id, CancellationToken ct = default)
    {
        await using var command = Command("DELETE FROM config.query_tag_rules WHERE id = $1");
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        return await command.ExecuteNonQueryAsync(ct) > 0;
    }

    /// <summary>Every query tag (id, name, parent).</summary>
    public async Task<IReadOnlyList<QueryTagNameRow>> ReadTagsAsync(CancellationToken ct = default)
    {
        var tags = new List<QueryTagNameRow>();
        await using var command = Command("SELECT id, name, parent_id FROM config.query_tags ORDER BY id");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            tags.Add(new QueryTagNameRow(reader.GetInt32(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetInt32(2)));
        }

        return tags;
    }

    /// <summary>Which of the ids are registered servers: the same registry the server-tag tools check.</summary>
    public async Task<IReadOnlySet<int>> FindRegisteredServerIdsAsync(IReadOnlyCollection<int> serverIds, CancellationToken ct = default)
    {
        var found = new HashSet<int>();
        if (serverIds.Count == 0)
        {
            return found;
        }

        await using var command = Command(ServerTagStore.RegisteredServerIdsSql);
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = [.. serverIds] });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(reader.GetInt32(0));
        }

        return found;
    }

    /// <summary>The hand assignments, as the resolver takes them. A row the resolver cannot use (a malformed key) is skipped.</summary>
    public async Task<IReadOnlyList<QueryTagHandAssignment>> ReadAssignmentsAsync(CancellationToken ct = default)
    {
        var rows = new List<QueryTagHandAssignment>();
        await using var command = Command("SELECT tag_id, kind, server_id, query_hash, database_name, schema_name, object_name FROM config.query_tag_assignments");
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            var tagId = reader.GetInt32(0);
            var kind = reader.GetString(1);
            int? serverId = reader.IsDBNull(2) ? null : reader.GetInt32(2);
            if (kind == "hash" && !reader.IsDBNull(3))
            {
                rows.Add(new QueryTagHandAssignment(tagId, QueryTagKeyKind.Hash, Key: reader.GetString(3), ServerId: serverId));
            }
            else if (kind == "object" && !reader.IsDBNull(4) && !reader.IsDBNull(5) && !reader.IsDBNull(6))
            {
                rows.Add(new QueryTagHandAssignment(
                    tagId, QueryTagKeyKind.Object, ObjectRef: new QueryTagObject(reader.GetString(4), reader.GetString(5), reader.GetString(6)), ServerId: serverId));
            }
        }

        return rows;
    }

    /// <summary>The version row: how far the rules have moved and how far the map has caught up.</summary>
    public async Task<QueryTagState> ReadStateAsync(CancellationToken ct = default)
    {
        await using var command = Command("SELECT rules_version, built_version FROM config.query_tag_state WHERE id = 1");
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? new QueryTagState(reader.GetInt64(0), reader.GetInt64(1)) : new QueryTagState(0, 0);
    }

    /// <summary>
    /// The sightings seen since <paramref name="since"/>, newest first, for one server or (null) every server, at most
    /// <paramref name="maxRows"/> of them. Ordered so a cut keeps the keys seen most recently.
    /// </summary>
    public async Task<IReadOnlyList<QueryTagSightingRow>> ReadSightingsAsync(int? serverId, DateTimeOffset since, int maxRows, CancellationToken ct = default)
    {
        var rows = new List<QueryTagSightingRow>();
        await using var command = Command(
            "SELECT server_id, key_kind, key, database_name, object_name, application_name, login_name, host_name, text_sample, last_seen " +
            "FROM collect.query_tag_sightings WHERE last_seen >= $1 AND ($2::integer IS NULL OR server_id = $2) " +
            "ORDER BY last_seen DESC, server_id, key_kind, key LIMIT $3");
        command.Parameters.Add(new NpgsqlParameter<DateTime> { TypedValue = since.UtcDateTime, NpgsqlDbType = NpgsqlDbType.TimestampTz });
        command.Parameters.Add(Nullable(serverId));
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = maxRows });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            rows.Add(new QueryTagSightingRow(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.GetString(2),
                Text(reader, 3), Text(reader, 4), Text(reader, 5), Text(reader, 6), Text(reader, 7), Text(reader, 8),
                new DateTimeOffset(reader.GetDateTime(9).ToUniversalTime())));
        }

        return rows;
    }

    private NpgsqlCommand Command(string sql)
    {
        var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = _commandTimeoutSeconds;
        return command;
    }

    private static NpgsqlParameter<int?> Nullable(int? value) => new() { TypedValue = value, NpgsqlDbType = NpgsqlDbType.Integer };

    private static string? Text(NpgsqlDataReader reader, int ordinal) => reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static async Task<IReadOnlyList<QueryTagRuleRow>> ReadRulesAsync(NpgsqlCommand command, CancellationToken ct)
    {
        var rows = new List<QueryTagRuleRow>();
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (!TryParseField(reader.GetString(1), out var field))
            {
                continue;
            }

            rows.Add(new QueryTagRuleRow(
                reader.GetInt32(0),
                field,
                reader.GetString(2),
                reader.GetInt32(3),
                reader.GetBoolean(4),
                reader.IsDBNull(5) ? null : reader.GetInt32(5),
                reader.IsDBNull(6) ? null : reader.GetInt32(6),
                reader.GetString(7),
                new DateTimeOffset(reader.GetDateTime(8).ToUniversalTime())));
        }

        return rows;
    }

    /// <summary>The sighting's object, in the form the resolver matches: the stored <c>object_name</c> is read as
    /// <c>database.schema.name</c> or <c>schema.name</c> when it carries dots, else as a bare name in the sighting's database.</summary>
    public static QueryTagObject? ObjectOf(QueryTagSightingRow row)
    {
        if (string.IsNullOrEmpty(row.ObjectName))
        {
            return null;
        }

        var parts = row.ObjectName.Split('.');
        return parts.Length switch
        {
            3 => new QueryTagObject(parts[0], parts[1], parts[2]),
            2 => new QueryTagObject(row.DatabaseName, parts[0], parts[1]),
            _ => new QueryTagObject(row.DatabaseName, null, row.ObjectName),
        };
    }

    /// <summary>The sighting as the shared resolver takes it.</summary>
    public static QueryTagSighting ToSighting(QueryTagSightingRow row)
    {
        var kind = row.KeyKind switch
        {
            "object" => QueryTagKeyKind.Object,
            "caller" => QueryTagKeyKind.Caller,
            _ => QueryTagKeyKind.Hash,
        };
        return new QueryTagSighting(kind, row.Key, row.DatabaseName, ObjectOf(row), row.ApplicationName, row.LoginName, row.HostName, row.TextSample);
    }
}
