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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>#7643: one stored version of a table or view: the header columns of <c>collect.schema_objects</c> and the definition rebuilt from its children.</summary>
public sealed record SchemaStoredVersion(
    long VersionId,
    string DatabaseName,
    string? AgName,
    DateTime ValidFrom,
    DateTime? ValidTo,
    string ChangeKind,
    string DetectedBy,
    int DetectedOnServerId,
    string? ChangeLogin,
    string? ChangeApp,
    string? ChangeHost,
    SchemaObjectDefinition Definition);

/// <summary>One line of an object's history: just enough to pick the version that was current at a time.</summary>
public sealed record SchemaVersionStamp(long VersionId, int ObjectId, string Schema, string Name, DateTime ValidFrom, DateTime? ValidTo, string ChangeKind);

/// <summary>A version row of the change list, with the version it follows (null for the first of its line).</summary>
public sealed record SchemaChangeRow(
    long VersionId, long? PreviousVersionId, string DatabaseName, string? AgName, int ObjectId, string Schema, string Name, char Type,
    DateTime ValidFrom, DateTime? ValidTo, string ChangeKind, string DetectedBy, string? Login, string? App, string? Host);

/// <summary>A current object a search found, and which texts of its definition matched when the search was a text one.</summary>
public sealed record SchemaFoundObject(
    long VersionId, string DatabaseName, string? AgName, int ObjectId, string Schema, string Name, char Type, DateTime ValidFrom,
    bool InViewText, bool InComputedColumn, bool InDefault, bool InCheck, bool InFilter);

/// <summary>The filters of a search over the current objects. Every text is matched ignoring case; a null filter is not applied.</summary>
public sealed record SchemaObjectSearch(
    string? Database, string? NamePrefix, string? NameContains, string? ColumnName, string? ReferencesSchema, string? ReferencesTable,
    string? DefinitionContains);

/// <summary>An availability group a server holds a database of, as the AG collector last saw it.</summary>
public sealed record SchemaAgMembership(Guid GroupId, string? AgName, string DatabaseName);

/// <summary>
/// #7643: the read side of the schema history. Every statement names its history key as <c>server_id = k AND ag_group_id IS NULL</c> or
/// <c>server_id IS NULL AND ag_group_id = g</c> and never <c>IS NOT DISTINCT FROM</c>, which no index serves. Lists (key, included,
/// foreign key and referenced columns) are stored joined and escaped and are read back with <see cref="SchemaCatalogReader.SplitList"/>.
/// </summary>
public static class SchemaHistoryReader
{
    /// <summary>One statement under construction: positional parameters are added as the text names them.</summary>
    private sealed class Sql
    {
        private readonly List<NpgsqlParameter> _parameters = [];

        public StringBuilder Text { get; } = new();

        public string P(NpgsqlDbType type, object? value)
        {
            _parameters.Add(new NpgsqlParameter { NpgsqlDbType = type, Value = value ?? DBNull.Value });
            return "$" + _parameters.Count.ToString(CultureInfo.InvariantCulture);
        }

        public NpgsqlCommand Command(NpgsqlConnection c, NpgsqlTransaction? tx)
        {
            var cmd = new NpgsqlCommand(Text.ToString(), c, tx) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
            foreach (var p in _parameters)
            {
                cmd.Parameters.Add(p);
            }

            return cmd;
        }

        /// <summary>The condition that selects the rows of <paramref name="keys"/> on <paramref name="alias"/>.</summary>
        public string Scope(IReadOnlyList<SchemaHistoryKey> keys, string alias)
        {
            var parts = keys.Select(k => k.AgGroupId is { } ag
                ? $"({alias}.server_id IS NULL AND {alias}.ag_group_id = {P(NpgsqlDbType.Uuid, ag)})"
                : $"({alias}.server_id = {P(NpgsqlDbType.Integer, k.ServerId)} AND {alias}.ag_group_id IS NULL)").ToList();
            return parts.Count == 1 ? parts[0] : "(" + string.Join(" OR ", parts) + ")";
        }
    }

    /// <summary>The availability groups <paramref name="serverId"/> holds a database of (or the one <paramref name="database"/>), newest sighting within a week.</summary>
    public static async Task<IReadOnlyList<SchemaAgMembership>> ReadAgMembershipsAsync(
        NpgsqlConnection c, int serverId, string? database, DateTime nowUtc, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        var q = new Sql();
        q.Text.Append("SELECT group_id, max(ag_name), database_name FROM collect.ag_database_replica_states WHERE server_id = ")
            .Append(q.P(NpgsqlDbType.Integer, serverId))
            .Append(" AND collection_time >= ").Append(q.P(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(nowUtc.AddDays(-7), DateTimeKind.Unspecified)))
            .Append(" AND group_id IS NOT NULL");
        if (database != null)
        {
            q.Text.Append(" AND lower(database_name) = lower(").Append(q.P(NpgsqlDbType.Text, database)).Append(')');
        }

        /* An availability group dropped and recreated within the week leaves two group ids for one database: the newest sighting first. */
        q.Text.Append(" GROUP BY group_id, database_name ORDER BY max(collection_time) DESC");
        var result = new List<SchemaAgMembership>();
        await using var cmd = q.Command(c, null);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (Guid.TryParse(reader.GetString(0), out var group))
            {
                result.Add(new SchemaAgMembership(group, reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2)));
            }
        }

        return result;
    }

    /// <summary>
    /// The versions whose name is <paramref name="name"/> (and schema, when given), one per object id, newest first. <paramref name="at"/> null
    /// takes the current ones; with <paramref name="at"/> the ones that held at that time; <paramref name="anyTime"/> every version ever.
    /// </summary>
    public static async Task<IReadOnlyList<SchemaVersionStamp>> FindByNameAsync(
        NpgsqlConnection c, SchemaHistoryKey key, string database, string? schema, string name, DateTime? at, bool anyTime, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        var q = new Sql();
        q.Text.Append("SELECT DISTINCT ON (o.object_id) o.version_id, o.object_id, o.schema_name, o.object_name, o.valid_from, o.valid_to, o.change_kind FROM collect.schema_objects o WHERE ")
            .Append(q.Scope([key], "o"))
            .Append(" AND o.database_name = ").Append(q.P(NpgsqlDbType.Text, database))
            .Append(" AND lower(o.object_name) = lower(").Append(q.P(NpgsqlDbType.Text, name)).Append(')');
        if (schema != null)
        {
            q.Text.Append(" AND lower(o.schema_name) = lower(").Append(q.P(NpgsqlDbType.Text, schema)).Append(')');
        }

        if (!anyTime)
        {
            q.Text.Append(at is { } t
                ? $" AND o.valid_from <= {q.P(NpgsqlDbType.Timestamp, Naive(t))} AND (o.valid_to IS NULL OR o.valid_to > {q.P(NpgsqlDbType.Timestamp, Naive(t))}) AND o.change_kind <> 'dropped'"
                : " AND o.valid_to IS NULL");
        }

        q.Text.Append(" ORDER BY o.object_id, o.version_id DESC");
        return await ReadStampsAsync(c, q, ct);
    }

    /// <summary>The whole line of one object: every version with its object id or its schema and name, oldest first.</summary>
    public static async Task<IReadOnlyList<SchemaVersionStamp>> ReadHistoryAsync(
        NpgsqlConnection c, SchemaHistoryKey key, string database, int objectId, string schema, string name, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        var q = new Sql();
        q.Text.Append("SELECT o.version_id, o.object_id, o.schema_name, o.object_name, o.valid_from, o.valid_to, o.change_kind FROM collect.schema_objects o WHERE ")
            .Append(q.Scope([key], "o"))
            .Append(" AND o.database_name = ").Append(q.P(NpgsqlDbType.Text, database))
            .Append(" AND (o.object_id = ").Append(q.P(NpgsqlDbType.Integer, objectId))
            .Append(" OR (o.schema_name = ").Append(q.P(NpgsqlDbType.Text, schema))
            .Append(" AND o.object_name = ").Append(q.P(NpgsqlDbType.Text, name))
            .Append(")) ORDER BY o.version_id");
        return await ReadStampsAsync(c, q, ct);
    }

    private static async Task<IReadOnlyList<SchemaVersionStamp>> ReadStampsAsync(NpgsqlConnection c, Sql q, CancellationToken ct)
    {
        var result = new List<SchemaVersionStamp>();
        await using var cmd = q.Command(c, null);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new SchemaVersionStamp(
                reader.GetInt64(0), reader.GetInt32(1), reader.GetString(2), reader.GetString(3), reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetDateTime(5), reader.GetString(6)));
        }

        return result;
    }

    /// <summary>The versions of <paramref name="versionIds"/> with their children, by version id.</summary>
    public static async Task<IReadOnlyDictionary<long, SchemaStoredVersion>> LoadVersionsAsync(
        NpgsqlConnection c, IReadOnlyCollection<long> versionIds, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        var result = new Dictionary<long, SchemaStoredVersion>();
        if (versionIds.Count == 0)
        {
            return result;
        }

        var ids = versionIds.Distinct().ToArray();
        var columns = new Dictionary<long, List<SchemaColumn>>();
        var indexes = new Dictionary<long, List<SchemaIndex>>();
        var constraints = new Dictionary<long, List<SchemaConstraint>>();
        var triggers = new Dictionary<long, List<SchemaTrigger>>();

        await using (var cmd = ChildCommand(c, @"
SELECT version_id, ordinal, name, type_name, max_length, precision, scale, is_nullable, identity_seed, identity_increment,
       computed_definition, is_persisted, default_name, default_definition, collation_name, is_sparse, is_rowguidcol
FROM collect.schema_columns WHERE version_id = ANY($1) ORDER BY version_id, ordinal", ids))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                Bucket(columns, reader.GetInt64(0)).Add(new SchemaColumn(
                    reader.GetInt32(1), reader.GetString(2), reader.GetString(3), Int(reader, 4), Int(reader, 5), Int(reader, 6), reader.GetBoolean(7),
                    Dec(reader, 8), Dec(reader, 9), Str(reader, 10), Bool(reader, 11), Str(reader, 12), Str(reader, 13), Str(reader, 14),
                    Bool(reader, 15), Bool(reader, 16)));
            }
        }

        await using (var cmd = ChildCommand(c, @"
SELECT version_id, index_id, name, type_desc, is_unique, is_primary_key, is_unique_constraint, key_columns, included_columns,
       filter_definition, data_compression, is_disabled
FROM collect.schema_indexes WHERE version_id = ANY($1) ORDER BY version_id, index_id", ids))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                Bucket(indexes, reader.GetInt64(0)).Add(new SchemaIndex(
                    reader.GetInt32(1), Str(reader, 2), reader.GetString(3), reader.GetBoolean(4), reader.GetBoolean(5), reader.GetBoolean(6),
                    SchemaCatalogReader.SplitList(Str(reader, 7)), SchemaCatalogReader.SplitList(Str(reader, 8)), Str(reader, 9), Str(reader, 10),
                    reader.GetBoolean(11)));
            }
        }

        await using (var cmd = ChildCommand(c, @"
SELECT version_id, kind, name, definition, fk_columns, referenced_schema, referenced_table, referenced_columns, delete_action,
       update_action, is_disabled, is_not_trusted, is_system_named
FROM collect.schema_constraints WHERE version_id = ANY($1) ORDER BY version_id, kind, name", ids))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                Bucket(constraints, reader.GetInt64(0)).Add(new SchemaConstraint(
                    reader.GetString(1)[0], reader.GetString(2), Str(reader, 3), SchemaCatalogReader.SplitList(Str(reader, 4)), Str(reader, 5),
                    Str(reader, 6), SchemaCatalogReader.SplitList(Str(reader, 7)), Str(reader, 8), Str(reader, 9), reader.GetBoolean(10),
                    reader.GetBoolean(11), reader.GetBoolean(12)));
            }
        }

        await using (var cmd = ChildCommand(c, @"
SELECT version_id, name, is_instead_of, events FROM collect.schema_triggers WHERE version_id = ANY($1) ORDER BY version_id, name", ids))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                Bucket(triggers, reader.GetInt64(0)).Add(new SchemaTrigger(reader.GetString(1), reader.GetBoolean(2), reader.GetString(3)));
            }
        }

        await using (var cmd = ChildCommand(c, @"
SELECT version_id, database_name, ag_name, object_id, schema_name, object_name, object_type, valid_from, valid_to, change_kind, detected_by,
       detected_on_server_id, modify_date, change_login, change_app, change_host, temporal_type, history_table, is_memory_optimized,
       durability, partition_scheme, partition_column, lock_escalation, is_schema_bound, is_indexed, view_definition
FROM collect.schema_objects WHERE version_id = ANY($1)", ids))
        await using (var reader = await cmd.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                var id = reader.GetInt64(0);
                var type = reader.GetString(6)[0];
                var table = type == 'U'
                    ? new SchemaTableProperties(Str(reader, 16), Str(reader, 17), Bool(reader, 18), Str(reader, 19), Str(reader, 20), Str(reader, 21), Str(reader, 22))
                    : null;
                var view = type == 'V' ? new SchemaViewProperties(Bool(reader, 23), Bool(reader, 24), Str(reader, 25)) : null;
                var definition = new SchemaObjectDefinition(
                    reader.GetInt32(3), reader.GetString(4), reader.GetString(5), type, reader.IsDBNull(12) ? null : reader.GetDateTime(12), table, view,
                    columns.GetValueOrDefault(id) ?? [], indexes.GetValueOrDefault(id) ?? [], constraints.GetValueOrDefault(id) ?? [],
                    triggers.GetValueOrDefault(id) ?? []);
                result[id] = new SchemaStoredVersion(
                    id, reader.GetString(1), Str(reader, 2), reader.GetDateTime(7), reader.IsDBNull(8) ? null : reader.GetDateTime(8),
                    reader.GetString(9), reader.GetString(10), reader.GetInt32(11), Str(reader, 13), Str(reader, 14), Str(reader, 15), definition);
            }
        }

        return result;
    }

    /// <summary>
    /// The versions that opened in [<paramref name="fromUtc"/>, <paramref name="toUtc"/>], newest first, from <paramref name="offset"/>,
    /// at most <paramref name="fetch"/> of them (the caller asks for the limit plus one). A baseline version is left out unless asked for.
    /// </summary>
    public static async Task<IReadOnlyList<SchemaChangeRow>> ReadChangesAsync(
        NpgsqlConnection c, IReadOnlyList<SchemaHistoryKey> keys, string? database, string? schema, string? name, DateTime fromUtc, DateTime toUtc,
        bool includeBaseline, int fetch, int offset, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        var q = new Sql();
        q.Text.Append("SELECT o.version_id, ").Append(PreviousVersionSql).Append(@",
       o.database_name, o.ag_name, o.object_id, o.schema_name, o.object_name, o.object_type, o.valid_from, o.valid_to, o.change_kind, o.detected_by,
       o.change_login, o.change_app, o.change_host
FROM collect.schema_objects o WHERE ")
            .Append(q.Scope(keys, "o"))
            .Append(" AND o.valid_from >= ").Append(q.P(NpgsqlDbType.Timestamp, Naive(fromUtc)))
            .Append(" AND o.valid_from <= ").Append(q.P(NpgsqlDbType.Timestamp, Naive(toUtc)));
        if (database != null)
        {
            q.Text.Append(" AND o.database_name = ").Append(q.P(NpgsqlDbType.Text, database));
        }

        if (name != null)
        {
            q.Text.Append(" AND lower(o.object_name) = lower(").Append(q.P(NpgsqlDbType.Text, name)).Append(')');
        }

        if (schema != null)
        {
            q.Text.Append(" AND lower(o.schema_name) = lower(").Append(q.P(NpgsqlDbType.Text, schema)).Append(')');
        }

        if (!includeBaseline)
        {
            q.Text.Append(" AND o.change_kind <> 'baseline'");
        }

        q.Text.Append(" ORDER BY o.valid_from DESC, o.version_id DESC LIMIT ").Append(q.P(NpgsqlDbType.Integer, fetch))
            .Append(" OFFSET ").Append(q.P(NpgsqlDbType.Integer, offset));

        var result = new List<SchemaChangeRow>();
        await using var cmd = q.Command(c, null);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new SchemaChangeRow(
                reader.GetInt64(0), reader.IsDBNull(1) ? null : reader.GetInt64(1), reader.GetString(2), Str(reader, 3), reader.GetInt32(4),
                reader.GetString(5), reader.GetString(6), reader.GetString(7)[0], reader.GetDateTime(8), reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                reader.GetString(10), reader.GetString(11), Str(reader, 12), Str(reader, 13), Str(reader, 14)));
        }

        return result;
    }

    /// <summary>
    /// The current objects that pass every filter of <paramref name="search"/>, by database and name, at most <paramref name="fetch"/> of them.
    /// A column name, a referenced table and a name use the partial indexes of the current rows. A text search is the caller's to bound
    /// (the transaction <paramref name="tx"/> carries its statement timeout).
    /// </summary>
    public static async Task<IReadOnlyList<SchemaFoundObject>> FindCurrentAsync(
        NpgsqlConnection c, NpgsqlTransaction? tx, IReadOnlyList<SchemaHistoryKey> keys, SchemaObjectSearch search, int fetch, CancellationToken ct)
    {
        ArgumentNullException.ThrowIfNull(c);
        ArgumentNullException.ThrowIfNull(search);
        var q = new Sql();
        var text = search.DefinitionContains is null ? null : q.P(NpgsqlDbType.Text, "%" + EscapeLike(search.DefinitionContains) + "%");
        q.Text.Append("SELECT o.version_id, o.database_name, o.ag_name, o.object_id, o.schema_name, o.object_name, o.object_type, o.valid_from");
        if (text != null)
        {
            q.Text.Append($@",
       coalesce(o.view_definition ILIKE {text} ESCAPE '\', false),
       EXISTS (SELECT 1 FROM collect.schema_columns x WHERE x.version_id = o.version_id AND x.is_current AND x.computed_definition ILIKE {text} ESCAPE '\'),
       EXISTS (SELECT 1 FROM collect.schema_columns x WHERE x.version_id = o.version_id AND x.is_current AND x.default_definition ILIKE {text} ESCAPE '\'),
       EXISTS (SELECT 1 FROM collect.schema_constraints x WHERE x.version_id = o.version_id AND x.is_current AND x.definition ILIKE {text} ESCAPE '\'),
       EXISTS (SELECT 1 FROM collect.schema_indexes x WHERE x.version_id = o.version_id AND x.is_current AND x.filter_definition ILIKE {text} ESCAPE '\')");
        }
        else
        {
            q.Text.Append(", false, false, false, false, false");
        }

        q.Text.Append(" FROM collect.schema_objects o WHERE o.valid_to IS NULL AND ").Append(q.Scope(keys, "o"));
        if (search.Database != null)
        {
            q.Text.Append(" AND o.database_name = ").Append(q.P(NpgsqlDbType.Text, search.Database));
        }

        if (search.NamePrefix != null)
        {
            q.Text.Append(" AND lower(o.object_name) LIKE lower(").Append(q.P(NpgsqlDbType.Text, EscapeLike(search.NamePrefix) + "%")).Append(@") ESCAPE '\'");
        }

        if (search.NameContains != null)
        {
            q.Text.Append(" AND lower(o.object_name) LIKE lower(").Append(q.P(NpgsqlDbType.Text, "%" + EscapeLike(search.NameContains) + "%")).Append(@") ESCAPE '\'");
        }

        if (search.ColumnName != null)
        {
            q.Text.Append(" AND EXISTS (SELECT 1 FROM collect.schema_columns k WHERE k.version_id = o.version_id AND k.is_current AND lower(k.name) = lower(")
                .Append(q.P(NpgsqlDbType.Text, search.ColumnName)).Append("))");
        }

        if (search.ReferencesTable != null)
        {
            q.Text.Append(" AND EXISTS (SELECT 1 FROM collect.schema_constraints k WHERE k.version_id = o.version_id AND k.is_current AND k.kind = 'F' AND lower(k.referenced_table) = lower(")
                .Append(q.P(NpgsqlDbType.Text, search.ReferencesTable)).Append(')');
            if (search.ReferencesSchema != null)
            {
                q.Text.Append(" AND lower(k.referenced_schema) = lower(").Append(q.P(NpgsqlDbType.Text, search.ReferencesSchema)).Append(')');
            }

            q.Text.Append(')');
        }

        if (text != null)
        {
            q.Text.Append($@" AND (o.view_definition ILIKE {text} ESCAPE '\'
       OR EXISTS (SELECT 1 FROM collect.schema_columns x WHERE x.version_id = o.version_id AND x.is_current AND (x.computed_definition ILIKE {text} ESCAPE '\' OR x.default_definition ILIKE {text} ESCAPE '\'))
       OR EXISTS (SELECT 1 FROM collect.schema_constraints x WHERE x.version_id = o.version_id AND x.is_current AND x.definition ILIKE {text} ESCAPE '\')
       OR EXISTS (SELECT 1 FROM collect.schema_indexes x WHERE x.version_id = o.version_id AND x.is_current AND x.filter_definition ILIKE {text} ESCAPE '\'))");
        }

        q.Text.Append(" ORDER BY o.database_name, o.schema_name, o.object_name, o.version_id LIMIT ").Append(q.P(NpgsqlDbType.Integer, fetch));

        var result = new List<SchemaFoundObject>();
        await using var cmd = q.Command(c, tx);
        await using var reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            result.Add(new SchemaFoundObject(
                reader.GetInt64(0), reader.GetString(1), Str(reader, 2), reader.GetInt32(3), reader.GetString(4), reader.GetString(5),
                reader.GetString(6)[0], reader.GetDateTime(7), reader.GetBoolean(8), reader.GetBoolean(9), reader.GetBoolean(10),
                reader.GetBoolean(11), reader.GetBoolean(12)));
        }

        return result;
    }

    /* The version a change row follows, for its diff. A baseline row has none, so it runs no lookup. Each lookup names the history key
       as an index serves it (one arm for a server key, one for a group key, no OR across them) and is first by object id, which
       idx_schema_objects_object_history serves. Only an altered row falls back to the schema and name: an object dropped and recreated
       under a new object_id continues its line. A created row never does, so a new table that took a renamed table's name in the same
       pass is not diffed against the old one. */
    private const string PreviousVersionSql = @"CASE
           WHEN o.change_kind = 'baseline' THEN NULL
           WHEN o.server_id IS NOT NULL THEN COALESCE(
               (SELECT p.version_id FROM collect.schema_objects p
                WHERE p.server_id = o.server_id AND p.ag_group_id IS NULL AND p.database_name = o.database_name AND p.object_id = o.object_id
                  AND p.valid_from <= o.valid_from AND p.version_id < o.version_id AND p.valid_to = o.valid_from
                ORDER BY p.version_id DESC LIMIT 1),
               CASE WHEN o.change_kind = 'altered' THEN
               (SELECT p.version_id FROM collect.schema_objects p
                WHERE p.server_id = o.server_id AND p.ag_group_id IS NULL AND p.database_name = o.database_name
                  AND p.schema_name = o.schema_name AND p.object_name = o.object_name
                  AND p.version_id < o.version_id AND p.valid_to = o.valid_from
                ORDER BY p.version_id DESC LIMIT 1) END)
           ELSE COALESCE(
               (SELECT p.version_id FROM collect.schema_objects p
                WHERE p.server_id IS NULL AND p.ag_group_id = o.ag_group_id AND p.database_name = o.database_name AND p.object_id = o.object_id
                  AND p.valid_from <= o.valid_from AND p.version_id < o.version_id AND p.valid_to = o.valid_from
                ORDER BY p.version_id DESC LIMIT 1),
               CASE WHEN o.change_kind = 'altered' THEN
               (SELECT p.version_id FROM collect.schema_objects p
                WHERE p.server_id IS NULL AND p.ag_group_id = o.ag_group_id AND p.database_name = o.database_name
                  AND p.schema_name = o.schema_name AND p.object_name = o.object_name
                  AND p.version_id < o.version_id AND p.valid_to = o.valid_from
                ORDER BY p.version_id DESC LIMIT 1) END)
       END";

    /// <summary>Escapes the LIKE metacharacters (<c>\</c>, <c>%</c>, <c>_</c>) so the text matches as typed.</summary>
    public static string EscapeLike(string text) =>
        text.Replace("\\", "\\\\", StringComparison.Ordinal).Replace("%", "\\%", StringComparison.Ordinal).Replace("_", "\\_", StringComparison.Ordinal);

    private static NpgsqlCommand ChildCommand(NpgsqlConnection c, string sql, long[] ids)
    {
        var cmd = new NpgsqlCommand(sql, c) { CommandTimeout = StorageCommandDeadlines.McpReadSeconds };
        cmd.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Array | NpgsqlDbType.Bigint, Value = ids });
        return cmd;
    }

    private static List<T> Bucket<T>(Dictionary<long, List<T>> map, long id)
    {
        if (!map.TryGetValue(id, out var list))
        {
            map[id] = list = [];
        }

        return list;
    }

    private static DateTime Naive(DateTime value) => DateTime.SpecifyKind(value, DateTimeKind.Unspecified);

    private static string? Str(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetString(i);

    private static int? Int(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetInt32(i);

    private static bool? Bool(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetBoolean(i);

    private static decimal? Dec(NpgsqlDataReader r, int i) => r.IsDBNull(i) ? null : r.GetDecimal(i);
}
