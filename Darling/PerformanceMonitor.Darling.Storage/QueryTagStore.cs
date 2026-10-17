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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.Tags;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One query tag. <c>ParentId</c> null = a root tag; <c>Colour</c> null = a neutral pill.</summary>
public sealed record QueryTagRow(int Id, string Name, int? ParentId, int SortOrder, string? Colour);

/// <summary>One hand assignment (#5971). <c>Kind</c> is <c>hash</c> (the <c>QueryHash</c> is set) or <c>object</c> (database, schema and
/// object are set). <c>ServerId</c> null = every server. <c>QueryId</c> is a display key only.</summary>
public sealed record QueryTagAssignmentRow(
    int Id,
    int TagId,
    string Kind,
    int? ServerId,
    string? QueryHash,
    string? DatabaseName,
    string? SchemaName,
    string? ObjectName,
    long? QueryId,
    DateTime CreatedAt,
    string CreatedBy);

/// <summary>Every tag, every hand assignment, the rule count (all rules, and the enabled ones the resolver considers) per tag, the registered server names and the map state, read for one answer.</summary>
public sealed record QueryTagSnapshot(
    IReadOnlyList<QueryTagRow> Tags,
    IReadOnlyList<QueryTagAssignmentRow> Assignments,
    IReadOnlyDictionary<int, int> RuleCountByTag,
    IReadOnlyDictionary<int, int> EnabledRuleCountByTag,
    IReadOnlyDictionary<int, string> ServerNames,
    QueryTagState State);

/// <summary>What deleting a tag takes with it: its descendants, their hand assignments and their rules.</summary>
public sealed record QueryTagDeleteImpact(int ChildTags, int Assignments, int Rules);

/// <summary>The outcome of a structural tag write. Nothing is written for any case but <see cref="Ok"/>.</summary>
public abstract record QueryTagWriteResult
{
    private QueryTagWriteResult()
    {
    }

    /// <summary>The write committed. <c>Tag</c> is the row re-read after a create or update (null after a delete);
    /// <c>Removed</c> and <c>RemovedTagIds</c> describe a delete's cascade.</summary>
    public sealed record Ok(
        QueryTagRow? Tag,
        QueryTagDeleteImpact? Removed = null,
        IReadOnlyList<int>? RemovedTagIds = null) : QueryTagWriteResult;

    /// <summary>The tag (or, with <c>Code</c> <c>unknown_parent</c>, the requested parent) does not exist.</summary>
    public sealed record NotFound(string Message, string? Code = null) : QueryTagWriteResult;

    /// <summary>A sibling already carries the name (<c>duplicate_name</c>).</summary>
    public sealed record Conflict(string Message) : QueryTagWriteResult;

    /// <summary>A rule refused the write: <c>bad_name</c>, <c>bad_colour</c>, <c>depth_limit</c> or <c>cycle</c>.</summary>
    public sealed record Refused(string Code, string Message) : QueryTagWriteResult;

    /// <summary>An unconfirmed delete found child tags, assignments or rules in the subtree, counted under the delete lock. Nothing was deleted.</summary>
    public sealed record ConfirmRequired(QueryTagDeleteImpact Impact) : QueryTagWriteResult;

    /// <summary>Maps a shared-rules refusal to the matching result.</summary>
    public static QueryTagWriteResult From(TagRefusal refusal) => refusal.Kind switch
    {
        TagRefusalKind.Conflict => new Conflict(refusal.Message),
        TagRefusalKind.NotFound => new NotFound(refusal.Message, refusal.Code),
        _ => new Refused(refusal.Code ?? "invalid", refusal.Message),
    };
}

/// <summary>A hand assignment key: <c>hash</c> by <c>QueryHash</c>, or <c>object</c> by database, schema and object. <c>ServerId</c> null = every server.</summary>
public sealed record QueryTagKey(string Kind, int? ServerId, string? QueryHash, string? DatabaseName, string? SchemaName, string? ObjectName);

/// <summary>The outcome of assigning a key.</summary>
public enum QueryTagAssignOutcome
{
    /// <summary>The key had no hand tag; a row was added.</summary>
    Added,

    /// <summary>The key already had a hand tag in the same server scope; it moved to the new tag.</summary>
    Replaced,

    /// <summary>The key already carried this tag; nothing changed.</summary>
    Unchanged,

    /// <summary>The tag does not exist (it was deleted concurrently).</summary>
    TagGone,
}

/// <summary>The result of <see cref="QueryTagStore.AssignAsync"/>: the stored row and, for a replace, the tag it left.</summary>
public sealed record QueryTagAssignResult(QueryTagAssignOutcome Outcome, QueryTagAssignmentRow? Assignment, int? PreviousTagId);

/// <summary>
/// The query-tag store (V192 <c>config.query_tags</c> and <c>config.query_tag_assignments</c>, #5971) over Postgres: tags and hand
/// assignments. Every name, colour, depth, duplicate and cycle check is the shared <see cref="TagTreeRules"/>, run against a snapshot
/// read under a transaction-scoped advisory lock so two writers cannot both pass a check against a stale tree; the per-parent unique
/// index is the backstop (a folded-name collision under two different parents is legal, the index is per parent).
///
/// <para><b>One tag per key.</b> A hand assignment is unique per key and server scope (the V192 unique indexes), so assigning a key that
/// already has a hand tag in the same scope moves it, in one transaction. The map is not rebuilt here: a write to tags or assignments
/// bumps <c>config.query_tag_state.rules_version</c> through the V192 trigger, and the collector's next pass builds the map.</para>
///
/// <para>A 42501 (the seat cannot write) is never caught here; it propagates as <see cref="PostgresException"/>.</para>
/// </summary>
public sealed class QueryTagStore
{
    /// <summary>Every tag, flat, ordered by name.</summary>
    public const string SelectTagsSql = "SELECT id, name, parent_id, sort_order, colour FROM config.query_tags ORDER BY name, id";

    /// <summary>Every hand assignment, flat.</summary>
    public const string SelectAssignmentsSql = @"
SELECT id, tag_id, kind, server_id, query_hash, database_name, schema_name, object_name, query_id, created_at, created_by
FROM config.query_tag_assignments
ORDER BY tag_id, kind, server_id NULLS FIRST, query_hash, database_name, schema_name, object_name, id";

    /// <summary>The rules per tag, for the list and for a delete's confirm counts.</summary>
    public const string RuleCountsSql = "SELECT tag_id, count(*)::integer, (count(*) FILTER (WHERE enabled))::integer FROM config.query_tag_rules WHERE tag_id IS NOT NULL GROUP BY tag_id";

    /// <summary>The map version state.</summary>
    public const string StateSql = "SELECT rules_version, built_version FROM config.query_tag_state WHERE id = 1";

    /// <summary>The registry's server names, to show beside a server id.</summary>
    public const string ServerNamesSql = "SELECT server_id, server_name FROM servers";

    /// <summary>Which of the requested ids are registered servers. $1 ids.</summary>
    public const string RegisteredServerIdsSql = "SELECT server_id FROM servers WHERE server_id = ANY($1)";

    private const string InsertTagSql = "INSERT INTO config.query_tags (name, parent_id) VALUES ($1, $2) RETURNING id";
    private const string RenameSql = "UPDATE config.query_tags SET name = $2 WHERE id = $1";
    private const string SetColourSql = "UPDATE config.query_tags SET colour = $2 WHERE id = $1";
    private const string ReparentSql = "UPDATE config.query_tags SET parent_id = $2 WHERE id = $1";
    private const string DeleteTagSql = "DELETE FROM config.query_tags WHERE id = $1";
    private const string GetTagSql = "SELECT id, name, parent_id, sort_order, colour FROM config.query_tags WHERE id = $1";
    private const string LockSubtreeSql = "SELECT id FROM config.query_tags WHERE id = ANY($1) FOR UPDATE";
    private const string CountAssignmentsSql = "SELECT count(*)::integer FROM config.query_tag_assignments WHERE tag_id = ANY($1)";
    private const string CountRulesSql = "SELECT count(*)::integer FROM config.query_tag_rules WHERE tag_id = ANY($1)";

    /// <summary>The columns of one assignment, in the order <see cref="ReadAssignment"/> reads them.</summary>
    private const string AssignmentColumns =
        "id, tag_id, kind, server_id, query_hash, database_name, schema_name, object_name, query_id, created_at, created_by";

    /// <summary>The one row a key addresses in a server scope, locked for the write. $1 kind, $2 server_id (NULL = every server), $3 hash,
    /// $4 database, $5 schema, $6 object. The hash and the three object parts compare ignoring case (NULL parts as the empty string), because the
    /// resolver matches a hash with OrdinalIgnoreCase and an object through SameObjectAs (#6020); the V192 unique indexes stay exact, and this find under
    /// <see cref="AssignLockKey"/> is what keeps a case-twin row from being written. The assignments table is small and hand-made, so no index is needed.</summary>
    private const string FindAssignmentSql = @"
SELECT " + AssignmentColumns + @"
FROM config.query_tag_assignments
WHERE kind = $1
  AND server_id IS NOT DISTINCT FROM $2
  AND lower(COALESCE(query_hash, '')) = lower(COALESCE($3, ''))
  AND lower(COALESCE(database_name, '')) = lower(COALESCE($4, ''))
  AND lower(COALESCE(schema_name, '')) = lower(COALESCE($5, ''))
  AND lower(COALESCE(object_name, '')) = lower(COALESCE($6, ''))
FOR UPDATE";

    private const string InsertAssignmentSql = @"
INSERT INTO config.query_tag_assignments (tag_id, kind, server_id, query_hash, database_name, schema_name, object_name, query_id, created_by)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
RETURNING " + AssignmentColumns;

    private const string MoveAssignmentSql = @"
UPDATE config.query_tag_assignments SET tag_id = $2, query_id = COALESCE($3, query_id) WHERE id = $1
RETURNING " + AssignmentColumns;

    private const string DeleteAssignmentSql = "DELETE FROM config.query_tag_assignments WHERE id = $1";

    /// <summary>Serializes structural tag writes (create / update / delete). $1 key.</summary>
    public const string AcquireLockSql = "SELECT pg_advisory_xact_lock($1)";

    /// <summary>The advisory-lock key for structural query-tag writes ("DARLQTAG"). Distinct from the server-tag key, the migration lock
    /// and the custom-alert-rule cap lock.</summary>
    public const long TreeLockKey = 0x4441524C_51544147;

    /// <summary>The advisory-lock key for hand-assignment writes ("DARLQASG"): the find-then-write of one key runs under it. An assign or
    /// unassign takes <see cref="TreeLockKey"/> first and this second, always in that order, so a delete's confirm counts (taken under
    /// <see cref="TreeLockKey"/>) cannot miss an assignment written meanwhile (#6020).</summary>
    public const long AssignLockKey = 0x4441524C_51415347;

    private const string UniqueViolationSqlState = "23505";
    private const string ForeignKeyViolationSqlState = "23503";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    /// <summary>Builds the store over a data source whose search_path reaches the <c>config</c> schema.</summary>
    public QueryTagStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <summary>Maps the store's rows to the shared rules' nodes.</summary>
    public static IReadOnlyList<TagTreeNode> Nodes(IReadOnlyList<QueryTagRow> tags) =>
        tags.Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();

    /// <summary>Reads tags, hand assignments, rule counts, server names and the map state.</summary>
    public async Task<QueryTagSnapshot> ReadSnapshotAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        var tags = await ReadTagsCoreAsync(connection, null, ct);

        var assignments = new List<QueryTagAssignmentRow>();
        await using (var command = Command(SelectAssignmentsSql, connection, null))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                assignments.Add(ReadAssignment(reader));
            }
        }

        var ruleCounts = new Dictionary<int, int>();
        var enabledRuleCounts = new Dictionary<int, int>();
        await using (var command = Command(RuleCountsSql, connection, null))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                ruleCounts[reader.GetInt32(0)] = reader.GetInt32(1);
                enabledRuleCounts[reader.GetInt32(0)] = reader.GetInt32(2);
            }
        }

        var serverNames = new Dictionary<int, string>();
        await using (var command = Command(ServerNamesSql, connection, null))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            while (await reader.ReadAsync(ct))
            {
                serverNames[reader.GetInt32(0)] = reader.GetString(1);
            }
        }

        QueryTagState state;
        await using (var command = Command(StateSql, connection, null))
        await using (var reader = await command.ExecuteReaderAsync(ct))
        {
            state = await reader.ReadAsync(ct) ? new QueryTagState(reader.GetInt64(0), reader.GetInt64(1)) : new QueryTagState(0, 0);
        }

        return new QueryTagSnapshot(tags, assignments, ruleCounts, enabledRuleCounts, serverNames, state);
    }

    /// <summary>Returns the subset of <paramref name="serverIds"/> that are registered servers.</summary>
    public async Task<IReadOnlySet<int>> FindRegisteredServerIdsAsync(IReadOnlyCollection<int> serverIds, CancellationToken ct = default)
    {
        var found = new HashSet<int>();
        if (serverIds.Count == 0)
        {
            return found;
        }

        await using var command = _dataSource.CreateCommand(RegisteredServerIdsSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = serverIds.ToArray() });
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            found.Add(reader.GetInt32(0));
        }

        return found;
    }

    /// <summary>Creates a tag. A null <paramref name="colour"/> stamps the palette colour for the new id.</summary>
    public async Task<QueryTagWriteResult> CreateAsync(string? name, int? parentId, string? colour, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        try
        {
            await AcquireLockAsync(connection, transaction, TreeLockKey, ct);
            var tags = await ReadTagsCoreAsync(connection, transaction, ct);
            var refusal = TagTreeRules.CheckCreate(Nodes(tags), name, parentId, colour, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return QueryTagWriteResult.From(refusal);
            }

            int id;
            await using (var insert = Command(InsertTagSql, connection, transaction))
            {
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = cleanName });
                insert.Parameters.Add(NullableInt(parentId));
                id = (int)(await insert.ExecuteScalarAsync(ct))!;
            }

            await SetColourCoreAsync(connection, transaction, id, cleanColour ?? TagColours.ForTagId(id), ct);
            var row = await GetTagCoreAsync(connection, transaction, id, ct);
            await transaction.CommitAsync(ct);
            return new QueryTagWriteResult.Ok(row);
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolationSqlState)
        {
            return new QueryTagWriteResult.Conflict(TagTreeRules.ConcurrentDuplicateMessage(name?.Trim()));
        }
    }

    /// <summary>Applies a partial edit (rename, colour, move) to one tag.</summary>
    public async Task<QueryTagWriteResult> UpdateAsync(int tagId, TagTreeEdit edit, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        try
        {
            await AcquireLockAsync(connection, transaction, TreeLockKey, ct);
            var tags = await ReadTagsCoreAsync(connection, transaction, ct);
            var refusal = TagTreeRules.CheckUpdate(Nodes(tags), tagId, edit, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return QueryTagWriteResult.From(refusal);
            }

            if (edit.HasName)
            {
                await using var rename = Command(RenameSql, connection, transaction);
                rename.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
                rename.Parameters.Add(new NpgsqlParameter<string> { TypedValue = cleanName! });
                await rename.ExecuteNonQueryAsync(ct);
            }

            if (edit.HasColour)
            {
                await SetColourCoreAsync(connection, transaction, tagId, cleanColour, ct);
            }

            if (edit.HasParent)
            {
                await using var reparent = Command(ReparentSql, connection, transaction);
                reparent.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
                reparent.Parameters.Add(NullableInt(edit.ParentId));
                await reparent.ExecuteNonQueryAsync(ct);
            }

            var row = await GetTagCoreAsync(connection, transaction, tagId, ct);
            await transaction.CommitAsync(ct);
            return new QueryTagWriteResult.Ok(row);
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolationSqlState)
        {
            return new QueryTagWriteResult.Conflict(TagTreeRules.ConcurrentDuplicateMessage(edit.HasName ? edit.Name?.Trim() : null));
        }
    }

    /// <summary>Deletes a tag and its whole subtree, with their hand assignments and rules. Without <paramref name="confirm"/>, a subtree
    /// that holds child tags, assignments or rules (counted under the delete lock) refuses with
    /// <see cref="QueryTagWriteResult.ConfirmRequired"/> and nothing changes.</summary>
    public async Task<QueryTagWriteResult> DeleteAsync(int tagId, bool confirm, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await AcquireLockAsync(connection, transaction, TreeLockKey, ct);
        var tags = await ReadTagsCoreAsync(connection, transaction, ct);
        var nodes = Nodes(tags);
        var refusal = TagTreeRules.CheckDelete(nodes, tagId);
        if (refusal is not null)
        {
            return QueryTagWriteResult.From(refusal);
        }

        var descendants = TagTreeRules.Descendants(nodes, tagId);
        var subtree = descendants.Append(tagId).ToArray();

        // Hold the subtree's tag rows, so a rule written by a writer that does not take TreeLockKey cannot slip a row in
        // between the counts below and the DELETE: its foreign-key check needs a share lock on the tag row (#6020).
        await using (var hold = Command(LockSubtreeSql, connection, transaction))
        {
            hold.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = subtree });
            await hold.ExecuteNonQueryAsync(ct);
        }

        var impact = new QueryTagDeleteImpact(
            descendants.Count,
            await CountAsync(CountAssignmentsSql, subtree, connection, transaction, ct),
            await CountAsync(CountRulesSql, subtree, connection, transaction, ct));
        if (!confirm && (impact.ChildTags > 0 || impact.Assignments > 0 || impact.Rules > 0))
        {
            return new QueryTagWriteResult.ConfirmRequired(impact);
        }

        await using (var delete = Command(DeleteTagSql, connection, transaction))
        {
            delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
            await delete.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return new QueryTagWriteResult.Ok(null, impact, subtree.OrderBy(i => i).ToList());
    }

    /// <summary>Gives a key its hand tag. One tag per key and server scope: a key that already has a hand tag in the scope moves to
    /// <paramref name="tagId"/> (<see cref="QueryTagAssignOutcome.Replaced"/>); one that already has this tag is
    /// <see cref="QueryTagAssignOutcome.Unchanged"/>.</summary>
    public async Task<QueryTagAssignResult> AssignAsync(int tagId, QueryTagKey key, long? queryId, string createdBy, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await AcquireLockAsync(connection, transaction, TreeLockKey, ct);
        await AcquireLockAsync(connection, transaction, AssignLockKey, ct);
        try
        {
            QueryTagAssignmentRow? existing;
            await using (var find = Command(FindAssignmentSql, connection, transaction))
            {
                AddKeyParameters(find, key);
                await using var reader = await find.ExecuteReaderAsync(ct);
                existing = await reader.ReadAsync(ct) ? ReadAssignment(reader) : null;
            }

            if (existing is not null && existing.TagId == tagId)
            {
                return new QueryTagAssignResult(QueryTagAssignOutcome.Unchanged, existing, null);
            }

            if (existing is not null)
            {
                await using var move = Command(MoveAssignmentSql, connection, transaction);
                move.Parameters.Add(new NpgsqlParameter<int> { TypedValue = existing.Id });
                move.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
                move.Parameters.Add(NullableLong(queryId));
                QueryTagAssignmentRow moved;
                await using (var reader = await move.ExecuteReaderAsync(ct))
                {
                    await reader.ReadAsync(ct);
                    moved = ReadAssignment(reader);
                }

                await transaction.CommitAsync(ct);
                return new QueryTagAssignResult(QueryTagAssignOutcome.Replaced, moved, existing.TagId);
            }

            await using var insert = Command(InsertAssignmentSql, connection, transaction);
            insert.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Kind });
            insert.Parameters.Add(NullableInt(key.ServerId));
            insert.Parameters.Add(NullableText(key.QueryHash));
            insert.Parameters.Add(NullableText(key.DatabaseName));
            insert.Parameters.Add(NullableText(key.SchemaName));
            insert.Parameters.Add(NullableText(key.ObjectName));
            insert.Parameters.Add(NullableLong(queryId));
            insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = createdBy });
            QueryTagAssignmentRow added;
            await using (var reader = await insert.ExecuteReaderAsync(ct))
            {
                await reader.ReadAsync(ct);
                added = ReadAssignment(reader);
            }

            await transaction.CommitAsync(ct);
            return new QueryTagAssignResult(QueryTagAssignOutcome.Added, added, null);
        }
        catch (PostgresException ex) when (ex.SqlState == ForeignKeyViolationSqlState)
        {
            // The tag was deleted between the caller's read and this write.
            return new QueryTagAssignResult(QueryTagAssignOutcome.TagGone, null, null);
        }
    }

    /// <summary>Removes the hand tag from a key in exactly one server scope. Returns the removed row, or null when the key had none.</summary>
    public async Task<QueryTagAssignmentRow?> UnassignAsync(QueryTagKey key, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await AcquireLockAsync(connection, transaction, TreeLockKey, ct);
        await AcquireLockAsync(connection, transaction, AssignLockKey, ct);
        QueryTagAssignmentRow? existing;
        await using (var find = Command(FindAssignmentSql, connection, transaction))
        {
            AddKeyParameters(find, key);
            await using var reader = await find.ExecuteReaderAsync(ct);
            existing = await reader.ReadAsync(ct) ? ReadAssignment(reader) : null;
        }

        if (existing is null)
        {
            return null;
        }

        await using (var delete = Command(DeleteAssignmentSql, connection, transaction))
        {
            delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = existing.Id });
            await delete.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return existing;
    }

    private static void AddKeyParameters(NpgsqlCommand command, QueryTagKey key)
    {
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = key.Kind });
        command.Parameters.Add(NullableInt(key.ServerId));
        command.Parameters.Add(NullableText(key.QueryHash));
        command.Parameters.Add(NullableText(key.DatabaseName));
        command.Parameters.Add(NullableText(key.SchemaName));
        command.Parameters.Add(NullableText(key.ObjectName));
    }

    private NpgsqlCommand Command(string sql, NpgsqlConnection connection, NpgsqlTransaction? transaction) =>
        new(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };

    private async Task AcquireLockAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, long key, CancellationToken ct)
    {
        await using var command = Command(AcquireLockSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = key });
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<int> CountAsync(string sql, int[] tagIds, NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var command = Command(sql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = tagIds });
        return (int)(await command.ExecuteScalarAsync(ct))!;
    }

    private async Task<List<QueryTagRow>> ReadTagsCoreAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        var tags = new List<QueryTagRow>();
        await using var command = Command(SelectTagsSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            tags.Add(ReadTag(reader));
        }

        return tags;
    }

    private async Task<QueryTagRow?> GetTagCoreAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int id, CancellationToken ct)
    {
        await using var command = Command(GetTagSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadTag(reader) : null;
    }

    private async Task SetColourCoreAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int id, string? colour, CancellationToken ct)
    {
        await using var command = Command(SetColourSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        command.Parameters.Add(NullableText(colour));
        await command.ExecuteNonQueryAsync(ct);
    }

    private static QueryTagRow ReadTag(NpgsqlDataReader reader) =>
        new(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.GetInt32(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));

    private static QueryTagAssignmentRow ReadAssignment(NpgsqlDataReader reader) =>
        new(
            reader.GetInt32(0),
            reader.GetInt32(1),
            reader.GetString(2),
            reader.IsDBNull(3) ? null : reader.GetInt32(3),
            reader.IsDBNull(4) ? null : reader.GetString(4),
            reader.IsDBNull(5) ? null : reader.GetString(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8),
            reader.GetFieldValue<DateTime>(9),
            reader.GetString(10));

    private static NpgsqlParameter NullableInt(int? value) =>
        value is int v ? new NpgsqlParameter<int> { TypedValue = v } : new NpgsqlParameter { Value = DBNull.Value };

    private static NpgsqlParameter NullableLong(long? value) =>
        value is long v ? new NpgsqlParameter<long> { TypedValue = v } : new NpgsqlParameter { Value = DBNull.Value };

    private static NpgsqlParameter NullableText(string? value) =>
        value is null ? new NpgsqlParameter { Value = DBNull.Value } : new NpgsqlParameter<string> { TypedValue = value };
}
