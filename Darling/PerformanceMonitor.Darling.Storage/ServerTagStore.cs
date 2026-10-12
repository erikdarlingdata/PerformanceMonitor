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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>One tag row. <c>ParentId</c> null = a root tag. <c>Colour</c> null = no colour (a neutral pill).</summary>
public sealed record ServerTagRow(int Id, string Name, int? ParentId, int SortOrder, string? Colour);

/// <summary>One server-to-tag assignment. Many-to-many: a server carries any number of tags.</summary>
public sealed record ServerTagAssignmentRow(int ServerId, int TagId);

/// <summary>A custom alert rule whose scope is a fleet tag (<c>definition.scope = {"mode":"tag","tagId":N}</c>).
/// Disabled rules are included: they come back into force when enabled.</summary>
public sealed record TagScopedRule(long RuleId, string Name, bool Enabled, int ScopeTagId);

/// <summary>Every tag, every assignment and every tag-scoped custom alert rule, read for one decision.</summary>
public sealed record ServerTagSnapshot(
    IReadOnlyList<ServerTagRow> Tags,
    IReadOnlyList<ServerTagAssignmentRow> Assignments,
    IReadOnlyList<TagScopedRule> Rules);

/// <summary>A partial edit of one tag. A <c>Has*</c> flag false means the field was not sent. <c>ParentId</c>
/// null with <c>HasParent</c> moves the tag to the root; <c>Colour</c> null with <c>HasColour</c> clears it.</summary>
public sealed record ServerTagEdit(
    bool HasName, string? Name,
    bool HasColour, string? Colour,
    bool HasParent, int? ParentId);

/// <summary>The outcome of a structural tag write. Nothing is written for any case but <see cref="Ok"/>.</summary>
public abstract record ServerTagWriteResult
{
    private ServerTagWriteResult()
    {
    }

    /// <summary>The write committed. <c>Tag</c> is the row re-read after a create or update (null after a
    /// delete); <c>RemovedAssignments</c> and <c>RemovedTagIds</c> describe a delete's cascade.</summary>
    public sealed record Ok(
        ServerTagRow? Tag,
        int RemovedAssignments = 0,
        IReadOnlyList<int>? RemovedTagIds = null) : ServerTagWriteResult;

    /// <summary>The tag (or, with <c>Code</c> <c>unknown_parent</c>, the requested parent) does not exist.</summary>
    public sealed record NotFound(string Message, string? Code = null) : ServerTagWriteResult;

    /// <summary>A sibling already carries the name (<c>duplicate_name</c>).</summary>
    public sealed record Conflict(string Message) : ServerTagWriteResult;

    /// <summary>A rule refused the write: <c>bad_name</c>, <c>bad_colour</c>, <c>depth_limit</c> or <c>cycle</c>.</summary>
    public sealed record Refused(string Code, string Message) : ServerTagWriteResult;

    /// <summary>A delete that was not confirmed found custom alert rules scoped into the subtree, read under the
    /// delete lock. Nothing was deleted.</summary>
    public sealed record ConfirmRequired(IReadOnlyList<TagScopedRule> Rules, string Message) : ServerTagWriteResult;
}

/// <summary>Thrown by <see cref="ServerTagStore.AssignAsync"/> when the tag no longer exists by the time the
/// insert runs (a foreign-key violation): the tag was deleted concurrently.</summary>
public sealed class ServerTagGoneException : Exception
{
    /// <summary>Creates the exception for <paramref name="tagId"/>.</summary>
    public ServerTagGoneException(int tagId)
        : base(string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} no longer exists."))
    {
        TagId = tagId;
    }

    /// <summary>The tag that is gone.</summary>
    public int TagId { get; }
}

/// <summary>The fleet-tag store surface shared by the viewer and the MCP tools.</summary>
public interface IServerTagStore
{
    /// <summary>Reads every tag, assignment and tag-scoped custom alert rule.</summary>
    Task<ServerTagSnapshot> ReadSnapshotAsync(CancellationToken ct = default);

    /// <summary>Returns the subset of <paramref name="serverIds"/> that are registered servers.</summary>
    Task<IReadOnlySet<int>> FindRegisteredServerIdsAsync(IReadOnlyCollection<int> serverIds, CancellationToken ct = default);

    /// <summary>Creates a tag. A null <paramref name="colour"/> stamps the palette colour for the new id.</summary>
    Task<ServerTagWriteResult> CreateAsync(string name, int? parentId, string? colour, CancellationToken ct = default);

    /// <summary>Applies a partial edit (rename, colour, move) to one tag.</summary>
    Task<ServerTagWriteResult> UpdateAsync(int tagId, ServerTagEdit edit, CancellationToken ct = default);

    /// <summary>Deletes a tag and its whole subtree and their assignments. Without <paramref name="confirm"/>, a
    /// custom alert rule scoped into the subtree (read under the delete lock) refuses the delete with
    /// <see cref="ServerTagWriteResult.ConfirmRequired"/>.</summary>
    Task<ServerTagWriteResult> DeleteAsync(int tagId, bool confirm, CancellationToken ct = default);

    /// <summary>Assigns a tag to servers; returns the ids actually inserted.</summary>
    Task<IReadOnlyList<int>> AssignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default);

    /// <summary>Removes a tag from servers; returns the ids actually removed.</summary>
    Task<IReadOnlyList<int>> UnassignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default);

    /// <summary>Whether a tag has child tags, read fresh.</summary>
    Task<bool> HasChildrenAsync(int tagId, CancellationToken ct = default);

    /// <summary>Drops every tag assignment for a removed server.</summary>
    Task ClearForServerAsync(int serverId, CancellationToken ct = default);
}

/// <summary>
/// The fleet-tag store (V32 <c>config.server_tags</c> + <c>config.server_tag_map</c>) over Postgres. The viewer
/// and the MCP tools both write through it, so the depth, cycle, colour and name rules in
/// <see cref="ServerTagRules"/> hold for every writer.
///
/// <para><b>Structural writes are serialized.</b> Create, update and delete each run in one transaction that
/// first takes a transaction-scoped advisory lock, re-read the tags under it, run the rules against that
/// snapshot and only then write. Two writers therefore cannot both pass a duplicate-name or depth check against
/// the same stale tree. A 23505 from the per-parent unique index is the backstop for a writer that does not take
/// the lock (an older viewer build), and for the cases where <c>lower()</c> and
/// <c>ToLowerInvariant</c> disagree. Assign and unassign are single statements: the map has no structure to
/// protect.</para>
///
/// <para>A 42501 (the seat cannot write) is never caught here; it propagates as <see cref="PostgresException"/>
/// so each surface can map it to its own read-only message.</para>
///
/// <para>Bare table names resolve through the connection's search_path to the <c>config</c> schema, the same as
/// every other config write.</para>
/// </summary>
public sealed class ServerTagStore : IServerTagStore
{
    /// <summary>Every tag, flat. The caller builds the tree. Ordered by name so sibling order is stable
    /// without a reordering UI.</summary>
    public const string SelectTagsSql =
        "SELECT id, name, parent_id, sort_order, colour FROM server_tags ORDER BY name";

    /// <summary>Every server-to-tag assignment, flat.</summary>
    public const string SelectAssignmentsSql =
        "SELECT server_id, tag_id FROM server_tag_map";

    /// <summary>Creates a tag. $1 name, $2 parent_id (NULL = root). Returns the new id. A duplicate name
    /// under the same parent violates the per-parent unique index and surfaces as 23505.</summary>
    public const string InsertSql =
        "INSERT INTO server_tags (name, parent_id) VALUES ($1, $2) RETURNING id";

    /// <summary>Renames a tag. $1 id, $2 name.</summary>
    public const string RenameSql =
        "UPDATE server_tags SET name = $2 WHERE id = $1";

    /// <summary>Sets (or clears, with NULL) a tag's colour. $1 id, $2 colour (<c>#RRGGBB</c> or NULL).</summary>
    public const string SetColourSql =
        "UPDATE server_tags SET colour = $2 WHERE id = $1";

    /// <summary>Reparents a tag. $1 id, $2 new parent_id (NULL = promote to root). The caller must have
    /// already rejected cycles and depth-cap violations against its in-memory tree.</summary>
    public const string ReparentSql =
        "UPDATE server_tags SET parent_id = $2 WHERE id = $1";

    /// <summary>Deletes a tag. $1 id. The self-referencing FK cascades to descendants and the map cascades
    /// to their assignments — it can never reach <c>config_monitored_servers</c>, so no server, credential
    /// or collected history is touched. Callers still refuse the delete when the tag has children, and
    /// offer to move them first, rather than silently removing a subtree.</summary>
    public const string DeleteSql =
        "DELETE FROM server_tags WHERE id = $1";

    /// <summary>Whether a tag has child tags. $1 id. Read immediately before a delete, because the tag
    /// tables are SHARED and this viewer's snapshot may be up to one refresh interval stale.</summary>
    public const string HasChildrenSql =
        "SELECT EXISTS (SELECT 1 FROM server_tags WHERE parent_id = $1)";

    /// <summary>Assigns a tag to many servers in ONE statement. $1 server ids, $2 tag id. Bulk by design:
    /// tagging 100 servers row-by-row would be 100 round trips. Already-assigned servers are no-ops, and
    /// only the rows actually inserted come back.</summary>
    public const string AssignSql =
        "INSERT INTO server_tag_map (server_id, tag_id) SELECT unnest($1), $2 ON CONFLICT DO NOTHING RETURNING server_id";

    /// <summary>Removes a tag from many servers in one statement. $1 server ids, $2 tag id. Only the rows
    /// actually removed come back.</summary>
    public const string UnassignSql =
        "DELETE FROM server_tag_map WHERE server_id = ANY($1) AND tag_id = $2 RETURNING server_id";

    /// <summary>Drops every tag assignment for a server. $1 server_id. Called when a server is REMOVED:
    /// <c>server_id</c> is a deterministic hash of host+database+read-only-intent, so leaving orphaned map
    /// rows behind would make a removed-then-re-added server silently RESURRECT its old tags.</summary>
    public const string ClearForServerSql =
        "DELETE FROM server_tag_map WHERE server_id = $1";

    /// <summary>One tag by id, re-read after every write. $1 id.</summary>
    public const string GetSql = @"
SELECT id, name, parent_id, sort_order, colour
FROM server_tags
WHERE id = $1";

    /// <summary>Every custom alert rule scoped to a fleet tag (#3350/#3367). The CASE guards the cast, so a
    /// hand-edited non-numeric tagId reads as NULL instead of failing the whole read. No parameters.</summary>
    public const string TagScopedRulesSql = @"
SELECT
    r.id,
    r.name,
    r.enabled,
    CASE
        WHEN r.definition->'scope'->>'tagId' ~ '^[0-9]{1,9}$'
        THEN (r.definition->'scope'->>'tagId')::integer
    END AS scope_tag_id
FROM custom_alert_rules AS r
WHERE r.definition->'scope'->>'mode' = 'tag'
ORDER BY r.id";

    /// <summary>Which of the requested ids are registered servers; the same registry
    /// MonitoredServerDisplayNameAsync reads. $1 ids.</summary>
    public const string RegisteredServerIdsSql = @"
SELECT server_id
FROM servers
WHERE server_id = ANY($1)";

    /// <summary>Serializes structural tag writes (create / update / delete). $1 key.</summary>
    public const string AcquireTreeLockSql = "SELECT pg_advisory_xact_lock($1)";

    /// <summary>The advisory-lock key for structural tag writes ("DARLTAGS"). Distinct from the migration lock
    /// and the custom-alert-rule cap lock, so the three never collide in Postgres's 64-bit key space.</summary>
    public const long TreeLockKey = 0x4441524C_54414753;

    private const string UniqueViolationSqlState = "23505";
    private const string ForeignKeyViolationSqlState = "23503";

    private readonly NpgsqlDataSource _dataSource;
    private readonly int _commandTimeoutSeconds;

    /// <summary>Builds the store over a data source whose search_path reaches the <c>config</c> schema.
    /// <paramref name="commandTimeoutSeconds"/> is applied to every command.</summary>
    public ServerTagStore(NpgsqlDataSource dataSource, int commandTimeoutSeconds)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        _commandTimeoutSeconds = commandTimeoutSeconds;
    }

    /// <inheritdoc />
    public async Task<ServerTagSnapshot> ReadSnapshotAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        var tags = await ReadTagsCoreAsync(connection, null, ct);
        var assignments = await ReadAssignmentsCoreAsync(connection, null, ct);
        var rules = await ReadRulesCoreAsync(connection, null, ct);
        return new ServerTagSnapshot(tags, assignments, rules);
    }

    /// <summary>Reads every tag, flat, without the assignments or rules.</summary>
    public async Task<IReadOnlyList<ServerTagRow>> ReadTagsAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        return await ReadTagsCoreAsync(connection, null, ct);
    }

    /// <summary>Reads every server-to-tag assignment, flat.</summary>
    public async Task<IReadOnlyList<ServerTagAssignmentRow>> ReadAssignmentsAsync(CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        return await ReadAssignmentsCoreAsync(connection, null, ct);
    }

    /// <inheritdoc />
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

    /// <inheritdoc />
    public async Task<ServerTagWriteResult> CreateAsync(string name, int? parentId, string? colour, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        try
        {
            await AcquireLockAsync(connection, transaction, ct);
            var tags = await ReadTagsCoreAsync(connection, transaction, ct);

            var refusal = ServerTagRules.CheckCreate(tags, name, parentId, colour, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return refusal;
            }

            int id;
            await using (var insert = Command(InsertSql, connection, transaction))
            {
                insert.Parameters.Add(new NpgsqlParameter<string> { TypedValue = cleanName });
                insert.Parameters.Add(NullableInt(parentId));
                id = (int)(await insert.ExecuteScalarAsync(ct))!;
            }

            await SetColourCoreAsync(connection, transaction, id, cleanColour ?? TagColours.ForTagId(id), ct);
            var row = await GetCoreAsync(connection, transaction, id, ct);
            await transaction.CommitAsync(ct);
            return new ServerTagWriteResult.Ok(row);
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolationSqlState)
        {
            return new ServerTagWriteResult.Conflict(ServerTagRules.ConcurrentDuplicateMessage(name.Trim()));
        }
    }

    /// <inheritdoc />
    public async Task<ServerTagWriteResult> UpdateAsync(int tagId, ServerTagEdit edit, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);
        try
        {
            await AcquireLockAsync(connection, transaction, ct);
            var tags = await ReadTagsCoreAsync(connection, transaction, ct);

            var refusal = ServerTagRules.CheckUpdate(tags, tagId, edit, out var cleanName, out var cleanColour);
            if (refusal is not null)
            {
                return refusal;
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

            var row = await GetCoreAsync(connection, transaction, tagId, ct);
            await transaction.CommitAsync(ct);
            return new ServerTagWriteResult.Ok(row);
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolationSqlState)
        {
            var intended = edit.HasName ? edit.Name?.Trim() : null;
            return new ServerTagWriteResult.Conflict(ServerTagRules.ConcurrentDuplicateMessage(intended));
        }
    }

    /// <inheritdoc />
    public async Task<ServerTagWriteResult> DeleteAsync(int tagId, bool confirm, CancellationToken ct = default)
    {
        await using var connection = await _dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await AcquireLockAsync(connection, transaction, ct);
        var tags = await ReadTagsCoreAsync(connection, transaction, ct);
        var refusal = ServerTagRules.CheckDelete(tags, tagId);
        if (refusal is not null)
        {
            return refusal;
        }

        var subtree = ServerTagRules.Descendants(tags, tagId);
        subtree.Add(tagId);
        if (!confirm)
        {
            var scoped = (await ReadRulesCoreAsync(connection, transaction, ct)).Where(r => subtree.Contains(r.ScopeTagId)).ToList();
            if (scoped.Count > 0)
            {
                return new ServerTagWriteResult.ConfirmRequired(
                    scoped,
                    string.Create(CultureInfo.InvariantCulture, $"Deleting tag {tagId} and its subtree would leave {scoped.Count} custom alert rule(s) scoped to it matching no server: {string.Join(", ", scoped.Select(r => $"'{r.Name}'"))}. Nothing was deleted. Repeat with confirm=true to proceed."));
            }
        }

        var assignments = await ReadAssignmentsCoreAsync(connection, transaction, ct);
        var removedAssignments = assignments.Count(a => subtree.Contains(a.TagId));

        await using (var delete = Command(DeleteSql, connection, transaction))
        {
            delete.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
            await delete.ExecuteNonQueryAsync(ct);
        }

        await transaction.CommitAsync(ct);
        return new ServerTagWriteResult.Ok(null, removedAssignments, subtree.OrderBy(i => i).ToList());
    }

    /// <inheritdoc />
    public Task<IReadOnlyList<int>> AssignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default) =>
        ChangeAssignmentsAsync(AssignSql, tagId, serverIds, ct);

    /// <inheritdoc />
    public Task<IReadOnlyList<int>> UnassignAsync(int tagId, IReadOnlyList<int> serverIds, CancellationToken ct = default) =>
        ChangeAssignmentsAsync(UnassignSql, tagId, serverIds, ct);

    /// <inheritdoc />
    public async Task<bool> HasChildrenAsync(int tagId, CancellationToken ct = default)
    {
        await using var command = _dataSource.CreateCommand(HasChildrenSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        return (bool)(await command.ExecuteScalarAsync(ct))!;
    }

    /// <inheritdoc />
    public async Task ClearForServerAsync(int serverId, CancellationToken ct = default)
    {
        await using var command = _dataSource.CreateCommand(ClearForServerSql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<IReadOnlyList<int>> ChangeAssignmentsAsync(string sql, int tagId, IReadOnlyList<int> serverIds, CancellationToken ct)
    {
        var changed = new List<int>();
        if (serverIds.Count == 0)
        {
            return changed;
        }

        await using var command = _dataSource.CreateCommand(sql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = serverIds.ToArray() });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        try
        {
            await using var reader = await command.ExecuteReaderAsync(ct);
            while (await reader.ReadAsync(ct))
            {
                changed.Add(reader.GetInt32(0));
            }
        }
        catch (PostgresException ex) when (ex.SqlState == ForeignKeyViolationSqlState && sql == AssignSql)
        {
            // The tag was deleted between the caller's read and this insert.
            throw new ServerTagGoneException(tagId);
        }

        changed.Sort();
        return changed;
    }

    private NpgsqlCommand Command(string sql, NpgsqlConnection connection, NpgsqlTransaction? transaction) =>
        new(sql, connection, transaction) { CommandTimeout = _commandTimeoutSeconds };

    private async Task AcquireLockAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, CancellationToken ct)
    {
        await using var command = Command(AcquireTreeLockSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = TreeLockKey });
        await command.ExecuteNonQueryAsync(ct);
    }

    private async Task<List<ServerTagRow>> ReadTagsCoreAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        var tags = new List<ServerTagRow>();
        await using var command = Command(SelectTagsSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            tags.Add(ReadRow(reader));
        }

        return tags;
    }

    private async Task<List<ServerTagAssignmentRow>> ReadAssignmentsCoreAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        var assignments = new List<ServerTagAssignmentRow>();
        await using var command = Command(SelectAssignmentsSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            assignments.Add(new ServerTagAssignmentRow(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return assignments;
    }

    private async Task<List<TagScopedRule>> ReadRulesCoreAsync(NpgsqlConnection connection, NpgsqlTransaction? transaction, CancellationToken ct)
    {
        var rules = new List<TagScopedRule>();
        await using var command = Command(TagScopedRulesSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
        {
            if (reader.IsDBNull(3))
            {
                continue;
            }

            rules.Add(new TagScopedRule(reader.GetInt64(0), reader.GetString(1), reader.GetBoolean(2), reader.GetInt32(3)));
        }

        return rules;
    }

    private async Task<ServerTagRow?> GetCoreAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int id, CancellationToken ct)
    {
        await using var command = Command(GetSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(ct);
        return await reader.ReadAsync(ct) ? ReadRow(reader) : null;
    }

    private async Task SetColourCoreAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, int id, string? colour, CancellationToken ct)
    {
        await using var command = Command(SetColourSql, connection, transaction);
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = id });
        command.Parameters.Add(colour is null
            ? new NpgsqlParameter { Value = DBNull.Value }
            : new NpgsqlParameter<string> { TypedValue = colour });
        await command.ExecuteNonQueryAsync(ct);
    }

    private static ServerTagRow ReadRow(NpgsqlDataReader reader) =>
        new(
            reader.GetInt32(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetInt32(2),
            reader.GetInt32(3),
            reader.IsDBNull(4) ? null : reader.GetString(4));

    private static NpgsqlParameter NullableInt(int? value)
    {
        if (value is int v)
        {
            return new NpgsqlParameter<int> { TypedValue = v };
        }

        return new NpgsqlParameter { Value = DBNull.Value };
    }
}
