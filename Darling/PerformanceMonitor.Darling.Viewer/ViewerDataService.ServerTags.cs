/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Ui;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>One tag row. <c>ParentId</c> null = a root tag. <c>Colour</c> null = no colour (a neutral pill);
/// otherwise a <c>#RRGGBB</c> string, auto-assigned from <see cref="TagColorPalette"/> at creation and
/// user-overridable.</summary>
public sealed record DarlingTag(int Id, string Name, int? ParentId, int SortOrder, string? Colour = null);

/// <summary>One server-to-tag assignment. Many-to-many: a server carries any number of tags.</summary>
public sealed record DarlingTagAssignment(int ServerId, int TagId);

/// <summary>
/// The fleet-tag store surface (V32 <c>config.server_tags</c> + <c>config.server_tag_map</c>): the
/// viewer's user-authored visual organization of a large server list.
///
/// <para><b>Direct writes, not the command queue.</b> The viewer's default seat is <c>admin</c>, which
/// holds write on the config tables, and it already writes <c>config_monitored_servers</c> directly. The
/// <c>config_command</c> queue exists for imperative actions only the SERVICE can perform — reaching a
/// monitored SQL Server, flipping service state — and tags are store state the service never reads, so
/// routing them through the queue would buy nothing and cost an enqueue-and-poll round trip per edit.
/// Every write goes through <see cref="ExecuteWriteAsync"/>, so a read-only <c>viewer</c> seat degrades
/// to <see cref="ViewerReadOnlyException"/> rather than a raw 42501.</para>
///
/// <para><b>Never grant the queue as a workaround.</b> If a read-only seat trips 42501 here, the fix is
/// the UI gate, NOT <c>GRANT INSERT ON config.config_command</c> — the enqueuer picks the command type
/// freely and the executor runs as superuser, so that grant is full service control.</para>
///
/// <para>Bare table names resolve through the connection's search_path to the <c>config</c> schema, the
/// same as every other config write in this service. The tree itself is assembled IN MEMORY from these
/// flat rows (the table is dozens of rows) — no recursive CTE, no <c>ltree</c>, no closure table — and
/// the depth cap and cycle checks live there too.</para>
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Every tag, flat. The caller builds the tree. Ordered by name so sibling order is stable
    /// without a reordering UI.</summary>
    public const string ServerTagsSelectSql =
        "SELECT id, name, parent_id, sort_order, colour FROM server_tags ORDER BY name";

    /// <summary>Every server-to-tag assignment, flat.</summary>
    public const string ServerTagMapSelectSql =
        "SELECT server_id, tag_id FROM server_tag_map";

    /// <summary>Creates a tag. $1 name, $2 parent_id (NULL = root). Returns the new id. A duplicate name
    /// under the same parent violates the per-parent unique index and surfaces as 23505.</summary>
    public const string ServerTagInsertSql =
        "INSERT INTO server_tags (name, parent_id) VALUES ($1, $2) RETURNING id";

    /// <summary>Renames a tag. $1 id, $2 name.</summary>
    public const string ServerTagRenameSql =
        "UPDATE server_tags SET name = $2 WHERE id = $1";

    /// <summary>Sets (or clears, with NULL) a tag's colour. $1 id, $2 colour (<c>#RRGGBB</c> or NULL).</summary>
    public const string ServerTagSetColourSql =
        "UPDATE server_tags SET colour = $2 WHERE id = $1";

    /// <summary>Reparents a tag. $1 id, $2 new parent_id (NULL = promote to root). The caller must have
    /// already rejected cycles and depth-cap violations against its in-memory tree.</summary>
    public const string ServerTagReparentSql =
        "UPDATE server_tags SET parent_id = $2 WHERE id = $1";

    /// <summary>Deletes a tag. $1 id. The self-referencing FK cascades to descendants and the map cascades
    /// to their assignments — it can never reach <c>config_monitored_servers</c>, so no server, credential
    /// or collected history is touched. Callers still refuse the delete when the tag has children, and
    /// offer to move them first, rather than silently removing a subtree.</summary>
    public const string ServerTagDeleteSql =
        "DELETE FROM server_tags WHERE id = $1";

    /// <summary>Whether a tag has child tags. $1 id. Read immediately before a delete, because the tag
    /// tables are SHARED and this viewer's snapshot may be up to one refresh interval stale.</summary>
    public const string ServerTagHasChildrenSql =
        "SELECT EXISTS (SELECT 1 FROM server_tags WHERE parent_id = $1)";

    /// <summary>Assigns a tag to many servers in ONE statement. $1 server ids, $2 tag id. Bulk by design:
    /// tagging 100 servers row-by-row would be 100 round trips. Already-assigned servers are no-ops.</summary>
    public const string ServerTagAssignSql =
        "INSERT INTO server_tag_map (server_id, tag_id) SELECT unnest($1), $2 ON CONFLICT DO NOTHING";

    /// <summary>Removes a tag from many servers in one statement. $1 server ids, $2 tag id.</summary>
    public const string ServerTagUnassignSql =
        "DELETE FROM server_tag_map WHERE server_id = ANY($1) AND tag_id = $2";

    /// <summary>Drops every tag assignment for a server. $1 server_id. Called when a server is REMOVED:
    /// <c>server_id</c> is a deterministic hash of host+database+read-only-intent, so leaving orphaned map
    /// rows behind would make a removed-then-re-added server silently RESURRECT its old tags.</summary>
    public const string ServerTagClearForServerSql =
        "DELETE FROM server_tag_map WHERE server_id = $1";

    /// <summary>Reads every tag, flat, for the in-memory tree build.</summary>
    public async Task<List<DarlingTag>> GetServerTagsAsync(CancellationToken cancellationToken = default)
    {
        var tags = new List<DarlingTag>();
        await using var command = _dataSource.CreateCommand(ServerTagsSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            tags.Add(new DarlingTag(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        return tags;
    }

    /// <summary>Reads every server-to-tag assignment, flat.</summary>
    public async Task<List<DarlingTagAssignment>> GetServerTagAssignmentsAsync(CancellationToken cancellationToken = default)
    {
        var assignments = new List<DarlingTagAssignment>();
        await using var command = _dataSource.CreateCommand(ServerTagMapSelectSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            assignments.Add(new DarlingTagAssignment(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return assignments;
    }

    /// <summary>Creates a tag and returns its id, assigning it a palette colour derived from that id.</summary>
    public async Task<int> CreateServerTagAsync(string name, int? parentId, CancellationToken cancellationToken = default)
    {
        int id;
        await using (var command = _dataSource.CreateCommand(ServerTagInsertSql))
        {
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
            command.Parameters.Add(parentId is int parent
                ? new NpgsqlParameter<int> { TypedValue = parent }
                : new NpgsqlParameter { Value = System.DBNull.Value });

            try
            {
                id = (int)(await command.ExecuteScalarAsync(cancellationToken))!;
            }
            catch (PostgresException ex) when (ex.SqlState == InsufficientPrivilegeSqlState)
            {
                throw new ViewerReadOnlyException(ex);
            }
        }

        /* Stamp the id-derived palette colour now that the id exists (#2008 2a), so a new tag has a stable,
           reproducible colour the user can later override. The insert already proved the seat can write, so
           this second statement runs only on a writable seat. */
        await SetServerTagColorAsync(id, TagColorPalette.ForTagId(id), cancellationToken);
        return id;
    }

    /// <summary>Renames a tag.</summary>
    public async Task RenameServerTagAsync(int tagId, string name, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagRenameSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Sets a tag's colour, or clears it to neutral when <paramref name="colour"/> is null. Goes
    /// through <see cref="ExecuteWriteAsync"/>, so a read-only seat degrades to
    /// <see cref="ViewerReadOnlyException"/>.</summary>
    public async Task SetServerTagColorAsync(int tagId, string? colour, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagSetColourSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        command.Parameters.Add(colour is null
            ? new NpgsqlParameter { Value = System.DBNull.Value }
            : new NpgsqlParameter<string> { TypedValue = colour });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Moves a tag under a new parent (null = promote to root). Cycle and depth-cap rejection is
    /// the caller's job, against its in-memory tree.</summary>
    public async Task ReparentServerTagAsync(int tagId, int? newParentId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagReparentSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        command.Parameters.Add(newParentId is int parent
            ? new NpgsqlParameter<int> { TypedValue = parent }
            : new NpgsqlParameter { Value = System.DBNull.Value });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Whether a tag currently has children, read fresh from the store.</summary>
    public async Task<bool> ServerTagHasChildrenAsync(int tagId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagHasChildrenSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>Deletes a tag.</summary>
    public async Task DeleteServerTagAsync(int tagId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagDeleteSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Assigns one tag to many servers in a single statement.</summary>
    public async Task AssignServerTagAsync(IReadOnlyList<int> serverIds, int tagId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagAssignSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = System.Linq.Enumerable.ToArray(serverIds) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Removes one tag from many servers in a single statement.</summary>
    public async Task UnassignServerTagAsync(IReadOnlyList<int> serverIds, int tagId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagUnassignSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = System.Linq.Enumerable.ToArray(serverIds) });
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = tagId });
        await ExecuteWriteAsync(command, cancellationToken);
    }

    /// <summary>Drops every tag assignment for a removed server, so a re-add cannot resurrect old tags.</summary>
    public async Task ClearServerTagsAsync(int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(ServerTagClearForServerSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await ExecuteWriteAsync(command, cancellationToken);
    }
}
