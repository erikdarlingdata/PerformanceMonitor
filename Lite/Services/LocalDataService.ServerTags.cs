/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Ui;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>One tag row. <c>ParentId</c> null = a root tag; <c>Colour</c> null = a neutral pill.</summary>
public sealed record ServerTag(int Id, string Name, int? ParentId, int SortOrder, string? Colour);

/// <summary>One server-to-tag assignment (many-to-many).</summary>
public sealed record ServerTagAssignment(int ServerId, int TagId);

/// <summary>
/// The Lite fleet-tag store (#2020 2b-i): the DuckDB twin of the Darling viewer's <c>config.server_tags</c>
/// tree + <c>config.server_tag_map</c>. Same shape and semantics; different mechanics, because DuckDB has no
/// IDENTITY, no self-FK cascade, and no partial/expression unique index — so this store assigns tag ids in
/// C# (max+1 under the exclusive write lock), enforces "unique name per parent" itself, and cascades a delete
/// to the subtree + its assignments with an explicit recursive walk. Reads take the read lock, writes the
/// write lock, mirroring every other Lite store. There is no read-only seat in Lite (single connection, the
/// user's own credentials), so unlike the viewer no privilege-degrade path is needed.
/// </summary>
public partial class LocalDataService
{
    /// <summary>Every tag, flat; the caller builds the tree. Ordered by name for stable sibling order.</summary>
    public async Task<List<ServerTag>> GetServerTagsAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id, name, parent_id, sort_order, colour FROM server_tags ORDER BY name";

        var tags = new List<ServerTag>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tags.Add(new ServerTag(
                reader.GetInt32(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4)));
        }

        return tags;
    }

    /// <summary>Every server-to-tag assignment, flat.</summary>
    public async Task<List<ServerTagAssignment>> GetServerTagAssignmentsAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT server_id, tag_id FROM server_tag_map";

        var assignments = new List<ServerTagAssignment>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            assignments.Add(new ServerTagAssignment(reader.GetInt32(0), reader.GetInt32(1)));
        }

        return assignments;
    }

    /// <summary>
    /// Creates a tag (<paramref name="parentId"/> null = a root) and returns its new id, assigning it a
    /// palette colour derived from that id. Rejects a duplicate name under the same parent with an
    /// <see cref="InvalidOperationException"/> — the invariant Darling's unique index enforces and DuckDB
    /// cannot. The whole read-max-then-insert runs under the single exclusive write lock, so the computed id
    /// can't race another writer.
    /// </summary>
    public async Task<int> CreateServerTagAsync(string name, int? parentId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tags");

        await ThrowIfDuplicateNameAsync(connection, parentId, name, excludeTagId: null);

        int id;
        using (var next = connection.CreateCommand())
        {
            next.CommandText = "SELECT COALESCE(MAX(id), 0) + 1 FROM server_tags";
            id = Convert.ToInt32(await next.ExecuteScalarAsync());
        }

        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO server_tags (id, name, parent_id, sort_order, colour, created_at)
SELECT $1, $2, $3, 0, $4, now()::TIMESTAMP";
            insert.Parameters.Add(new DuckDBParameter { Value = id });
            insert.Parameters.Add(new DuckDBParameter { Value = name });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)parentId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = TagColorPalette.ForTagId(id) });
            await insert.ExecuteNonQueryAsync();
        }

        return id;
    }

    /// <summary>Renames a tag, rejecting a duplicate name under its parent (excluding itself).</summary>
    public async Task RenameServerTagAsync(int tagId, string name)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tags");

        var parentId = await GetParentIdAsync(connection, tagId);
        await ThrowIfDuplicateNameAsync(connection, parentId, name, excludeTagId: tagId);

        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE server_tags SET name = $2 WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        command.Parameters.Add(new DuckDBParameter { Value = name });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Sets a tag's colour, or clears it to neutral when <paramref name="colour"/> is null.</summary>
    public async Task SetServerTagColorAsync(int tagId, string? colour)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tags");
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE server_tags SET colour = $2 WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)colour ?? DBNull.Value });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>Moves a tag under a new parent (null = promote to root). Cycle and depth-cap rejection is the
    /// caller's job against its in-memory tree, exactly as in the viewer.</summary>
    public async Task ReparentServerTagAsync(int tagId, int? newParentId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tags");
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE server_tags SET parent_id = $2 WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)newParentId ?? DBNull.Value });
        await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// Deletes a tag and its whole subtree, plus every assignment those tags carried. Darling leans on a
    /// self-FK <c>ON DELETE CASCADE</c>; DuckDB has none, so the subtree is gathered with a recursive walk and
    /// deleted explicitly. Can never reach the server registry — only <c>server_tags</c> / <c>server_tag_map</c>
    /// rows are touched, so no server, credential, or collected history is affected.
    /// </summary>
    public async Task DeleteServerTagAsync(int tagId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tags");

        var subtree = new List<int> { tagId };
        using (var walk = connection.CreateCommand())
        {
            walk.CommandText = @"
WITH RECURSIVE subtree(id) AS (
    SELECT id FROM server_tags WHERE id = $1
    UNION ALL
    SELECT t.id FROM server_tags t JOIN subtree s ON t.parent_id = s.id
)
SELECT id FROM subtree WHERE id <> $1";
            walk.Parameters.Add(new DuckDBParameter { Value = tagId });
            using var reader = await walk.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                subtree.Add(reader.GetInt32(0));
            }
        }

        /* ids are DB-sourced integers, so an inline IN list is injection-safe and avoids a variadic param bind. */
        var inList = string.Join(",", subtree);

        using (var delMap = connection.CreateCommand())
        {
            delMap.CommandText = $"DELETE FROM server_tag_map WHERE tag_id IN ({inList})";
            await delMap.ExecuteNonQueryAsync();
        }

        using (var delTags = connection.CreateCommand())
        {
            delTags.CommandText = $"DELETE FROM server_tags WHERE id IN ({inList})";
            await delTags.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Whether a tag has child tags — read immediately before a delete so the confirmation can warn.</summary>
    public async Task<bool> ServerTagHasChildrenAsync(int tagId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM server_tags WHERE parent_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        return Convert.ToInt64(await command.ExecuteScalarAsync()) > 0;
    }

    /// <summary>Assigns a tag to servers. Already-assigned servers are no-ops (composite PK + ON CONFLICT).</summary>
    public async Task AssignServerTagAsync(IEnumerable<int> serverIds, int tagId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tag_map");
        foreach (var serverId in serverIds)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "INSERT INTO server_tag_map (server_id, tag_id) VALUES ($1, $2) ON CONFLICT DO NOTHING";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = tagId });
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Removes a tag from servers.</summary>
    public async Task UnassignServerTagAsync(IEnumerable<int> serverIds, int tagId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tag_map");
        foreach (var serverId in serverIds)
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM server_tag_map WHERE server_id = $1 AND tag_id = $2";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = tagId });
            await command.ExecuteNonQueryAsync();
        }
    }

    /// <summary>Drops every tag assignment for a server. Called when a server is removed: the server_id is a
    /// deterministic hash of host+database+intent, so a removed-then-re-added server would otherwise silently
    /// resurrect its old tags. Same rationale as the Darling twin.</summary>
    public async Task ClearServerTagsForServerAsync(int serverId)
    {
        using var connection = await OpenWriteConnectionAsync();
        PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, "server_tag_map");
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM server_tag_map WHERE server_id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<int?> GetParentIdAsync(LockedConnection connection, int tagId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT parent_id FROM server_tags WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        var result = await command.ExecuteScalarAsync();
        return result is null || result is DBNull ? null : Convert.ToInt32(result);
    }

    private static async Task ThrowIfDuplicateNameAsync(LockedConnection connection, int? parentId, string name, int? excludeTagId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COUNT(*) FROM server_tags
WHERE COALESCE(parent_id, 0) = COALESCE($1, 0)
AND   lower(name) = lower($2)
AND   ($3 IS NULL OR id <> $3)";
        command.Parameters.Add(new DuckDBParameter { Value = (object?)parentId ?? DBNull.Value });
        command.Parameters.Add(new DuckDBParameter { Value = name });
        command.Parameters.Add(new DuckDBParameter { Value = (object?)excludeTagId ?? DBNull.Value });

        if (Convert.ToInt64(await command.ExecuteScalarAsync()) > 0)
        {
            throw new InvalidOperationException($"A tag named “{name}” already exists here.");
        }
    }
}
