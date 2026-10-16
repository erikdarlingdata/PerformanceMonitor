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
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;
using PerformanceMonitor.Common.QueryTags;
using PerformanceMonitor.Common.Tags;
using PerformanceMonitorLite.Database;

namespace PerformanceMonitorLite.Services;

/// <summary>One query tag. <c>ParentId</c> null = a root tag; <c>Colour</c> null = a neutral pill.</summary>
public sealed record QueryTagRow(int Id, string Name, int? ParentId, int SortOrder, string? Colour, DateTime CreatedAt);

/// <summary>A hand assignment as the store holds it. Kind <c>hash</c> binds by <c>QueryHash</c>; kind <c>object</c> by
/// database, schema and name. <c>ServerId</c> null = every server. <c>QueryId</c> is a display key only.</summary>
public sealed record QueryTagAssignmentRow(
    int Id, int TagId, string Kind, int? ServerId, string? QueryHash,
    string? DatabaseName, string? SchemaName, string? ObjectName, long? QueryId,
    DateTime CreatedAt, string CreatedBy);

/// <summary>What a caller supplies to make a hand assignment.</summary>
public sealed record QueryTagAssignmentInput(
    int TagId, QueryTagKeyKind KeyKind, int? ServerId = null, string? QueryHash = null,
    string? DatabaseName = null, string? SchemaName = null, string? ObjectName = null,
    long? QueryId = null, string CreatedBy = "lite");

/// <summary>A rule as the store holds it. <c>MatchField</c> is the stored text (<c>application</c>, <c>login</c>,
/// <c>host</c>, <c>database</c>, <c>object</c>, <c>text</c> or <c>text_tag</c>); <c>TagId</c> is null only for <c>text_tag</c>.</summary>
public sealed record QueryTagRuleRow(
    int Id, int? TagId, int Priority, bool Enabled, int? ServerId, string MatchField, string Pattern,
    DateTime CreatedAt, string CreatedBy);

/// <summary>What a caller supplies to make or replace a rule. A <c>TextTag</c> rule names no tag and reads its tag
/// from the statement text; its pattern is unused and is stored as <c>/* tag:NAME */</c> when blank.</summary>
public sealed record QueryTagRuleInput(
    QueryTagRuleField Field, string? Pattern, int? TagId, int Priority = 100, bool Enabled = true,
    int? ServerId = null, string CreatedBy = "lite");

/// <summary>The one row of <c>query_tag_state</c>. The map is current when <c>BuiltVersion</c> equals <c>RulesVersion</c>.</summary>
public sealed record QueryTagState(long RulesVersion, long BuiltVersion, DateTime? UpdatedAt);

/// <summary>A refused query-tag write. <c>Code</c> is the shared code (<c>bad_name</c>, <c>bad_colour</c>, <c>depth_limit</c>,
/// <c>cycle</c>, <c>duplicate_name</c>, <c>unknown_parent</c>) or one of the store's own (<see cref="QueryTagStoreCodes"/>);
/// null for a not-found with no code.</summary>
public sealed class QueryTagRefusedException(TagRefusalKind kind, string? code, string message)
    : InvalidOperationException(message)
{
    public TagRefusalKind Kind { get; } = kind;
    public string? Code { get; } = code;
}

/// <summary>Codes the store adds to the shared tag-tree codes, for the assignment and rule checks the shared tree rules do not cover.</summary>
public static class QueryTagStoreCodes
{
    public const string UnknownTag = "unknown_tag";
    public const string BadAssignment = "bad_assignment";
    public const string DuplicateAssignment = "duplicate_assignment";
    public const string BadRule = "bad_rule";
    public const string NotFound = "not_found";
}

/// <summary>
/// The Lite query-tag store (#5989): tags, hand assignments and rules, in DuckDB, the twin of Darling's
/// <c>config.query_tags</c>, <c>query_tag_assignments</c> and <c>query_tag_rules</c>. Like the server-tag store, and for
/// the same reasons (DuckDB has no IDENTITY, no self-FK cascade, no partial unique index), ids are assigned in C#
/// (max + 1 under the write lock), a tag delete walks its subtree by hand, and unique name per parent and one assignment
/// per key are enforced here. Every tree check comes from <see cref="TagTreeRules"/> and every rule check from
/// <see cref="QueryTagRuleValidator"/>; this file holds no copy of either. DuckDB has no trigger, so every write bumps
/// <c>query_tag_state.rules_version</c> itself. A folded tag name (one name under two parents) is never blocked here:
/// the resolver treats a tag comment that names it as ambiguous.
/// </summary>
public partial class LocalDataService
{
    private static readonly string[] s_queryTagTables = ["query_tags", "query_tag_assignments", "query_tag_rules"];

    // ---- reads ----------------------------------------------------------------------------------

    /// <summary>Every query tag, flat, ordered by name; the caller builds the tree.</summary>
    public async Task<List<QueryTagRow>> GetQueryTagsAsync()
    {
        using var connection = await OpenConnectionAsync();
        return await ReadQueryTagsAsync(connection);
    }

    /// <summary>Every hand assignment, optionally only those for one tag.</summary>
    public async Task<List<QueryTagAssignmentRow>> GetQueryTagAssignmentsAsync(int? tagId = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT id, tag_id, kind, server_id, query_hash, database_name, schema_name, object_name, query_id, created_at, created_by
FROM query_tag_assignments
WHERE ($1 IS NULL OR tag_id = $1)
ORDER BY id";
        command.Parameters.Add(new DuckDBParameter { Value = (object?)tagId ?? DBNull.Value });

        var rows = new List<QueryTagAssignmentRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new QueryTagAssignmentRow(
                reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.GetDateTime(9), reader.GetString(10)));
        }

        return rows;
    }

    /// <summary>Every rule, in evaluation order (priority, then id).</summary>
    public async Task<List<QueryTagRuleRow>> GetQueryTagRulesAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT id, tag_id, priority, enabled, server_id, match_field, pattern, created_at, created_by
FROM query_tag_rules
ORDER BY priority, id";

        var rows = new List<QueryTagRuleRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new QueryTagRuleRow(
                reader.GetInt32(0),
                reader.IsDBNull(1) ? null : reader.GetInt32(1),
                reader.GetInt32(2), reader.GetBoolean(3),
                reader.IsDBNull(4) ? null : reader.GetInt32(4),
                reader.GetString(5), reader.GetString(6), reader.GetDateTime(7), reader.GetString(8)));
        }

        return rows;
    }

    /// <summary>The rules version and the version the map was last built at. A store never written to reports 1 and 0.</summary>
    public async Task<QueryTagState> GetQueryTagStateAsync()
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT rules_version, built_version, updated_at FROM query_tag_state WHERE id = 1";
        using var reader = await command.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            return new QueryTagState(reader.GetInt64(0), reader.GetInt64(1), reader.GetDateTime(2));
        }

        return new QueryTagState(1, 0, null);
    }

    // ---- tags -----------------------------------------------------------------------------------

    /// <summary>
    /// Creates a tag (<paramref name="parentId"/> null = a root) and returns its id. A null <paramref name="colour"/> gets the
    /// palette colour for the new id. Refused with <see cref="QueryTagRefusedException"/> (shared code) for a bad name or
    /// colour, an unknown parent, the depth limit or a duplicate name under the parent.
    /// </summary>
    public async Task<int> CreateQueryTagAsync(string name, int? parentId, string? colour = null)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();

        var tags = await ReadQueryTagNodesAsync(connection);
        ThrowIfRefused(TagTreeRules.CheckCreate(tags, name, parentId, colour, out var cleanName, out var cleanColour));

        var id = await NextIdAsync(connection, "query_tags");
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO query_tags (id, name, parent_id, sort_order, colour, created_at)
SELECT $1, $2, $3, 0, $4, now()::TIMESTAMP";
            insert.Parameters.Add(new DuckDBParameter { Value = id });
            insert.Parameters.Add(new DuckDBParameter { Value = cleanName });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)parentId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)cleanColour ?? TagColours.ForTagId(id) });
            await insert.ExecuteNonQueryAsync();
        }

        await BumpQueryTagRulesVersionAsync(connection);
        return id;
    }

    /// <summary>Renames, recolours and/or moves a tag in one checked edit (the shared <see cref="TagTreeEdit"/>: only the
    /// parts flagged are changed; a null colour clears it, a null parent promotes to a root).</summary>
    public async Task UpdateQueryTagAsync(int tagId, TagTreeEdit edit)
    {
        ArgumentNullException.ThrowIfNull(edit);
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();

        var tags = await ReadQueryTagNodesAsync(connection);
        ThrowIfRefused(TagTreeRules.CheckUpdate(tags, tagId, edit, out var cleanName, out var cleanColour));

        if (edit.HasName || edit.HasColour || edit.HasParent)
        {
            using var command = connection.CreateCommand();
            command.CommandText = @"
UPDATE query_tags SET
    name = CASE WHEN $2 THEN $3 ELSE name END,
    colour = CASE WHEN $4 THEN $5 ELSE colour END,
    parent_id = CASE WHEN $6 THEN $7 ELSE parent_id END
WHERE id = $1";
            command.Parameters.Add(new DuckDBParameter { Value = tagId });
            command.Parameters.Add(new DuckDBParameter { Value = edit.HasName });
            command.Parameters.Add(new DuckDBParameter { Value = (object?)cleanName ?? DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = edit.HasColour });
            command.Parameters.Add(new DuckDBParameter { Value = (object?)cleanColour ?? DBNull.Value });
            command.Parameters.Add(new DuckDBParameter { Value = edit.HasParent });
            command.Parameters.Add(new DuckDBParameter { Value = (object?)edit.ParentId ?? DBNull.Value });
            await command.ExecuteNonQueryAsync();
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    /// <summary>
    /// Deletes a tag and its whole subtree, plus every assignment, rule and map row those tags carried. A <c>text_tag</c>
    /// rule names no tag and stays. Refused with <c>not_found</c> for an unknown tag.
    /// </summary>
    public async Task DeleteQueryTagAsync(int tagId)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();

        var tags = await ReadQueryTagNodesAsync(connection);
        ThrowIfRefused(TagTreeRules.CheckDelete(tags, tagId));

        /* The shared walk, over the rows just read, so the cascade follows the same parent edges the rules do. */
        var subtree = TagTreeRules.Descendants(tags, tagId);
        subtree.Add(tagId);
        /* ids are DB-sourced integers, so an inline IN list is injection-safe. */
        var inList = string.Join(",", subtree.Select(i => i.ToString(CultureInfo.InvariantCulture)));

        foreach (var sql in new[]
        {
            $"DELETE FROM query_tag_map WHERE tag_id IN ({inList})",
            $"DELETE FROM query_tag_assignments WHERE tag_id IN ({inList})",
            $"DELETE FROM query_tag_rules WHERE tag_id IN ({inList})",
            $"DELETE FROM query_tags WHERE id IN ({inList})",
        })
        {
            using var command = connection.CreateCommand();
            command.CommandText = sql;
            await command.ExecuteNonQueryAsync();
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    // ---- assignments ----------------------------------------------------------------------------

    /// <summary>
    /// Adds a hand assignment and returns its id. A <c>hash</c> assignment needs a query hash and no object parts; an
    /// <c>object</c> assignment needs database, schema and name and no hash. A second assignment of the same key on the
    /// same server (or on every server) is refused as <c>duplicate_assignment</c>; an unknown tag as <c>unknown_tag</c>.
    /// </summary>
    public async Task<int> AddQueryTagAssignmentAsync(QueryTagAssignmentInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var (kind, hash, db, schema, name) = NormaliseAssignment(input);

        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        await ThrowIfTagMissingAsync(connection, input.TagId);

        using (var dup = connection.CreateCommand())
        {
            dup.CommandText = @"
SELECT COUNT(*) FROM query_tag_assignments
WHERE kind = $1
AND   COALESCE(server_id, -1) = COALESCE($2, -1)
AND   COALESCE(query_hash, '') = COALESCE($3, '')
AND   COALESCE(database_name, '') = COALESCE($4, '')
AND   COALESCE(schema_name, '') = COALESCE($5, '')
AND   COALESCE(object_name, '') = COALESCE($6, '')";
            dup.Parameters.Add(new DuckDBParameter { Value = kind });
            dup.Parameters.Add(new DuckDBParameter { Value = (object?)input.ServerId ?? DBNull.Value });
            dup.Parameters.Add(new DuckDBParameter { Value = (object?)hash ?? DBNull.Value });
            dup.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
            dup.Parameters.Add(new DuckDBParameter { Value = (object?)schema ?? DBNull.Value });
            dup.Parameters.Add(new DuckDBParameter { Value = (object?)name ?? DBNull.Value });
            if (Convert.ToInt64(await dup.ExecuteScalarAsync(), CultureInfo.InvariantCulture) > 0)
            {
                throw new QueryTagRefusedException(TagRefusalKind.Conflict, QueryTagStoreCodes.DuplicateAssignment,
                    "That query or object is already assigned a tag on that server.");
            }
        }

        var id = await NextIdAsync(connection, "query_tag_assignments");
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO query_tag_assignments
    (id, tag_id, kind, server_id, query_hash, database_name, schema_name, object_name, query_id, created_at, created_by)
SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, now()::TIMESTAMP, $10";
            insert.Parameters.Add(new DuckDBParameter { Value = id });
            insert.Parameters.Add(new DuckDBParameter { Value = input.TagId });
            insert.Parameters.Add(new DuckDBParameter { Value = kind });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)input.ServerId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)hash ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)db ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)schema ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)name ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)input.QueryId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(input.CreatedBy) ? "lite" : input.CreatedBy });
            await insert.ExecuteNonQueryAsync();
        }

        await BumpQueryTagRulesVersionAsync(connection);
        return id;
    }

    /// <summary>Removes one hand assignment. <c>not_found</c> when there is no such id.</summary>
    public async Task DeleteQueryTagAssignmentAsync(int assignmentId)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        if (await DeleteByIdAsync(connection, "query_tag_assignments", assignmentId) == 0)
        {
            throw NotFound("Assignment", assignmentId);
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    // ---- rules ----------------------------------------------------------------------------------

    /// <summary>
    /// Adds a rule and returns its id. Checked by <see cref="QueryTagRuleValidator.Validate"/>; a failure is refused as
    /// <c>bad_rule</c> with the validator's error named in the message. A rule's tag must exist (<c>unknown_tag</c>).
    /// </summary>
    public async Task<int> AddQueryTagRuleAsync(QueryTagRuleInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        ThrowIfRuleInvalid(input);

        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        if (input.TagId is int tagId)
        {
            await ThrowIfTagMissingAsync(connection, tagId);
        }

        var id = await NextIdAsync(connection, "query_tag_rules");
        using (var insert = connection.CreateCommand())
        {
            insert.CommandText = @"
INSERT INTO query_tag_rules (id, tag_id, priority, enabled, server_id, match_field, pattern, created_at, created_by)
SELECT $1, $2, $3, $4, $5, $6, $7, now()::TIMESTAMP, $8";
            insert.Parameters.Add(new DuckDBParameter { Value = id });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)input.TagId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = input.Priority });
            insert.Parameters.Add(new DuckDBParameter { Value = input.Enabled });
            insert.Parameters.Add(new DuckDBParameter { Value = (object?)input.ServerId ?? DBNull.Value });
            insert.Parameters.Add(new DuckDBParameter { Value = QueryTagFieldText(input.Field) });
            insert.Parameters.Add(new DuckDBParameter { Value = StoredRulePattern(input) });
            insert.Parameters.Add(new DuckDBParameter { Value = string.IsNullOrWhiteSpace(input.CreatedBy) ? "lite" : input.CreatedBy });
            await insert.ExecuteNonQueryAsync();
        }

        await BumpQueryTagRulesVersionAsync(connection);
        return id;
    }

    /// <summary>Replaces a rule's definition (field, pattern, tag, priority, enabled, server), keeping its id and creator.</summary>
    public async Task UpdateQueryTagRuleAsync(int ruleId, QueryTagRuleInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        ThrowIfRuleInvalid(input);

        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        if (input.TagId is int tagId)
        {
            await ThrowIfTagMissingAsync(connection, tagId);
        }

        int changed;
        using (var update = connection.CreateCommand())
        {
            update.CommandText = @"
UPDATE query_tag_rules
SET tag_id = $2, priority = $3, enabled = $4, server_id = $5, match_field = $6, pattern = $7
WHERE id = $1";
            update.Parameters.Add(new DuckDBParameter { Value = ruleId });
            update.Parameters.Add(new DuckDBParameter { Value = (object?)input.TagId ?? DBNull.Value });
            update.Parameters.Add(new DuckDBParameter { Value = input.Priority });
            update.Parameters.Add(new DuckDBParameter { Value = input.Enabled });
            update.Parameters.Add(new DuckDBParameter { Value = (object?)input.ServerId ?? DBNull.Value });
            update.Parameters.Add(new DuckDBParameter { Value = QueryTagFieldText(input.Field) });
            update.Parameters.Add(new DuckDBParameter { Value = StoredRulePattern(input) });
            changed = await update.ExecuteNonQueryAsync();
        }

        if (changed == 0)
        {
            throw NotFound("Rule", ruleId);
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    /// <summary>Turns a rule on or off without touching the rest of it.</summary>
    public async Task SetQueryTagRuleEnabledAsync(int ruleId, bool enabled)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        using (var update = connection.CreateCommand())
        {
            update.CommandText = "UPDATE query_tag_rules SET enabled = $2 WHERE id = $1";
            update.Parameters.Add(new DuckDBParameter { Value = ruleId });
            update.Parameters.Add(new DuckDBParameter { Value = enabled });
            if (await update.ExecuteNonQueryAsync() == 0)
            {
                throw NotFound("Rule", ruleId);
            }
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    /// <summary>Removes one rule. <c>not_found</c> when there is no such id.</summary>
    public async Task DeleteQueryTagRuleAsync(int ruleId)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();
        if (await DeleteByIdAsync(connection, "query_tag_rules", ruleId) == 0)
        {
            throw NotFound("Rule", ruleId);
        }

        await BumpQueryTagRulesVersionAsync(connection);
    }

    // ---- server removal and retention -----------------------------------------------------------

    /// <summary>
    /// Drops every query-tag row that belongs to one server: its hand assignments and rules (those that name it, never the
    /// every-server ones), its sightings and its map rows. Called when a server is removed, for the reason
    /// <see cref="ClearServerTagsForServerAsync"/> is: the server id is a hash, so a server added again would otherwise get
    /// its old rows back. Bumps <c>rules_version</c> when it removed an assignment or a rule.
    /// </summary>
    public async Task ClearQueryTagsForServerAsync(int serverId)
    {
        using var connection = await OpenUserWriteConnectionAsync();
        ThrowIfQueryTagRestorePending();

        var removedConfig = 0;
        foreach (var table in new[] { "query_tag_assignments", "query_tag_rules", "query_tag_sightings", "query_tag_map" })
        {
            using var command = connection.CreateCommand();
            command.CommandText = $"DELETE FROM {table} WHERE server_id = $1";
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            var deleted = await command.ExecuteNonQueryAsync();
            if (table is "query_tag_assignments" or "query_tag_rules")
            {
                removedConfig += deleted;
            }
        }

        if (removedConfig > 0)
        {
            await BumpQueryTagRulesVersionAsync(connection);
        }
    }

    /// <summary>Retention for <c>query_tag_sightings</c>: removes rows not seen since <paramref name="retentionDays"/> ago
    /// (by <c>last_seen</c>) and returns how many. Only this table is aged; tags, assignments and rules are configuration.
    /// A background sweep, so it is not refused while a restore is pending (it touches no preserved table).</summary>
    public async Task<int> PurgeOldQueryTagSightingsAsync(int retentionDays = QueryTagSightingRetentionDays)
    {
        using var connection = await OpenWriteConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM query_tag_sightings WHERE last_seen < $1";
        command.Parameters.Add(new DuckDBParameter { Value = DateTime.UtcNow.AddDays(-retentionDays) });
        return await command.ExecuteNonQueryAsync();
    }

    /// <summary>
    /// How long a sighting is kept after it was last seen. Lite keeps collected rows hot for
    /// <see cref="ArchiveService.HotDataDays"/> days and the archive for <see cref="RetentionService.ArchiveRetentionMonths"/>
    /// months, so a sighting outlives the data that produced it by no more than that archive window.
    /// </summary>
    internal const int QueryTagSightingRetentionDays = RetentionService.ArchiveRetentionMonths * 31;

    // ---- helpers --------------------------------------------------------------------------------

    /// <summary>Every writer refuses while the restore of any of the three user-choice tables is pending.</summary>
    private void ThrowIfQueryTagRestorePending()
    {
        foreach (var table in s_queryTagTables)
        {
            PreservedTableRestore.ThrowIfRestorePending(_duckDb.ArchivePath, table);
        }
    }

    private static string QueryTagFieldText(QueryTagRuleField field) => field switch
    {
        QueryTagRuleField.Application => "application",
        QueryTagRuleField.Login => "login",
        QueryTagRuleField.Host => "host",
        QueryTagRuleField.Database => "database",
        QueryTagRuleField.Object => "object",
        QueryTagRuleField.Text => "text",
        QueryTagRuleField.TextTag => "text_tag",
        _ => throw new QueryTagRefusedException(TagRefusalKind.Refused, QueryTagStoreCodes.BadRule, "Unknown rule field."),
    };

    private static string StoredRulePattern(QueryTagRuleInput input) =>
        input.Field == QueryTagRuleField.TextTag
            ? (string.IsNullOrWhiteSpace(input.Pattern) ? "/* tag:NAME */" : input.Pattern.Trim())
            : input.Pattern!.Trim();

    private static void ThrowIfRuleInvalid(QueryTagRuleInput input)
    {
        /* Enum.IsDefined first so an out-of-range field is a refusal, not an unmapped value reaching the validator. */
        if (!Enum.IsDefined(input.Field))
        {
            throw new QueryTagRefusedException(TagRefusalKind.Refused, QueryTagStoreCodes.BadRule, "Unknown rule field.");
        }

        var error = QueryTagRuleValidator.Validate(new QueryTagRule(
            0, input.Field, input.Pattern, input.Priority, input.Enabled, input.TagId, input.ServerId));
        if (error != QueryTagRuleError.None)
        {
            throw new QueryTagRefusedException(TagRefusalKind.Refused, QueryTagStoreCodes.BadRule, $"The rule is not valid: {error}.");
        }
    }

    private static (string Kind, string? Hash, string? Database, string? Schema, string? Name) NormaliseAssignment(QueryTagAssignmentInput input)
    {
        static string? Clean(string? s) => string.IsNullOrWhiteSpace(s) ? null : s.Trim();
        var hash = Clean(input.QueryHash);
        var db = Clean(input.DatabaseName);
        var schema = Clean(input.SchemaName);
        var name = Clean(input.ObjectName);

        switch (input.KeyKind)
        {
            case QueryTagKeyKind.Hash when hash is not null && db is null && schema is null && name is null:
                return ("hash", hash, null, null, null);
            case QueryTagKeyKind.Object when hash is null && db is not null && schema is not null && name is not null:
                return ("object", null, db, schema, name);
            default:
                throw new QueryTagRefusedException(TagRefusalKind.Refused, QueryTagStoreCodes.BadAssignment,
                    "A hash assignment needs a query hash and nothing else; an object assignment needs a database, schema and name and no hash.");
        }
    }

    private static void ThrowIfRefused(TagRefusal? refusal)
    {
        if (refusal is not null)
        {
            throw new QueryTagRefusedException(refusal.Kind, refusal.Code, refusal.Message);
        }
    }

    private static QueryTagRefusedException NotFound(string what, int id) =>
        new(TagRefusalKind.NotFound, QueryTagStoreCodes.NotFound, string.Create(CultureInfo.InvariantCulture, $"{what} {id} does not exist."));

    private static async Task ThrowIfTagMissingAsync(LockedConnection connection, int tagId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM query_tags WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = tagId });
        if (Convert.ToInt64(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture) == 0)
        {
            throw new QueryTagRefusedException(TagRefusalKind.NotFound, QueryTagStoreCodes.UnknownTag,
                string.Create(CultureInfo.InvariantCulture, $"Tag {tagId} does not exist."));
        }
    }

    private static async Task<int> NextIdAsync(LockedConnection connection, string table)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COALESCE(MAX(id), 0) + 1 FROM {table}";
        return Convert.ToInt32(await command.ExecuteScalarAsync(), CultureInfo.InvariantCulture);
    }

    private static async Task<int> DeleteByIdAsync(LockedConnection connection, string table, int id)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"DELETE FROM {table} WHERE id = $1";
        command.Parameters.Add(new DuckDBParameter { Value = id });
        return await command.ExecuteNonQueryAsync();
    }

    /// <summary>Raises <c>rules_version</c> by one, creating the state row on the first write. Every query-tag write ends here.</summary>
    private static async Task BumpQueryTagRulesVersionAsync(LockedConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
INSERT INTO query_tag_state (id, rules_version, built_version, updated_at)
VALUES (1, 2, 0, now()::TIMESTAMP)
ON CONFLICT (id) DO UPDATE SET rules_version = query_tag_state.rules_version + 1, updated_at = now()::TIMESTAMP";
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<List<QueryTagRow>> ReadQueryTagsAsync(LockedConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT id, name, parent_id, sort_order, colour, created_at FROM query_tags ORDER BY name, id";

        var tags = new List<QueryTagRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            tags.Add(new QueryTagRow(
                reader.GetInt32(0), reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetInt32(2),
                reader.GetInt32(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetDateTime(5)));
        }

        return tags;
    }

    private static async Task<List<TagTreeNode>> ReadQueryTagNodesAsync(LockedConnection connection) =>
        (await ReadQueryTagsAsync(connection)).Select(t => new TagTreeNode(t.Id, t.Name, t.ParentId)).ToList();
}
