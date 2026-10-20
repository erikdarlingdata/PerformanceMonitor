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
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Typed CRUD over <c>config.custom_views</c> (#1563) — the web dashboard's user-authored view/notebook
/// definitions. Built from the web host's least-privilege VIEWER-role pool exactly like
/// <c>DarlingAnalysisService</c> is built from it (<c>new CustomViewStore(postgres)</c>); the viewer role
/// carries the single narrow <c>INSERT/UPDATE/DELETE ON config.custom_views</c> grant (see
/// <see cref="DarlingManagedRoles"/>). Same store discipline as <c>ViewerDataService.Commands</c>: public-const
/// SQL, bound <c>$N</c> parameters, <c>definition</c> bound as <c>jsonb</c>. Bare table name — the viewer pool's
/// <c>search_path = collect, config, public</c> resolves <c>custom_views</c> to <c>config.custom_views</c>
/// (there is no such table in <c>collect</c>), mirroring <c>config_command</c> in ViewerDataService.Commands.
///
/// <para><b>Optimistic concurrency:</b> <see cref="UpdateAsync"/> carries <c>WHERE id = $1 AND version = $6</c>
/// and bumps <c>version</c> in place; a 0-rowcount is disambiguated by a follow-up probe into
/// <see cref="CustomViewResult.NotFound"/> (the id is gone) vs <see cref="CustomViewResult.Conflict"/> (a stale
/// version — someone else wrote first). A duplicate <c>name</c> (unique violation, 23505) is also a
/// <see cref="CustomViewResult.Conflict"/>. The route layer validates the definition STRUCTURE first
/// (DarlingWebEndpoints.ValidateDefinition, the authority); this store adds only light argument guards
/// (<see cref="CustomViewResult.Invalid"/>) so it is safe to call independently.</para>
/// </summary>
public sealed class CustomViewStore
{
    private readonly NpgsqlDataSource _dataSource;

    public CustomViewStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>The bare-array list read: the (potentially large) <c>definition</c> BODY is never selected — only
    /// the view <c>kind</c> is extracted as a scalar (<c>definition-&gt;&gt;'kind'</c>) so the list can badge/route
    /// a notebook vs a dashboard without fetching each definition. $1 = include EVERY person's personal views (true for MCP, which has
    /// no person), $2 = the caller's identity (the personal views it owns are listed; null lists none), $3 = a folder filter
    /// (null = no filter, '' = the top level, else that exact folder).</summary>
    public const string ListSql = @"
SELECT id, name, description, version, updated_at, updated_by, (definition->>'kind') AS kind, folder, personal, created_by
FROM custom_views
WHERE (NOT personal OR $1::boolean OR created_by = $2::text)
  AND ($3::text IS NULL OR COALESCE(folder, '') = $3::text)
ORDER BY name, folder NULLS FIRST, personal, id";

    /// <summary>The full single-view read (includes <c>definition</c>). $1 id.</summary>
    public const string GetSql = @"
SELECT id, name, definition, description, version, created_at, updated_at, updated_by, folder, personal, created_by
FROM custom_views
WHERE id = $1";

    /// <summary>Inserts a new view at version 1. $1 name, $2 definition (jsonb), $3 description, $4 updated_by (also the creator),
    /// $5 folder, $6 personal.</summary>
    public const string InsertSql = @"
INSERT INTO custom_views (name, definition, description, version, created_at, updated_at, updated_by, created_by, folder, personal)
VALUES ($1, $2, $3, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), $4, $4, $5, $6)
RETURNING id, name, definition, description, version, created_at, updated_at, updated_by, folder, personal, created_by";

    /// <summary>Optimistic-concurrency update. $1 id, $2 name, $3 definition (jsonb), $4 description,
    /// $5 updated_by, $6 expected_version, $7 folder, $8 personal (the new value), $9 actor (the signed-in identity; null when there is none).
    /// Returns the row only when the expected version matched AND the actor may change this view: anyone may change a shared view and keep
    /// it shared; a personal view only its owner may change; a shared view becomes personal only when the actor created it (a view with no
    /// recorded creator can never be made personal, so nobody can take a team's legacy view out of everyone else's list).</summary>
    public const string UpdateSql = @"
UPDATE custom_views
SET name = $2,
    definition = $3,
    description = $4,
    version = version + 1,
    updated_at = (now() AT TIME ZONE 'UTC'),
    updated_by = $5,
    folder = $7,
    personal = $8
WHERE id = $1 AND version = $6
  AND ((NOT personal AND NOT $8::boolean)
       OR (NOT personal AND $8::boolean AND created_by = $9::text)
       OR (personal AND created_by = $9::text))
RETURNING id, name, definition, description, version, created_at, updated_at, updated_by, folder, personal, created_by";

    /// <summary>Disambiguates a 0-rowcount update or delete: the current version, whether the view is personal and who created it, or no
    /// row when the id is gone. $1 id.</summary>
    public const string VersionProbeSql = "SELECT version, personal, created_by FROM custom_views WHERE id = $1";

    /// <summary>Deletes a view by id. $1 id, $2 actor (the signed-in identity; null when there is none). A personal view is deleted only by
    /// its owner.</summary>
    public const string DeleteSql = "DELETE FROM custom_views WHERE id = $1 AND (NOT personal OR created_by = $2::text)";

    /// <summary>Postgres <c>unique_violation</c> — a duplicate <c>name</c> insert/rename.</summary>
    private const string UniqueViolation = "23505";

    /// <summary>An abuse bound on the view name (the column itself is unbounded text).</summary>
    public const int MaxNameLength = 200;

    /// <summary>The longest folder name (#6214); the store holds the same bound as a check.</summary>
    public const int MaxFolderLength = 100;

    /// <summary>The plain sentence for a personal view somebody other than its owner tries to change or delete.</summary>
    public const string NotOwnerMessage = "This is a personal view. Only its owner can change or delete it.";

    /// <summary>The plain sentence for turning a shared view that somebody else created into a personal one.</summary>
    public const string NotCreatorMessage = "Only the person who created a view can make it personal.";

    /// <summary>The plain sentence for turning a shared view with no recorded creator (made before V204) into a personal one.</summary>
    public const string NoCreatorMessage = "This shared view has no recorded creator, so it can't be made personal. Save a copy as a personal view instead.";

    /// <summary>The plain sentence for a personal view with nobody to own it.</summary>
    public const string NoIdentityMessage = "A personal view needs a signed-in identity; this session has none.";

    /// <summary>
    /// Normalizes a folder name the way every writer stores it: trimmed, and an empty or blank name becomes null (the top level).
    /// Sets <paramref name="error"/> instead when the name is longer than <see cref="MaxFolderLength"/> or holds a control character.
    /// </summary>
    public static string? NormalizeFolder(string? folder, out string? error)
    {
        error = null;
        var trimmed = folder?.Trim();
        if (string.IsNullOrEmpty(trimmed))
        {
            return null;
        }

        if (trimmed.Length > MaxFolderLength)
        {
            error = $"'folder' exceeds the maximum length of {MaxFolderLength} characters.";
            return null;
        }

        foreach (var c in trimmed)
        {
            if (char.IsControl(c))
            {
                error = "'folder' must not contain control characters.";
                return null;
            }
        }

        return trimmed;
    }

    /// <summary>The 409 sentence for a duplicate name, naming the scope the name collided in.</summary>
    internal static string DuplicateMessage(string name, string? folder, bool personal) =>
        personal
            ? $"You already have a personal view named '{name}'."
            : folder is null
                ? $"A shared view named '{name}' already exists at the top level."
                : $"A shared view named '{name}' already exists in folder '{folder}'.";

    /// <summary>
    /// Lists saved views as lightweight summaries (no <c>definition</c>), ordered by name. Every shared view is listed. A personal view is
    /// listed only for its owner (<paramref name="callerIdentity"/>), or for everybody when <paramref name="allPersonal"/> is true (MCP,
    /// which has no person). <paramref name="folder"/> narrows to one folder ('' is the top level); null lists every folder. "Personal" is a
    /// list filter, not access control: <see cref="GetAsync"/> still returns any view.
    /// </summary>
    public async Task<IReadOnlyList<CustomViewSummary>> ListAsync(
        string? callerIdentity = null, bool allPersonal = false, string? folder = null, CancellationToken cancellationToken = default)
    {
        var results = new List<CustomViewSummary>();
        await using var command = _dataSource.CreateCommand(ListSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = allPersonal });                              // $1
        AddNullableText(command, callerIdentity);                                                                     // $2
        AddNullableText(command, folder);                                                                             // $3
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new CustomViewSummary(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetInt32(3),
                reader.GetDateTime(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetBoolean(8),
                reader.IsDBNull(9) ? null : reader.GetString(9)));
        }

        return results;
    }

    /// <summary>Reads one view in full (with <c>definition</c>), or <see cref="CustomViewResult.NotFound"/>.</summary>
    public async Task<CustomViewResult> GetAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(GetSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CustomViewResult.NotFound();
        }

        return new CustomViewResult.Ok(ReadFullView(reader));
    }

    /// <summary>
    /// Inserts a new view. Returns <see cref="CustomViewResult.Ok"/> with the stored row (version 1),
    /// <see cref="CustomViewResult.Conflict"/> on a duplicate name (23505), or
    /// <see cref="CustomViewResult.Invalid"/> on a failed argument guard. The caller is expected to have
    /// validated <paramref name="definitionJson"/>'s structure first.
    /// </summary>
    public async Task<CustomViewResult> CreateAsync(
        string name, string? description, string definitionJson, string? updatedBy, string? folder = null, bool personal = false,
        CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        folder = NormalizeFolder(folder, out var folderError);
        if (folderError is not null)
        {
            return new CustomViewResult.Invalid(folderError);
        }

        if (personal && string.IsNullOrWhiteSpace(updatedBy))
        {
            return new CustomViewResult.Invalid(NoIdentityMessage);
        }

        await using var command = _dataSource.CreateCommand(InsertSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $1
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $2
        AddNullableText(command, description);                                                                      // $3
        AddNullableText(command, updatedBy);                                                                        // $4 (also created_by)
        AddNullableText(command, folder);                                                                           // $5
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = personal });                               // $6

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new CustomViewResult.Ok(ReadFullView(reader));
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomViewResult.Conflict(DuplicateMessage(name, folder, personal));
        }
    }

    /// <summary>
    /// Updates a view under optimistic concurrency. Returns <see cref="CustomViewResult.Ok"/> with the
    /// new row (version bumped) on success; <see cref="CustomViewResult.Conflict"/> on a stale
    /// <paramref name="expectedVersion"/> OR a duplicate name; <see cref="CustomViewResult.NotFound"/> when
    /// the id no longer exists; <see cref="CustomViewResult.Invalid"/> on a failed argument guard.
    /// </summary>
    public async Task<CustomViewResult> UpdateAsync(
        long id, string name, string? description, string definitionJson, int expectedVersion, string? updatedBy,
        string? folder, bool personal, string? actor, CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        folder = NormalizeFolder(folder, out var folderError);
        if (folderError is not null)
        {
            return new CustomViewResult.Invalid(folderError);
        }

        if (personal && string.IsNullOrWhiteSpace(actor))
        {
            return new CustomViewResult.Invalid(NoIdentityMessage);
        }

        await using var command = _dataSource.CreateCommand(UpdateSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });                                     // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $2
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $3
        AddNullableText(command, description);                                                                      // $4
        AddNullableText(command, updatedBy);                                                                        // $5
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expectedVersion });                         // $6
        AddNullableText(command, folder);                                                                           // $7
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = personal });                               // $8
        AddNullableText(command, actor);                                                                            // $9

        try
        {
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    return new CustomViewResult.Ok(ReadFullView(reader));
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomViewResult.Conflict(DuplicateMessage(name, folder, personal));
        }

        /* 0 rows updated: the id is gone (NotFound), the actor may not change it (Forbidden) or the version moved under us (stale Conflict). */
        return await ClassifyMissedUpdateAsync(id, actor, personal, cancellationToken);
    }

    /// <summary>Deletes a view. Returns <see cref="CustomViewResult.Ok"/> (no body) when a row was deleted,
    /// <see cref="CustomViewResult.Forbidden"/> for a personal view <paramref name="actor"/> does not own (a null actor owns none),
    /// else <see cref="CustomViewResult.NotFound"/>.</summary>
    public async Task<CustomViewResult> DeleteAsync(long id, string? actor, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });                                     // $1
        AddNullableText(command, actor);                                                                            // $2
        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        if (affected > 0)
        {
            return new CustomViewResult.Ok(null);
        }

        /* Nothing deleted: the id is gone, or it is somebody else's personal view. */
        var missed = await ClassifyMissedUpdateAsync(id, actor, wantPersonal: false, cancellationToken);
        return missed is CustomViewResult.Forbidden ? missed : new CustomViewResult.NotFound();
    }

    private async Task<CustomViewResult> ClassifyMissedUpdateAsync(
        long id, string? actor, bool wantPersonal, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(VersionProbeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CustomViewResult.NotFound();
        }

        var currentVersion = reader.GetInt32(0);
        var isPersonal = reader.GetBoolean(1);
        var createdBy = reader.IsDBNull(2) ? null : reader.GetString(2);
        if (isPersonal && (actor is null || createdBy != actor))
        {
            return new CustomViewResult.Forbidden(NotOwnerMessage);
        }

        if (!isPersonal && wantPersonal && createdBy != actor)
        {
            return new CustomViewResult.Forbidden(createdBy is null ? NoCreatorMessage : NotCreatorMessage);
        }

        return new CustomViewResult.Conflict(
            $"This view was changed by someone else (current version {currentVersion}). Reload it and re-apply your edit.");
    }

    /// <summary>
    /// PURE argument guard the store applies before any write (the route's ValidateDefinition owns the
    /// definition STRUCTURE; this only guards presence/length so the store is safe to call on its own).
    /// Returns <see cref="CustomViewResult.Invalid"/> on a bad argument, else null.
    /// </summary>
    internal static CustomViewResult? ValidateArgs(string? name, string? definitionJson)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return new CustomViewResult.Invalid("'name' is required.");
        }

        if (name.Length > MaxNameLength)
        {
            return new CustomViewResult.Invalid($"'name' exceeds the maximum length of {MaxNameLength} characters.");
        }

        if (string.IsNullOrWhiteSpace(definitionJson))
        {
            return new CustomViewResult.Invalid("'definition' is required.");
        }

        return null;
    }

    /// <summary>Reads a full-view row (column order: id, name, definition, description, version, created_at,
    /// updated_at, updated_by, folder, personal, created_by — shared by <see cref="GetSql"/>/<see cref="InsertSql"/>/<see cref="UpdateSql"/>'s
    /// RETURNING). <c>definition</c> is jsonb, read as its text representation.</summary>
    private static CustomView ReadFullView(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetFieldValue<string>(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.GetInt32(4),
        reader.GetDateTime(5),
        reader.GetDateTime(6),
        reader.IsDBNull(7) ? null : reader.GetString(7),
        reader.IsDBNull(8) ? null : reader.GetString(8),
        reader.GetBoolean(9),
        reader.IsDBNull(10) ? null : reader.GetString(10));

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });
}

/// <summary>A saved custom view in full — the <c>definition</c> is the raw view JSON text (jsonb).</summary>
public sealed record CustomView(
    long Id,
    string Name,
    string DefinitionJson,
    string? Description,
    int Version,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    string? UpdatedBy,
    string? Folder,
    bool Personal,
    string? CreatedBy);

/// <summary>The lightweight list projection — every field except the <c>definition</c> body, plus the view
/// <c>Kind</c> extracted as a scalar (<c>null</c> when the stored definition declares none — the wire layer
/// defaults that to <c>"dashboard"</c>) so the list can badge/route a notebook vs a dashboard cheaply.</summary>
public sealed record CustomViewSummary(
    long Id,
    string Name,
    string? Description,
    int Version,
    DateTime UpdatedAt,
    string? UpdatedBy,
    string? Kind,
    string? Folder,
    bool Personal,
    string? CreatedBy);

/// <summary>
/// The discriminated outcome of a store operation. Closed (the private base constructor blocks external
/// subclassing): <see cref="Ok"/> (success — carries the row for read/create/update, null body for delete),
/// <see cref="NotFound"/>, <see cref="Conflict"/> (duplicate name in its scope OR stale optimistic-concurrency version), <see cref="Forbidden"/>
/// (a personal view that is not the caller's),
/// <see cref="Invalid"/> (a failed argument guard). The route layer maps these to 200/201/204, 404, 409, 403, 400.
/// </summary>
public abstract record CustomViewResult
{
    private CustomViewResult()
    {
    }

    public sealed record Ok(CustomView? View) : CustomViewResult;

    public sealed record NotFound : CustomViewResult;

    public sealed record Conflict(string Message) : CustomViewResult;

    /// <summary>The caller may not change or delete this view: a personal view is the owner's alone (#6214).</summary>
    public sealed record Forbidden(string Message) : CustomViewResult;

    public sealed record Invalid(string Message) : CustomViewResult;
}
