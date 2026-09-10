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
    /// a notebook vs a dashboard without fetching each definition.</summary>
    public const string ListSql = @"
SELECT id, name, description, version, updated_at, updated_by, (definition->>'kind') AS kind
FROM custom_views
ORDER BY name";

    /// <summary>The full single-view read (includes <c>definition</c>). $1 id.</summary>
    public const string GetSql = @"
SELECT id, name, definition, description, version, created_at, updated_at, updated_by
FROM custom_views
WHERE id = $1";

    /// <summary>Inserts a new view at version 1. $1 name, $2 definition (jsonb), $3 description, $4 updated_by.</summary>
    public const string InsertSql = @"
INSERT INTO custom_views (name, definition, description, version, created_at, updated_at, updated_by)
VALUES ($1, $2, $3, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), $4)
RETURNING id, name, definition, description, version, created_at, updated_at, updated_by";

    /// <summary>Optimistic-concurrency update. $1 id, $2 name, $3 definition (jsonb), $4 description,
    /// $5 updated_by, $6 expected_version. Returns the row only when the expected version matched.</summary>
    public const string UpdateSql = @"
UPDATE custom_views
SET name = $2,
    definition = $3,
    description = $4,
    version = version + 1,
    updated_at = (now() AT TIME ZONE 'UTC'),
    updated_by = $5
WHERE id = $1 AND version = $6
RETURNING id, name, definition, description, version, created_at, updated_at, updated_by";

    /// <summary>Disambiguates a 0-rowcount update: the current version, or no row when the id is gone. $1 id.</summary>
    public const string VersionProbeSql = "SELECT version FROM custom_views WHERE id = $1";

    /// <summary>Deletes a view by id. $1 id.</summary>
    public const string DeleteSql = "DELETE FROM custom_views WHERE id = $1";

    /// <summary>Postgres <c>unique_violation</c> — a duplicate <c>name</c> insert/rename.</summary>
    private const string UniqueViolation = "23505";

    /// <summary>An abuse bound on the view name (the column itself is unbounded text).</summary>
    public const int MaxNameLength = 200;

    /// <summary>Lists every saved view as a lightweight summary (no <c>definition</c>), ordered by name.</summary>
    public async Task<IReadOnlyList<CustomViewSummary>> ListAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<CustomViewSummary>();
        await using var command = _dataSource.CreateCommand(ListSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
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
                reader.IsDBNull(6) ? null : reader.GetString(6)));
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
        string name, string? description, string definitionJson, string? updatedBy, CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        await using var command = _dataSource.CreateCommand(InsertSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $1
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $2
        AddNullableText(command, description);                                                                      // $3
        AddNullableText(command, updatedBy);                                                                        // $4

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new CustomViewResult.Ok(ReadFullView(reader));
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomViewResult.Conflict($"A view named '{name}' already exists.");
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
        CancellationToken cancellationToken = default)
    {
        var invalid = ValidateArgs(name, definitionJson);
        if (invalid is not null)
        {
            return invalid;
        }

        await using var command = _dataSource.CreateCommand(UpdateSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });                                     // $1
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = name });                                 // $2
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Jsonb, Value = definitionJson }); // $3
        AddNullableText(command, description);                                                                      // $4
        AddNullableText(command, updatedBy);                                                                        // $5
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expectedVersion });                         // $6

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
            return new CustomViewResult.Conflict($"A view named '{name}' already exists.");
        }

        /* 0 rows updated: the id is gone (NotFound) or the version moved under us (stale Conflict). */
        return await ClassifyMissedUpdateAsync(id, cancellationToken);
    }

    /// <summary>Deletes a view. Returns <see cref="CustomViewResult.Ok"/> (no body) when a row was deleted,
    /// else <see cref="CustomViewResult.NotFound"/>.</summary>
    public async Task<CustomViewResult> DeleteAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        return affected > 0 ? new CustomViewResult.Ok(null) : new CustomViewResult.NotFound();
    }

    private async Task<CustomViewResult> ClassifyMissedUpdateAsync(long id, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(VersionProbeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var current = await command.ExecuteScalarAsync(cancellationToken);
        if (current is null or DBNull)
        {
            return new CustomViewResult.NotFound();
        }

        var currentVersion = Convert.ToInt32(current, CultureInfo.InvariantCulture);
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
    /// updated_at, updated_by — shared by <see cref="GetSql"/>/<see cref="InsertSql"/>/<see cref="UpdateSql"/>'s
    /// RETURNING). <c>definition</c> is jsonb, read as its text representation.</summary>
    private static CustomView ReadFullView(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetFieldValue<string>(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.GetInt32(4),
        reader.GetDateTime(5),
        reader.GetDateTime(6),
        reader.IsDBNull(7) ? null : reader.GetString(7));

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
    string? UpdatedBy);

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
    string? Kind);

/// <summary>
/// The discriminated outcome of a store operation. Closed (the private base constructor blocks external
/// subclassing): <see cref="Ok"/> (success — carries the row for read/create/update, null body for delete),
/// <see cref="NotFound"/>, <see cref="Conflict"/> (duplicate name OR stale optimistic-concurrency version),
/// <see cref="Invalid"/> (a failed argument guard). The route layer maps these to 200/201/204, 404, 409, 400.
/// </summary>
public abstract record CustomViewResult
{
    private CustomViewResult()
    {
    }

    public sealed record Ok(CustomView? View) : CustomViewResult;

    public sealed record NotFound : CustomViewResult;

    public sealed record Conflict(string Message) : CustomViewResult;

    public sealed record Invalid(string Message) : CustomViewResult;
}
