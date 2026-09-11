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
/// Typed CRUD over <c>config.custom_alert_rules</c> (#3285) — the user-authored alert rules. Deliberately the
/// same store discipline as <see cref="CustomViewStore"/>: public-const SQL, bound <c>$N</c> parameters,
/// <c>definition</c> bound as <c>jsonb</c>, bare table name (the pool's <c>search_path = collect, config,
/// public</c> resolves <c>custom_alert_rules</c> to <c>config.custom_alert_rules</c>). The CRUD callers (the
/// web editor + MCP tools) connect on the least-privilege viewer/mcp pool, which carries the single narrow
/// <c>INSERT/UPDATE/DELETE ON config.custom_alert_rules</c> grant (see <c>DarlingManagedRoles</c>); the
/// <see cref="CustomAlertEvaluator"/> instead instantiates this store on the worker's owner pool to
/// <see cref="ListEnabledAsync"/> each sweep.
///
/// <para>Adds one column beyond the <c>custom_views</c> shape: <c>enabled</c>, which the evaluator filters on
/// so a rule can be paused without deleting it (and its accumulated <c>custom_alert_state</c>). Optimistic
/// concurrency (<c>WHERE id = $1 AND version = $n</c>) and the 0-rowcount NotFound-vs-Conflict disambiguation
/// are identical to <see cref="CustomViewStore"/>; the route/tool layer owns the definition STRUCTURE
/// validation (the compose <c>TryParsePanel</c> authority + the rule validator), this store adds only light
/// argument guards so it is safe to call independently.</para>
/// </summary>
public sealed class CustomAlertRuleStore
{
    private readonly NpgsqlDataSource _dataSource;

    public CustomAlertRuleStore(NpgsqlDataSource dataSource)
    {
        _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
    }

    /// <summary>The lightweight list read: the (potentially large) <c>definition</c> body is never selected.</summary>
    public const string ListSql = @"
SELECT id, name, description, enabled, version, updated_at, updated_by
FROM custom_alert_rules
ORDER BY name";

    /// <summary>The full single-rule read (includes <c>definition</c>). $1 id.</summary>
    public const string GetSql = @"
SELECT id, name, definition, description, enabled, version, created_at, updated_at, updated_by
FROM custom_alert_rules
WHERE id = $1";

    /// <summary>Every ENABLED rule in full — the evaluator's per-sweep read (a handful of rows). Ordered by
    /// id so evaluation order is stable across sweeps.</summary>
    public const string ListEnabledSql = @"
SELECT id, name, definition, description, enabled, version, created_at, updated_at, updated_by
FROM custom_alert_rules
WHERE enabled
ORDER BY id";

    /// <summary>Inserts a new rule at version 1. $1 name, $2 definition (jsonb), $3 description, $4 enabled, $5 updated_by.</summary>
    public const string InsertSql = @"
INSERT INTO custom_alert_rules (name, definition, description, enabled, version, created_at, updated_at, updated_by)
VALUES ($1, $2, $3, $4, 1, (now() AT TIME ZONE 'UTC'), (now() AT TIME ZONE 'UTC'), $5)
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Optimistic-concurrency update. $1 id, $2 name, $3 definition (jsonb), $4 description,
    /// $5 enabled, $6 updated_by, $7 expected_version. Returns the row only when the expected version matched.</summary>
    public const string UpdateSql = @"
UPDATE custom_alert_rules
SET name = $2,
    definition = $3,
    description = $4,
    enabled = $5,
    version = version + 1,
    updated_at = (now() AT TIME ZONE 'UTC'),
    updated_by = $6
WHERE id = $1 AND version = $7
RETURNING id, name, definition, description, enabled, version, created_at, updated_at, updated_by";

    /// <summary>Disambiguates a 0-rowcount update: the current version, or no row when the id is gone. $1 id.</summary>
    public const string VersionProbeSql = "SELECT version FROM custom_alert_rules WHERE id = $1";

    /// <summary>Deletes a rule by id (its <c>custom_alert_state</c> rows cascade). $1 id.</summary>
    public const string DeleteSql = "DELETE FROM custom_alert_rules WHERE id = $1";

    /// <summary>Postgres <c>unique_violation</c> — a duplicate <c>name</c> insert/rename.</summary>
    private const string UniqueViolation = "23505";

    /// <summary>An abuse bound on the rule name (the column itself is unbounded text).</summary>
    public const int MaxNameLength = 200;

    /// <summary>Lists every rule as a lightweight summary (no <c>definition</c>), ordered by name.</summary>
    public async Task<IReadOnlyList<CustomAlertRuleSummary>> ListAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<CustomAlertRuleSummary>();
        await using var command = _dataSource.CreateCommand(ListSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(new CustomAlertRuleSummary(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetBoolean(3),
                reader.GetInt32(4),
                reader.GetDateTime(5),
                reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return results;
    }

    /// <summary>Reads every ENABLED rule in full — the evaluator's per-sweep read.</summary>
    public async Task<IReadOnlyList<CustomAlertRule>> ListEnabledAsync(CancellationToken cancellationToken = default)
    {
        var results = new List<CustomAlertRule>();
        await using var command = _dataSource.CreateCommand(ListEnabledSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            results.Add(ReadFullRule(reader));
        }

        return results;
    }

    /// <summary>Reads one rule in full (with <c>definition</c>), or <see cref="CustomAlertRuleResult.NotFound"/>.</summary>
    public async Task<CustomAlertRuleResult> GetAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(GetSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return new CustomAlertRuleResult.NotFound();
        }

        return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
    }

    /// <summary>
    /// Inserts a new rule. Returns <see cref="CustomAlertRuleResult.Ok"/> with the stored row (version 1),
    /// <see cref="CustomAlertRuleResult.Conflict"/> on a duplicate name (23505), or
    /// <see cref="CustomAlertRuleResult.Invalid"/> on a failed argument guard. The caller is expected to have
    /// validated <paramref name="definitionJson"/>'s structure first.
    /// </summary>
    public async Task<CustomAlertRuleResult> CreateAsync(
        string name, string? description, string definitionJson, bool enabled, string? updatedBy,
        CancellationToken cancellationToken = default)
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
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });                                 // $4
        AddNullableText(command, updatedBy);                                                                        // $5

        try
        {
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            await reader.ReadAsync(cancellationToken);
            return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }
    }

    /// <summary>
    /// Updates a rule under optimistic concurrency. Returns <see cref="CustomAlertRuleResult.Ok"/> with the
    /// new row (version bumped) on success; <see cref="CustomAlertRuleResult.Conflict"/> on a stale
    /// <paramref name="expectedVersion"/> OR a duplicate name; <see cref="CustomAlertRuleResult.NotFound"/>
    /// when the id no longer exists; <see cref="CustomAlertRuleResult.Invalid"/> on a failed argument guard.
    /// </summary>
    public async Task<CustomAlertRuleResult> UpdateAsync(
        long id, string name, string? description, string definitionJson, bool enabled, int expectedVersion,
        string? updatedBy, CancellationToken cancellationToken = default)
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
        command.Parameters.Add(new NpgsqlParameter<bool> { TypedValue = enabled });                                 // $5
        AddNullableText(command, updatedBy);                                                                        // $6
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = expectedVersion });                         // $7

        try
        {
            await using (var reader = await command.ExecuteReaderAsync(cancellationToken))
            {
                if (await reader.ReadAsync(cancellationToken))
                {
                    return new CustomAlertRuleResult.Ok(ReadFullRule(reader));
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState == UniqueViolation)
        {
            return new CustomAlertRuleResult.Conflict($"An alert rule named '{name}' already exists.");
        }

        /* 0 rows updated: the id is gone (NotFound) or the version moved under us (stale Conflict). */
        return await ClassifyMissedUpdateAsync(id, cancellationToken);
    }

    /// <summary>Deletes a rule (its <c>custom_alert_state</c> rows cascade). Returns
    /// <see cref="CustomAlertRuleResult.Ok"/> (no body) when a row was deleted, else
    /// <see cref="CustomAlertRuleResult.NotFound"/>. The caller force-resolves any open incident BEFORE
    /// calling this — the cascade drops the state rows, so the resolve delivery must happen first.</summary>
    public async Task<CustomAlertRuleResult> DeleteAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _dataSource.CreateCommand(DeleteSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var affected = await command.ExecuteNonQueryAsync(cancellationToken);
        return affected > 0 ? new CustomAlertRuleResult.Ok(null) : new CustomAlertRuleResult.NotFound();
    }

    private async Task<CustomAlertRuleResult> ClassifyMissedUpdateAsync(long id, CancellationToken cancellationToken)
    {
        await using var command = _dataSource.CreateCommand(VersionProbeSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<long> { TypedValue = id });
        var current = await command.ExecuteScalarAsync(cancellationToken);
        if (current is null or DBNull)
        {
            return new CustomAlertRuleResult.NotFound();
        }

        var currentVersion = Convert.ToInt32(current, CultureInfo.InvariantCulture);
        return new CustomAlertRuleResult.Conflict(
            $"This rule was changed by someone else (current version {currentVersion}). Reload it and re-apply your edit.");
    }

    /// <summary>
    /// PURE argument guard the store applies before any write (the route/tool's validator owns the
    /// definition STRUCTURE; this only guards presence/length so the store is safe to call on its own).
    /// Returns <see cref="CustomAlertRuleResult.Invalid"/> on a bad argument, else null.
    /// </summary>
    internal static CustomAlertRuleResult? ValidateArgs(string? name, string? definitionJson)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return new CustomAlertRuleResult.Invalid("'name' is required.");
        }

        if (name.Length > MaxNameLength)
        {
            return new CustomAlertRuleResult.Invalid($"'name' exceeds the maximum length of {MaxNameLength} characters.");
        }

        if (string.IsNullOrWhiteSpace(definitionJson))
        {
            return new CustomAlertRuleResult.Invalid("'definition' is required.");
        }

        return null;
    }

    /// <summary>Reads a full-rule row (column order: id, name, definition, description, enabled, version,
    /// created_at, updated_at, updated_by — shared by the SELECT/RETURNING lists). <c>definition</c> is jsonb,
    /// read as its text representation.</summary>
    private static CustomAlertRule ReadFullRule(NpgsqlDataReader reader) => new(
        reader.GetInt64(0),
        reader.GetString(1),
        reader.GetFieldValue<string>(2),
        reader.IsDBNull(3) ? null : reader.GetString(3),
        reader.GetBoolean(4),
        reader.GetInt32(5),
        reader.GetDateTime(6),
        reader.GetDateTime(7),
        reader.IsDBNull(8) ? null : reader.GetString(8));

    private static void AddNullableText(NpgsqlCommand command, string? value) =>
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value });
}

/// <summary>A saved custom alert rule in full — the <c>definition</c> is the raw rule JSON text (jsonb).</summary>
public sealed record CustomAlertRule(
    long Id,
    string Name,
    string DefinitionJson,
    string? Description,
    bool Enabled,
    int Version,
    DateTime CreatedAt,
    DateTime UpdatedAt,
    string? UpdatedBy);

/// <summary>The lightweight list projection — every field except the <c>definition</c> body.</summary>
public sealed record CustomAlertRuleSummary(
    long Id,
    string Name,
    string? Description,
    bool Enabled,
    int Version,
    DateTime UpdatedAt,
    string? UpdatedBy);

/// <summary>
/// The discriminated outcome of a store operation. Closed (the private base constructor blocks external
/// subclassing): <see cref="Ok"/> (success — carries the row for read/create/update, null body for delete),
/// <see cref="NotFound"/>, <see cref="Conflict"/> (duplicate name OR stale optimistic-concurrency version),
/// <see cref="Invalid"/> (a failed argument guard). The route layer maps these to 200/201/204, 404, 409, 400.
/// </summary>
public abstract record CustomAlertRuleResult
{
    private CustomAlertRuleResult()
    {
    }

    public sealed record Ok(CustomAlertRule? Rule) : CustomAlertRuleResult;

    public sealed record NotFound : CustomAlertRuleResult;

    public sealed record Conflict(string Message) : CustomAlertRuleResult;

    public sealed record Invalid(string Message) : CustomAlertRuleResult;
}
