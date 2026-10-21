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
using PerformanceMonitor.Common.KnownPatterns;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #6334: the known-pattern entries (<c>config.config_known_patterns</c>, V214) over a Postgres data source.
///
/// <para><b>Reads</b> take the table as it is: <see cref="LoadAllAsync"/> returns every entry, expired or not, because an alert is
/// matched against the entries in force at ITS time (<see cref="KnownPatternMatcher"/>). The table is small (an operator types each
/// row), so a reader loads it once per call and matches in memory; it never queries per alert.</para>
///
/// <para><b>Writes</b> go through the owner-rights functions <c>config.insert_known_pattern</c>, <c>update_known_pattern</c> and
/// <c>delete_known_pattern</c> (<see cref="DarlingManagedRoles.BuildKnownPatternFunctionsSql"/>), the only write the viewer and admin roles
/// hold on the table. Input is checked first with <see cref="KnownPatternInput.Validate"/>, so a refusal reads the same in every
/// editor; a refused write throws <see cref="ArgumentException"/> carrying that reason. The writes are operator-only: no MCP tool calls
/// them.</para>
/// </summary>
public sealed class PgKnownPatternStore
{
    private const int CommandTimeoutSeconds = 30;

    private const string SelectColumns =
        "id, server_id, metric_name, time_of_day, time_zone, window_minutes, note, created_by, created_at_utc, expires_at_utc";

    private readonly NpgsqlDataSource _postgres;

    public PgKnownPatternStore(NpgsqlDataSource postgres)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
    }

    /// <summary>Every entry, oldest id first. A store fault propagates: an empty list must mean "no entries", never "could not read them".</summary>
    public async Task<IReadOnlyList<KnownPattern>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        var entries = new List<KnownPattern>();
        await using var command = _postgres.CreateCommand(
            $"SELECT {SelectColumns} FROM config.config_known_patterns ORDER BY id");
        command.CommandTimeout = CommandTimeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            entries.Add(new KnownPattern(
                reader.GetInt64(0),
                reader.IsDBNull(1) ? null : reader.GetInt32(1),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.IsDBNull(3) ? null : reader.GetFieldValue<TimeOnly>(3),
                reader.IsDBNull(4) ? null : reader.GetString(4),
                reader.GetInt32(5),
                reader.GetString(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetDateTime(8),
                reader.IsDBNull(9) ? null : reader.GetDateTime(9)));
        }

        return entries;
    }

    /// <summary>Adds an entry and returns its id. Throws <see cref="ArgumentException"/> with the reason when the input is refused.</summary>
    public async Task<long> InsertAsync(KnownPatternInput input, string? createdBy, CancellationToken cancellationToken = default)
    {
        var clean = Checked(input);
        await using var command = _postgres.CreateCommand(
            "SELECT config.insert_known_pattern($1, $2, $3, $4, $5, $6, $7, $8)");
        command.CommandTimeout = CommandTimeoutSeconds;
        AddScope(command, clean);
        command.Parameters.Add(Text(string.IsNullOrWhiteSpace(createdBy) ? null : createdBy.Trim()));
        command.Parameters.Add(Timestamp(clean.ExpiresAtUtc));
        return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
    }

    /// <summary>Replaces an entry's scope, window, note and expiry (not its creator or creation time). False when no such entry exists.</summary>
    public async Task<bool> UpdateAsync(long id, KnownPatternInput input, CancellationToken cancellationToken = default)
    {
        var clean = Checked(input);
        await using var command = _postgres.CreateCommand(
            "SELECT config.update_known_pattern($1, $2, $3, $4, $5, $6, $7, $8)");
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        AddScope(command, clean);
        command.Parameters.Add(Timestamp(clean.ExpiresAtUtc));
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    /// <summary>Deletes an entry. False when no such entry exists.</summary>
    public async Task<bool> DeleteAsync(long id, CancellationToken cancellationToken = default)
    {
        await using var command = _postgres.CreateCommand("SELECT config.delete_known_pattern($1)");
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
    }

    private static KnownPatternInput Checked(KnownPatternInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var refusal = input.Validate();
        if (refusal is not null)
        {
            throw new ArgumentException(refusal, nameof(input));
        }

        return input.Normalize();
    }

    /// <summary>Server, metric, time of day, zone, window, note: the six scope parameters the insert and update functions share, in order.</summary>
    private static void AddScope(NpgsqlCommand command, KnownPatternInput clean)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)clean.ServerId ?? DBNull.Value });
        command.Parameters.Add(Text(clean.MetricName));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Time, Value = (object?)clean.TimeOfDay ?? DBNull.Value });
        command.Parameters.Add(Text(clean.TimeZone));
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = clean.WindowMinutes });
        command.Parameters.Add(Text(clean.Note));
    }

    private static NpgsqlParameter Text(string? value) =>
        new() { NpgsqlDbType = NpgsqlDbType.Text, Value = (object?)value ?? DBNull.Value };

    private static NpgsqlParameter Timestamp(DateTime? value) =>
        new()
        {
            NpgsqlDbType = NpgsqlDbType.Timestamp,
            Value = value is { } v ? DateTime.SpecifyKind(v, DateTimeKind.Unspecified) : DBNull.Value,
        };
}
