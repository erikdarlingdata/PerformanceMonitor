/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
        "id, server_id, metric_name, time_of_day, time_zone, window_minutes, note, created_by, created_at_utc, expires_at_utc, updated_by, updated_at_utc";

    /// <summary>Rows already reported as unreadable (id and reason), so a row that stays bad is logged once per process, not once per read.</summary>
    private static readonly ConcurrentDictionary<string, byte> ReportedUnreadable = new(StringComparer.Ordinal);

    /// <summary>The service's logger for rows the reader skips; set once at startup by the first host that has one.</summary>
    private static ILogger? _sharedLogger;

    private readonly NpgsqlDataSource _postgres;
    private readonly ILogger? _logger;

    public PgKnownPatternStore(NpgsqlDataSource postgres, ILogger? logger = null)
    {
        _postgres = postgres ?? throw new ArgumentNullException(nameof(postgres));
        _logger = logger;
    }

    /// <summary>Gives rows skipped by readers that were built without a logger (the MCP tools) somewhere to report. First caller wins.</summary>
    internal static void UseSharedLogger(ILogger logger) => Interlocked.CompareExchange(ref _sharedLogger, logger, null);

    /// <summary>
    /// Every entry that can be read, oldest id first. A store fault propagates: an empty list must mean "no entries", never "could not
    /// read them". A single row that cannot be read (a value the model cannot hold) is skipped and reported once with its id and the
    /// reason, so one bad row never breaks the read of every other entry (the table's CHECKs keep such rows out, but a store older than
    /// them, or a restore, can still hold one).
    /// </summary>
    public async Task<IReadOnlyList<KnownPattern>> LoadAllAsync(CancellationToken cancellationToken = default)
    {
        var entries = new List<KnownPattern>();
        await using var command = _postgres.CreateCommand(
            $"SELECT {SelectColumns} FROM config.config_known_patterns ORDER BY id");
        command.CommandTimeout = CommandTimeoutSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            var id = reader.GetInt64(0);
            try
            {
                entries.Add(new KnownPattern(
                    id,
                    reader.IsDBNull(1) ? null : reader.GetInt32(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetFieldValue<TimeOnly>(3),
                    reader.IsDBNull(4) ? null : reader.GetString(4),
                    reader.GetInt32(5),
                    reader.GetString(6),
                    reader.IsDBNull(7) ? null : reader.GetString(7),
                    reader.GetDateTime(8),
                    reader.IsDBNull(9) ? null : reader.GetDateTime(9),
                    reader.IsDBNull(10) ? null : reader.GetString(10),
                    reader.IsDBNull(11) ? null : reader.GetDateTime(11)));
            }
            catch (Exception ex) when (ex is InvalidCastException or ArgumentException or OverflowException or FormatException or InvalidOperationException)
            {
                ReportUnreadable(id, ex);
            }
        }

        return entries;
    }

    private void ReportUnreadable(long id, Exception ex)
    {
        var reason = ex.Message.Length > 200 ? ex.Message[..200] : ex.Message;
        if (!ReportedUnreadable.TryAdd(id.ToString(System.Globalization.CultureInfo.InvariantCulture) + ":" + reason, 0))
        {
            return;
        }

        (_logger ?? _sharedLogger)?.LogWarning("Known pattern {Id} cannot be read and is skipped: {Reason}", id, reason);
    }

    /// <summary>
    /// The one place a known-pattern write's raw fields become a checked <see cref="KnownPatternInput"/> (#6398): the web routes (after their JSON
    /// parse) and the MCP write tools both call it, so they refuse the same things in the same words. Returns the input, or the reason it is
    /// refused: a time of day or expiry that does not parse, <see cref="KnownPatternInput.Validate"/>'s own text, then a server id that
    /// <paramref name="serverLookup"/> says is not registered.
    /// </summary>
    internal static async Task<(KnownPatternInput? Input, string? Refusal)> BuildInputAsync(
        int? serverId, string? metricName, string? timeOfDayText, string? timeZone, int? windowMinutes, string? note, string? expiresText,
        Func<int, Task<string?>> serverLookup)
    {
        TimeOnly? timeOfDay = null;
        if (!string.IsNullOrWhiteSpace(timeOfDayText))
        {
            if (!TimeOnly.TryParseExact(timeOfDayText.Trim(), ["HH:mm", "H:mm", "HH:mm:ss"], System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.None, out var parsedTime))
            {
                return (null, "time_of_day must be a time such as 02:00.");
            }

            timeOfDay = parsedTime;
        }

        DateTime? expires = null;
        if (!string.IsNullOrWhiteSpace(expiresText))
        {
            if (!DateTime.TryParse(expiresText.Trim(), System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.AssumeUniversal | System.Globalization.DateTimeStyles.AdjustToUniversal, out var parsedExpiry))
            {
                return (null, "expires_at_utc must be a date and time such as 2026-12-31T00:00:00Z.");
            }

            expires = parsedExpiry;
        }

        var input = new KnownPatternInput(serverId, metricName, timeOfDay, timeZone, windowMinutes, note, expires);

        /* The reason first, so a bad note reads the same here as in the Viewer; then the server, which only the store behind the registry can say. */
        var refusal = input.Validate();
        if (refusal is not null)
        {
            return (null, refusal);
        }

        if (serverId is { } id && await serverLookup(id) is null)
        {
            return (null, $"server_id {id} is not a registered server.");
        }

        return (input, null);
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
        try
        {
            return Convert.ToInt64(await command.ExecuteScalarAsync(cancellationToken), System.Globalization.CultureInfo.InvariantCulture);
        }
        catch (PostgresException ex) when (ex.SqlState == "DL404")
        {
            throw new ArgumentException(ex.MessageText, nameof(input));
        }
    }

    /// <summary>Replaces an entry's scope, window, note and expiry (not its creator or creation time) and stamps the editor and the time of
    /// the edit. False when no such entry exists. A server id that names no registered server throws <see cref="ArgumentException"/>.</summary>
    public async Task<bool> UpdateAsync(long id, KnownPatternInput input, string? updatedBy, CancellationToken cancellationToken = default)
    {
        var clean = Checked(input);
        await using var command = _postgres.CreateCommand(
            "SELECT config.update_known_pattern($1, $2, $3, $4, $5, $6, $7, $8, $9)");
        command.CommandTimeout = CommandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
        AddScope(command, clean);
        command.Parameters.Add(Timestamp(clean.ExpiresAtUtc));
        command.Parameters.Add(Text(string.IsNullOrWhiteSpace(updatedBy) ? null : updatedBy.Trim()));
        try
        {
            return (bool)(await command.ExecuteScalarAsync(cancellationToken))!;
        }
        catch (PostgresException ex) when (ex.SqlState == "DL404")
        {
            throw new ArgumentException(ex.MessageText, nameof(input));
        }
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
