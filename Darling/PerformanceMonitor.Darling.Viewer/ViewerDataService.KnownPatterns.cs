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
using PerformanceMonitor.Common.KnownPatterns;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The viewer's known-pattern editor writes (#6334): list, add, edit and delete over <c>config.config_known_patterns</c>. The
/// web editor and the MCP write tools (#6398) write through the same functions. Straight to Postgres like the mute rules, through the three
/// store functions V214 adds, which are the one write path and bump the <c>config_version</c> reload beacon themselves. The shared
/// <see cref="KnownPatternInput.Validate"/> decides what a refused save says, so the editor, the web editor and the store
/// functions' callers all show one text.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>The first store schema version that has the known-pattern table and its functions.</summary>
    public const int KnownPatternsStoreSchemaVersion = 214;

    /// <summary>What the editor says on a store older than <see cref="KnownPatternsStoreSchemaVersion"/>.</summary>
    public const string KnownPatternsNeedNewerStoreMessage =
        "Known patterns need a newer store (schema v214 or later). Update or restart the Darling service so it migrates the store, then reopen the viewer.";

    public const string KnownPatternsSelectSql =
        "SELECT id, server_id, metric_name, time_of_day, time_zone, window_minutes, note, created_by, created_at_utc, expires_at_utc, updated_by, updated_at_utc " +
        "FROM config.config_known_patterns ORDER BY id";

    public const string KnownPatternInsertSql = "SELECT config.insert_known_pattern($1, $2, $3, $4, $5, $6, $7, $8)";

    public const string KnownPatternUpdateSql = "SELECT config.update_known_pattern($1, $2, $3, $4, $5, $6, $7, $8, $9)";

    public const string KnownPatternDeleteSql = "SELECT config.delete_known_pattern($1)";

    /// <summary>True when a store probed at <paramref name="probedVersion"/> has the known-pattern table. An unanswered probe
    /// (null) fails open, as every other gate on the probe does: a healthy store is never hidden from the editor by a probe hiccup,
    /// and a write that reaches a store without the table is translated to the schema-skew message.</summary>
    public static bool KnownPatternsAvailable(int? probedVersion) =>
        probedVersion is not int version || version >= KnownPatternsStoreSchemaVersion;

    /// <summary>Whether this store has the known-pattern table, from the probed schema version (cached per instance).</summary>
    public async Task<bool> KnownPatternsSupportedAsync(CancellationToken cancellationToken = default)
    {
        var storeVersion = _cachedStoreSchemaVersion ??= await GetStoreSchemaVersionAsync(cancellationToken);
        return KnownPatternsAvailable(storeVersion);
    }

    /// <summary>The machine's time zone as an IANA id, the form the entries store (a Windows id is converted). UTC when the
    /// machine's zone has no IANA name.</summary>
    public static string DefaultKnownPatternTimeZone()
    {
        var local = TimeZoneInfo.Local;
        if (TimeZoneInfo.TryConvertWindowsIdToIanaId(local.Id, out var iana) && !string.IsNullOrEmpty(iana))
        {
            return iana;
        }

        return local.Id.Contains('/', StringComparison.Ordinal) ? local.Id : "UTC";
    }

    /// <summary>All known-pattern entries, oldest id first — the editor's list.</summary>
    public async Task<List<KnownPattern>> GetKnownPatternsAsync(CancellationToken cancellationToken = default)
    {
        return await KnownPatternSkewAsync(async () =>
        {
            var entries = new List<KnownPattern>();
            await using var command = _dataSource.CreateCommand(KnownPatternsSelectSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
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
                        DateTime.SpecifyKind(reader.GetDateTime(8), DateTimeKind.Utc),
                        reader.IsDBNull(9) ? null : DateTime.SpecifyKind(reader.GetDateTime(9), DateTimeKind.Utc),
                        reader.IsDBNull(10) ? null : reader.GetString(10),
                        reader.IsDBNull(11) ? null : DateTime.SpecifyKind(reader.GetDateTime(11), DateTimeKind.Utc)));
                }
                catch (Exception ex) when (ex is InvalidCastException or ArgumentException or OverflowException or FormatException or InvalidOperationException)
                {
                    /* One row the model cannot hold must not hide every other entry; the table's CHECKs keep such rows out of a current store. */
                    ViewerLogger.Warn("KnownPatterns", $"Known pattern {id} cannot be read and is skipped: {ex.Message}");
                }
            }

            return entries;
        });
    }

    /// <summary>Adds an entry and returns its id. A refused input throws <see cref="ArgumentException"/> carrying the
    /// <see cref="KnownPatternInput.Validate"/> text; nothing is written.</summary>
    public async Task<long> InsertKnownPatternAsync(KnownPatternInput input, CancellationToken cancellationToken = default)
    {
        var clean = CheckedKnownPattern(input);
        return await KnownPatternSkewAsync(async () =>
        {
            await using var command = _dataSource.CreateCommand(KnownPatternInsertSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            AddKnownPatternScope(command, clean);
            /* The mute rules record no author; the entries do, and the viewer labels its writes the way it labels a queued command. */
            AddNullableText(command, RequestedBy());
            AddNullableTimestamp(command, clean.ExpiresAtUtc);
            return Convert.ToInt64(await ExecuteWriteScalarAsync(command, cancellationToken), CultureInfo.InvariantCulture);
        });
    }

    /// <summary>Rewrites an entry. False when no entry has that id (another operator deleted it).</summary>
    public async Task<bool> UpdateKnownPatternAsync(long id, KnownPatternInput input, CancellationToken cancellationToken = default)
    {
        var clean = CheckedKnownPattern(input);
        return await KnownPatternSkewAsync(async () =>
        {
            await using var command = _dataSource.CreateCommand(KnownPatternUpdateSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
            AddKnownPatternScope(command, clean);
            AddNullableTimestamp(command, clean.ExpiresAtUtc);
            /* The same label an add records as the author; the function stamps it with the time of the edit. */
            AddNullableText(command, RequestedBy());
            return (bool)(await ExecuteWriteScalarAsync(command, cancellationToken))!;
        });
    }

    /// <summary>Removes an entry. False when no entry has that id.</summary>
    public async Task<bool> DeleteKnownPatternAsync(long id, CancellationToken cancellationToken = default)
    {
        return await KnownPatternSkewAsync(async () =>
        {
            await using var command = _dataSource.CreateCommand(KnownPatternDeleteSql);
            command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Bigint, Value = id });
            return (bool)(await ExecuteWriteScalarAsync(command, cancellationToken))!;
        });
    }

    private static KnownPatternInput CheckedKnownPattern(KnownPatternInput input)
    {
        ArgumentNullException.ThrowIfNull(input);
        var refusal = input.Validate();
        if (refusal is not null)
        {
            throw new ArgumentException(refusal, nameof(input));
        }

        return input.Normalize();
    }

    private static void AddKnownPatternScope(NpgsqlCommand command, KnownPatternInput clean)
    {
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = (object?)clean.ServerId ?? DBNull.Value });
        AddNullableText(command, clean.MetricName);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Time, Value = (object?)clean.TimeOfDay ?? DBNull.Value });
        AddNullableText(command, clean.TimeZone);
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Integer, Value = clean.WindowMinutes });
        AddNullableText(command, clean.Note);
    }

    /// <summary>A store that lacks the table or a function reaches here as 42P01 or 42883; both read as the house schema-skew message.</summary>
    private static async Task<T> KnownPatternSkewAsync<T>(Func<Task<T>> call)
    {
        try
        {
            return await call();
        }
        catch (PostgresException ex) when (ex.SqlState is "DL404" or "DL409")
        {
            /* The store refused a server id that names no registered server (another operator removed it), or an add past the entry cap (DL409): the editor shows the sentence. */
            throw new ArgumentException(ex.MessageText);
        }
        catch (PostgresException ex) when (ex.SqlState is UndefinedColumnSqlState or UndefinedTableSqlState or "42883")
        {
            throw new ViewerSchemaSkewException(ex);
        }
    }
}
