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

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// One database in the newest capture (#6873), as the collector stored it. Times are UTC. The state words are the collector's:
/// <c>checkdb_state</c> (checked, never, unknown), <c>full_backup_state</c> and <c>log_backup_state</c> (recorded, none_in_window,
/// not_expected, unavailable). No overdue judgment is stored: <see cref="DatabaseProtectionBands"/> makes it.
/// </summary>
public sealed record DatabaseProtectionFact(
    string DatabaseName,
    string? StateDesc,
    string? RecoveryModelDesc,
    DateTime? CreateDate,
    string? ReplicaRole,
    string? Platform,
    DateTime? LastGoodCheckDbTime,
    string? CheckDbState,
    DateTime? LastFullBackupTime,
    string? FullBackupState,
    DateTime? LastLogBackupTime,
    string? LogBackupState);

/// <summary>What <see cref="DarlingDatabaseProtectionReader.ReadAsync"/> found: one entry per database in the newest capture, by name.</summary>
/// <param name="Databases">The databases of the newest capture. Empty when the window holds no capture.</param>
/// <param name="NewestCapture">The newest capture in the window (UTC), or null when the window holds none.</param>
public sealed record DatabaseProtectionRead(IReadOnlyList<DatabaseProtectionFact> Databases, DateTime? NewestCapture)
{
    /// <summary>The window holds at least one capture.</summary>
    public bool HasCapture => NewestCapture is not null;
}

/// <summary>
/// #6873: per-database last good CHECKDB and last full and log backup, from <c>collect.database_protection</c> (V223). One pass stamps one
/// <c>collection_time</c> for every database, so the newest capture in the window is the current set of databases: a dropped database
/// leaves the next capture and is not listed, though its older rows stay until retention. Bounded by <c>server_id</c> and the window
/// on <c>collection_time</c> (<c>idx_database_protection_time</c>).
/// </summary>
public static class DarlingDatabaseProtectionReader
{
    /// <summary>The newest capture's rows. $1 server, $2 window start, $3 window end.</summary>
    public const string NewestCaptureSql = @"
SELECT collection_time, database_name, state_desc, recovery_model_desc, create_date, replica_role, platform,
       last_good_checkdb_time, checkdb_state, last_full_backup_time, full_backup_state, last_log_backup_time, log_backup_state
FROM collect.database_protection
WHERE server_id = $1
AND   collection_time = (
        SELECT MAX(collection_time) FROM collect.database_protection
        WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3)
ORDER BY database_name";

    /// <summary>Reads the newest capture of one server in the window <paramref name="start"/> to <paramref name="end"/> (UTC).</summary>
    public static async Task<DatabaseProtectionRead> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime start, DateTime end,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        await using var command = postgres.CreateCommand(NewestCaptureSql);
        command.CommandTimeout = commandTimeoutSeconds;
        command.Parameters.AddWithValue(serverId);
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(start, DateTimeKind.Unspecified));
        command.Parameters.AddWithValue(NpgsqlDbType.Timestamp, DateTime.SpecifyKind(end, DateTimeKind.Unspecified));

        DateTime? newest = null;
        var databases = new List<DatabaseProtectionFact>();
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            newest = reader.GetDateTime(0);
            string? Text(int i) => reader.IsDBNull(i) ? null : reader.GetString(i);
            DateTime? Time(int i) => reader.IsDBNull(i) ? null : reader.GetDateTime(i);
            databases.Add(new DatabaseProtectionFact(
                Text(1) ?? string.Empty, Text(2), Text(3), Time(4), Text(5), Text(6),
                Time(7), Text(8), Time(9), Text(10), Time(11), Text(12)));
        }

        return new DatabaseProtectionRead(databases, newest);
    }
}
