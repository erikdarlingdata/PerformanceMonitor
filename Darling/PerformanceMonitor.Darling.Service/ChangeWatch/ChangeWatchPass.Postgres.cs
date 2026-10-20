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
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.ChangeWatch;

/// <summary>
/// #6234, the PostgreSQL pass: the change sources of a PostgreSQL target. Server-wide settings and per-database or per-role
/// overrides are the SAME statements <c>get_pg_config_changes</c> reads (<see cref="DarlingPgServerConfigReader.ConfigChangesSql"/>,
/// <see cref="DarlingPgServerConfigReader.ScopedConfigChangesSql"/>, through <see cref="DarlingPgServerConfigReader.GetConfigChangesAsync"/>);
/// extension installs, drops and version moves come from the daily extension snapshots (<c>pg_extension_availability</c>), diffed per
/// database and extension. Each source has its own mark (<see cref="ChangeWatchStore.PostgresMarkSources"/>) and reads only snapshots
/// from the newest one at or before the mark, so a tick costs the new rows.
/// </summary>
public sealed partial class ChangeWatchPass
{
    /// <summary>The most setting changes one tick reads. A window's worth of settings is a few dozen; the cap only bounds a catch-up.</summary>
    private const int PostgresReadCap = 1000;

    /* The baseline of a snapshot diff: the newest capture at or before the mark ($2), looked for no further back than $3. The first
       capture after the mark is diffed against it. The config collector is hourly, the extension collector daily. */
    private const string ConfigBaselineSql = @"
SELECT max(collection_time) FROM pg_server_config WHERE server_id = $1 AND collection_time <= $2 AND collection_time >= $3";

    private const string ExtensionBaselineSql = @"
SELECT max(collection_time) FROM pg_extension_availability WHERE server_id = $1 AND collection_time <= $2 AND collection_time >= $3";

    private const string ExtensionChangesSql = @"
SELECT collection_time, database_name, extension_name, prev_version, installed_version
FROM (
    SELECT collection_time, database_name, extension_name, installed_version,
           LAG(installed_version) OVER w AS prev_version, row_number() OVER w AS rn
    FROM pg_extension_availability
    WHERE server_id = $1 AND collection_time >= $2 AND collection_time <= $3 AND extension_name IS NOT NULL
    WINDOW w AS (PARTITION BY database_name, extension_name ORDER BY collection_time)
) AS x
WHERE rn > 1 AND collection_time > $4 AND installed_version IS DISTINCT FROM prev_version";

    /// <summary>The changes of one PostgreSQL target NEWER than each source's mark and no later than <paramref name="end"/>.</summary>
    internal async Task<List<ChangeWatchChange>> ReadPostgresChangesAsync(
        int serverId, IReadOnlyDictionary<string, DateTime> marks, DateTime end, CancellationToken cancellationToken)
    {
        var changes = new List<ChangeWatchChange>();
        var naiveEnd = Naive(end);

        var serverMark = Naive(marks["pg_server_config"]);
        var scopedMark = Naive(marks["pg_scoped_config"]);
        var configBaseline = await NewestAtOrBeforeAsync(ConfigBaselineSql, serverId, Earlier(serverMark, scopedMark), TimeSpan.FromDays(2), cancellationToken);

        /* One reader call serves both statements. Each row is kept only when it is newer than ITS source's mark. */
        foreach (var row in await DarlingPgServerConfigReader.GetConfigChangesAsync(
                     _readSource, serverId, configBaseline, naiveEnd, PostgresReadCap, cancellationToken))
        {
            var time = Naive(row.ChangedAtUtc);
            var scoped = row.DatabaseName is not null || row.RoleName is not null;
            if (time <= (scoped ? scopedMark : serverMark))
            {
                continue;
            }

            if (!scoped)
            {
                changes.Add(new ChangeWatchChange($"pg_config:{row.Name}:{Stamp(time)}", "pg_config", Utc(time),
                    $"server setting {row.Name}: changed from {Value(row.OldValue)} to {Value(row.NewValue)}"));
            }
            else
            {
                var scope = row.DatabaseName is not null && row.RoleName is not null
                    ? $"database {row.DatabaseName}, role {row.RoleName}"
                    : row.DatabaseName is not null ? $"database {row.DatabaseName}" : $"role {row.RoleName}";
                changes.Add(new ChangeWatchChange(
                    $"pg_scoped_config:{row.DatabaseName}:{row.RoleName}:{row.Name}:{Stamp(time)}", "pg_scoped_config", Utc(time),
                    $"{scope} setting {row.Name}: {row.ChangeKind} (from {Value(row.OldValue)} to {Value(row.NewValue)})", row.DatabaseName));
            }
        }

        var extensionMark = Naive(marks["pg_extensions"]);
        var extensionBaseline = await NewestAtOrBeforeAsync(ExtensionBaselineSql, serverId, extensionMark, TimeSpan.FromDays(3), cancellationToken);
        await using (var command = _readSource.CreateCommand(ExtensionChangesSql))
        {
            command.CommandTimeout = _commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter { Value = serverId });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = extensionBaseline });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = naiveEnd });
            command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = extensionMark });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var time = reader.GetDateTime(0);
                var database = reader.IsDBNull(1) ? "" : reader.GetString(1);
                var extension = reader.GetString(2);
                var previous = reader.IsDBNull(3) ? null : reader.GetString(3);
                var current = reader.IsDBNull(4) ? null : reader.GetString(4);
                var what = previous is null ? $"installed (version {current})"
                    : current is null ? $"dropped (was version {previous})"
                    : $"version changed from {previous} to {current}";
                changes.Add(new ChangeWatchChange($"pg_extension:{database}:{extension}:{Stamp(time)}", "pg_extension", Utc(time),
                    $"extension {extension} in {database}: {what}", database));
            }
        }

        return changes;
    }

    private static DateTime Earlier(DateTime a, DateTime b) => a <= b ? a : b;

    private static string Value(string? value) =>
        value is null ? "(none)" : value.Length <= 120 ? value : value[..117] + "...";

    /// <summary>The newest snapshot time at or before <paramref name="mark"/>, looked for within <paramref name="reach"/>; the mark itself when there is none (the first snapshot after it then has nothing to differ from and reports nothing).</summary>
    private async Task<DateTime> NewestAtOrBeforeAsync(string sql, int serverId, DateTime mark, TimeSpan reach, CancellationToken cancellationToken)
    {
        await using var command = _readSource.CreateCommand(sql);
        command.CommandTimeout = _commandTimeoutSeconds;
        command.Parameters.Add(new NpgsqlParameter { Value = serverId });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mark });
        command.Parameters.Add(new NpgsqlParameter { NpgsqlDbType = NpgsqlDbType.Timestamp, Value = mark - reach });
        var value = await command.ExecuteScalarAsync(cancellationToken);
        return value is DateTime time ? DateTime.SpecifyKind(time, DateTimeKind.Unspecified) : mark;
    }
}
