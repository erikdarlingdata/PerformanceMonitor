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
using PerformanceMonitor.Alerting;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>The three global Database Protection ages (#7611) as the alert settings hold them: full backup hours, log backup minutes, CHECKDB hours.</summary>
public sealed record DatabaseProtectionGlobalThresholds(int BackupHours, int LogBackupMinutes, int CheckDbHours)
{
    /// <summary>The built-in values: 168 hours (7 days), 60 minutes and 336 hours (14 days). They equal <see cref="DatabaseProtectionThresholds.Default"/>.</summary>
    public static DatabaseProtectionGlobalThresholds Default { get; } = new(168, 60, 336);
}

/// <summary>The bounds of the three ages and the reads that feed <see cref="DatabaseProtectionThresholdResolver"/> (#7611).</summary>
public static class DatabaseProtectionThresholdSettings
{
    public const string BackupHoursKey = DarlingThresholdKeys.BackupOverdueHours;
    public const string LogBackupMinutesKey = DarlingThresholdKeys.LogBackupOverdueMinutes;
    public const string CheckDbHoursKey = DarlingThresholdKeys.CheckDbOverdueHours;

    internal static readonly string[] Keys = [BackupHoursKey, LogBackupMinutesKey, CheckDbHoursKey];

    /// <summary>The value held inside its key's bounds, so a hand-edited file or a store written by another build cannot give the judgment a zero or negative age.</summary>
    public static int Clamp(int value, string wire)
    {
        DarlingThresholdKeys.TryGet(wire, out var entry);
        return Math.Clamp(value, entry.Min, entry.Max);
    }

    /// <summary>The global ages, each clamped into its bounds.</summary>
    public static DatabaseProtectionGlobalThresholds Normalize(int backupHours, int logBackupMinutes, int checkDbHours) =>
        new(Clamp(backupHours, BackupHoursKey), Clamp(logBackupMinutes, LogBackupMinutesKey), Clamp(checkDbHours, CheckDbHoursKey));

    /// <summary>The ages the three global columns hold. A store without the columns (a pre-V245 store) answers 42703, read as the built-in values.</summary>
    public const string GlobalSql =
        "SELECT backup_overdue_threshold_hours, log_backup_overdue_threshold_minutes, checkdb_overdue_threshold_hours " +
        "FROM config.config_alert_settings WHERE id = 1";

    /// <summary>The server's own database and server overrides of the three keys. A tag override is not here: the service's sweep resolves tags with the tag tree
    /// (<c>DarlingThresholdOverrides</c>), and the reads that call this are the tool, the fleet page and the Viewer.</summary>
    public const string OverridesSql =
        "SELECT database_name, threshold_key, value FROM config.config_threshold_overrides " +
        "WHERE server_id = $1 AND scope_tag_id IS NULL AND threshold_key = ANY($2)";

    /// <summary>Every server's database and server overrides of the three keys, for the fleet reads that judge many servers in one pass.</summary>
    public const string FleetOverridesSql =
        "SELECT server_id, database_name, threshold_key, value FROM config.config_threshold_overrides " +
        "WHERE server_id IS NOT NULL AND scope_tag_id IS NULL AND threshold_key = ANY($1)";

    /// <summary>One read of the global ages and every server's overrides; <paramref name="serverId"/> picks a server's resolver. Reads as <see cref="ReadAsync"/> does when a table cannot be read.</summary>
    public static async Task<Func<int, DatabaseProtectionThresholdResolver>> ReadFleetAsync(
        NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var global = await ReadGlobalAsync(postgres, commandTimeoutSeconds, cancellationToken);
        var byServer = new Dictionary<int, List<(string? Database, string Key, double Value)>>();
        try
        {
            await using var command = postgres.CreateCommand(FleetOverridesSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(Keys);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt32(0);
                if (!byServer.TryGetValue(id, out var list))
                    byServer[id] = list = [];
                list.Add((reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetString(2), reader.GetDouble(3)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedTable)
        {
        }

        return serverId => DatabaseProtectionThresholdResolver.FromRows(global, byServer.TryGetValue(serverId, out var rows) ? rows : []);
    }

    private static async Task<DatabaseProtectionGlobalThresholds> ReadGlobalAsync(NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        try
        {
            await using var command = postgres.CreateCommand(GlobalSql);
            command.CommandTimeout = commandTimeoutSeconds;
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (await reader.ReadAsync(cancellationToken))
                return Normalize(reader.GetInt32(0), reader.GetInt32(1), reader.GetInt32(2));
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedColumn or PostgresErrorCodes.UndefinedTable)
        {
        }

        return DatabaseProtectionGlobalThresholds.Default;
    }

    /// <summary>
    /// Reads the global ages and one server's overrides, and returns the resolver for that server. A role or store that cannot read a table
    /// (42501 no privilege, 42P01 no table, 42703 no column) reads as the built-in values or no override, never as an error: the judgment stays the
    /// shipped one rather than the page failing.
    /// </summary>
    public static async Task<DatabaseProtectionThresholdResolver> ReadAsync(
        NpgsqlDataSource postgres, int serverId, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var global = await ReadGlobalAsync(postgres, commandTimeoutSeconds, cancellationToken);
        var rows = new List<(string? Database, string Key, double Value)>();
        try
        {
            await using var command = postgres.CreateCommand(OverridesSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(Keys);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
                rows.Add((reader.IsDBNull(0) ? null : reader.GetString(0), reader.GetString(1), reader.GetDouble(2)));
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedTable)
        {
        }

        return DatabaseProtectionThresholdResolver.FromRows(global, rows);
    }
}

/// <summary>
/// One server's effective Database Protection ages (#7611): for a database, its override, else the server's, else the global value, else the built-in one.
/// The one resolution the sweep's alerts, <c>get_database_protection</c>, the fleet page and the Viewer share, so none of them keeps its own copy. It only
/// supplies the ages: <see cref="DatabaseProtectionBands"/> stays the one judgment.
/// </summary>
public sealed class DatabaseProtectionThresholdResolver
{
    private readonly Func<string, string?, int, int> _resolve;

    /// <summary>The ages the service resolved elsewhere: <paramref name="resolve"/> is (wire name, database or null, global value) to the value in force, as <c>DarlingThresholdOverrides.ResolveWire</c> answers for one server.</summary>
    public DatabaseProtectionThresholdResolver(DatabaseProtectionGlobalThresholds global, Func<string, string?, int, int> resolve)
    {
        Global = global ?? throw new ArgumentNullException(nameof(global));
        _resolve = resolve ?? throw new ArgumentNullException(nameof(resolve));
    }

    /// <summary>The global ages, before any override.</summary>
    public DatabaseProtectionGlobalThresholds Global { get; }

    /// <summary>A resolver over the rows of <c>config.config_threshold_overrides</c> for one server. A row whose key is not one of the three, whose value is not
    /// an integer inside the key's bounds, is skipped, as <c>DarlingThresholdOverrides</c> skips it.</summary>
    public static DatabaseProtectionThresholdResolver FromRows(
        DatabaseProtectionGlobalThresholds global, IEnumerable<(string? Database, string Key, double Value)> rows)
    {
        var map = new Dictionary<(string Database, string Key), int>();
        foreach (var (database, key, raw) in rows)
        {
            if (!DarlingThresholdKeys.TryGet(key, out var entry) || raw != Math.Floor(raw) || raw < entry.Min || raw > entry.Max)
                continue;
            map[(database ?? string.Empty, entry.Wire)] = (int)raw;
        }

        return new DatabaseProtectionThresholdResolver(global, (wire, database, globalValue) =>
        {
            if (!string.IsNullOrEmpty(database) && map.TryGetValue((database, wire), out var perDatabase))
                return perDatabase;
            return map.TryGetValue((string.Empty, wire), out var perServer) ? perServer : globalValue;
        });
    }

    /// <summary>The ages that apply to <paramref name="database"/>, or to the server as a whole when it is null.</summary>
    public DatabaseProtectionThresholds For(string? database) => new(
        TimeSpan.FromHours(DatabaseProtectionThresholdSettings.Clamp(_resolve(DatabaseProtectionThresholdSettings.BackupHoursKey, database, Global.BackupHours), DatabaseProtectionThresholdSettings.BackupHoursKey)),
        TimeSpan.FromMinutes(DatabaseProtectionThresholdSettings.Clamp(_resolve(DatabaseProtectionThresholdSettings.LogBackupMinutesKey, database, Global.LogBackupMinutes), DatabaseProtectionThresholdSettings.LogBackupMinutesKey)),
        TimeSpan.FromHours(DatabaseProtectionThresholdSettings.Clamp(_resolve(DatabaseProtectionThresholdSettings.CheckDbHoursKey, database, Global.CheckDbHours), DatabaseProtectionThresholdSettings.CheckDbHoursKey)));

    /// <summary>The ages for a database name, in the shape <see cref="DatabaseProtectionBands.OverdueFacts(IReadOnlyList{DatabaseProtectionFact}, DateTime, Func{string, DatabaseProtectionThresholds})"/> takes.</summary>
    public Func<string, DatabaseProtectionThresholds> ForDatabase => name => For(name);

    /// <summary>"7 days", "1 day", "36 hours" or "60 minutes": an age limit in the words the alert, the tool and the Viewer share. A log backup limit is always in minutes.</summary>
    public static string LimitText(string kind, TimeSpan limit)
    {
        string Unit(long n, string unit) => string.Create(CultureInfo.InvariantCulture, $"{n} {unit}{(n == 1 ? "" : "s")}");
        if (kind == DatabaseProtectionBands.KindLogBackup)
            return Unit((long)limit.TotalMinutes, "minute");
        return limit.TotalHours % 24 == 0 ? Unit((long)limit.TotalDays, "day") : Unit((long)limit.TotalHours, "hour");
    }
}
