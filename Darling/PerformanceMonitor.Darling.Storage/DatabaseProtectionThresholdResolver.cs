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
using System.Linq;
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

    /// <summary>The server's own database and server overrides of the three keys, and every tag override (#6926). A tag override has no server id: it reaches a
    /// server through the tag tree, which <see cref="TagThresholdResolver"/> walks, the same resolution the service's alerts use.</summary>
    public const string OverridesSql =
        "SELECT id, server_id, database_name, scope_tag_id, threshold_key, value FROM config.config_threshold_overrides " +
        "WHERE threshold_key = ANY($2) AND (scope_tag_id IS NOT NULL OR server_id = $1)";

    /// <summary>Every server's database and server overrides of the three keys, and every tag override, for the fleet reads that judge many servers in one pass.</summary>
    public const string FleetOverridesSql =
        "SELECT id, server_id, database_name, scope_tag_id, threshold_key, value FROM config.config_threshold_overrides " +
        "WHERE threshold_key = ANY($1) AND (scope_tag_id IS NOT NULL OR server_id IS NOT NULL)";

    /// <summary>One read of the global ages and every server's overrides; <paramref name="serverId"/> picks a server's resolver. Reads as <see cref="ReadAsync"/> does when a table cannot be read.</summary>
    public static async Task<Func<int, DatabaseProtectionThresholdResolver>> ReadFleetAsync(
        NpgsqlDataSource postgres, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(postgres);
        var global = await ReadGlobalAsync(postgres, commandTimeoutSeconds, cancellationToken);
        var byServer = new Dictionary<int, List<(string? Database, string Key, double Value)>>();
        var tagRows = new List<TagOverrideCandidate>();
        try
        {
            await using var command = postgres.CreateCommand(FleetOverridesSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(Keys);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(3))
                {
                    tagRows.Add(new TagOverrideCandidate(reader.GetInt64(0), reader.GetInt32(3), reader.GetString(4), reader.GetDouble(5)));
                    continue;
                }

                var id = reader.GetInt32(1);
                if (!byServer.TryGetValue(id, out var list))
                    byServer[id] = list = [];
                list.Add((reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(4), reader.GetDouble(5)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedTable)
        {
        }

        var tagWinners = await ReadTagWinnersAsync(postgres, tagRows, commandTimeoutSeconds, cancellationToken);
        return serverId => DatabaseProtectionThresholdResolver.FromRows(
            global, byServer.TryGetValue(serverId, out var rows) ? rows : [], TagWinnersFor(tagWinners, serverId));
    }

    private static Dictionary<string, int> TagWinnersFor(IReadOnlyDictionary<(int ServerId, string Key), TagOverrideWinner> winners, int serverId)
    {
        var forServer = new Dictionary<string, int>();
        foreach (var key in Keys)
        {
            if (winners.TryGetValue((serverId, key), out var winner))
                forServer[key] = (int)winner.Value;
        }

        return forServer;
    }

    /// <summary>
    /// The tag override that wins per (server, key) (#6926), by the one resolution the service's alerts use: <see cref="TagThresholdResolver"/>, the deepest tag
    /// the server is under. Reads the registry, the tag tree and the server-to-tag map only when a tag override exists. A role that cannot read those tables
    /// reads as no tag override, never as an error, as the other reads here do. A row outside its key's bounds or not an integer is skipped, as the service skips it.
    /// </summary>
    private static async Task<IReadOnlyDictionary<(int ServerId, string Key), TagOverrideWinner>> ReadTagWinnersAsync(
        NpgsqlDataSource postgres, List<TagOverrideCandidate> tagRows, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var accepted = tagRows.FindAll(r => DarlingThresholdKeys.TryGet(r.Key, out var entry) && r.Value == Math.Floor(r.Value) && r.Value >= entry.Min && r.Value <= entry.Max);
        if (accepted.Count == 0)
            return new Dictionary<(int ServerId, string Key), TagOverrideWinner>();

        var known = new HashSet<int>();
        var parents = new Dictionary<int, int?>();
        var serverTags = new Dictionary<int, HashSet<int>>();
        try
        {
            await using (var command = postgres.CreateCommand(TagThresholdResolver.ServersSql))
            {
                command.CommandTimeout = commandTimeoutSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    known.Add(reader.GetInt32(0));
            }

            await using (var command = postgres.CreateCommand(TagThresholdResolver.TagsSql))
            {
                command.CommandTimeout = commandTimeoutSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                    parents[reader.GetInt32(0)] = reader.IsDBNull(1) ? null : reader.GetInt32(1);
            }

            await using (var command = postgres.CreateCommand(TagThresholdResolver.TagMapSql))
            {
                command.CommandTimeout = commandTimeoutSeconds;
                await using var reader = await command.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    var serverId = reader.GetInt32(0);
                    if (!serverTags.TryGetValue(serverId, out var tags))
                        serverTags[serverId] = tags = [];
                    tags.Add(reader.GetInt32(1));
                }
            }
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedTable)
        {
            return new Dictionary<(int ServerId, string Key), TagOverrideWinner>();
        }

        return TagThresholdResolver.Resolve(
            accepted, known, parents, serverTags.ToDictionary(p => p.Key, p => (IReadOnlyCollection<int>)p.Value), true);
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
        var tagRows = new List<TagOverrideCandidate>();
        try
        {
            await using var command = postgres.CreateCommand(OverridesSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            command.Parameters.AddWithValue(Keys);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!reader.IsDBNull(3))
                    tagRows.Add(new TagOverrideCandidate(reader.GetInt64(0), reader.GetInt32(3), reader.GetString(4), reader.GetDouble(5)));
                else
                    rows.Add((reader.IsDBNull(2) ? null : reader.GetString(2), reader.GetString(4), reader.GetDouble(5)));
            }
        }
        catch (PostgresException ex) when (ex.SqlState is PostgresErrorCodes.InsufficientPrivilege or PostgresErrorCodes.UndefinedTable)
        {
        }

        var tagWinners = await ReadTagWinnersAsync(postgres, tagRows, commandTimeoutSeconds, cancellationToken);
        return DatabaseProtectionThresholdResolver.FromRows(global, rows, TagWinnersFor(tagWinners, serverId));
    }
}

/// <summary>
/// One server's effective Database Protection ages (#7611): for a database, its override, else the server's, else the tag's (#6926, the deepest tag the server is under), else the global value, else the built-in one.
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

    /// <summary>A resolver over the rows of <c>config.config_threshold_overrides</c> for one server, plus the tag override that won per wire name
    /// (<paramref name="tagOverrides"/>, from <see cref="TagThresholdResolver"/>). A row whose key is not one of the three, whose value is not
    /// an integer inside the key's bounds, is skipped, as <c>DarlingThresholdOverrides</c> skips it.</summary>
    public static DatabaseProtectionThresholdResolver FromRows(
        DatabaseProtectionGlobalThresholds global, IEnumerable<(string? Database, string Key, double Value)> rows,
        IReadOnlyDictionary<string, int>? tagOverrides = null)
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
            if (map.TryGetValue((string.Empty, wire), out var perServer))
                return perServer;
            return tagOverrides is not null && tagOverrides.TryGetValue(wire, out var perTag) ? perTag : globalValue;
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
