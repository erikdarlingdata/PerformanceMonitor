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

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>How far back a server's newest database list is looked for by <see cref="FindDatabaseAsync"/>: seven days, the twin of
    /// Darling's <c>DarlingMcpFindDatabaseTools.LookBack</c>.</summary>
    internal static readonly TimeSpan FindDatabaseLookBack = TimeSpan.FromDays(7);

    /// <summary>One server's newest database list: its server id, the list's time, and the stored spelling of the database when the list names it.</summary>
    internal sealed record DatabaseListRow(int ServerId, DateTime NewestSnapshotUtc, string? MatchedName);

    /// <summary>
    /// #6074: for every server with a <c>database_states</c> list in the look-back, the newest list's time and the stored spelling of
    /// <paramref name="databaseName"/> when that list names it (exact, case-insensitive). Written by the <c>database_states</c>
    /// collector (<c>RemoteCollectorService.CollectDatabaseStatesAsync</c>), read from <c>v_database_states</c> (hot plus archive).
    /// </summary>
    internal async Task<List<DatabaseListRow>> FindDatabaseAsync(string databaseName, DateTime? nowUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT n.server_id,
       n.t,
       (SELECT MIN(d.database_name)
        FROM v_database_states d
        WHERE d.server_id = n.server_id
        AND   d.collection_time = n.t
        AND   lower(d.database_name) = lower($1)) AS matched_name
FROM (SELECT server_id, MAX(collection_time) AS t
      FROM v_database_states
      WHERE collection_time > $2
      GROUP BY server_id) n
ORDER BY n.server_id";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = (nowUtc ?? DateTime.UtcNow) - FindDatabaseLookBack });

        var rows = new List<DatabaseListRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new DatabaseListRow(
                (int)ToInt64(reader.GetValue(0)),
                DateTime.SpecifyKind(reader.GetDateTime(1), DateTimeKind.Utc),
                reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return rows;
    }

    /// <summary>
    /// #9065: the stored spellings of <paramref name="databaseName"/> on one server, for the MCP <c>database_name</c> resolution
    /// (<c>McpDatabaseNames</c>): the names on the server's newest <c>database_states</c> list that equal it ignoring case, plus the
    /// given spelling itself when that list holds it only in another case and an earlier list in <see cref="FindDatabaseLookBack"/>
    /// holds it exactly (a database renamed or recreated in another letter case). Null when the server has no list in the
    /// look-back, empty when its newest list holds no such name. Darling's twin is <c>DarlingMcpDatabaseNames</c>.
    ///
    /// <para>The hot <c>database_states</c> table is read first, as <c>PickDatabaseStatesSourceAsync</c> reads it: it holds the
    /// newest rows, so a list found there is the newest one. <c>v_database_states</c> (hot plus the Parquet archive) is read for
    /// the newest list only when the hot table has none for the server, which is the state just after an archive-and-reset. For
    /// the earlier spelling it is read only when the hot table neither holds that spelling nor reaches back to the start of the
    /// look-back: rows leave the hot table oldest first, so a hot list at or before that start means no later list of the server
    /// is in the archive alone.</para>
    /// </summary>
    internal async Task<PerformanceMonitor.Common.McpDatabaseNameLookup?> DatabaseNameCandidatesAsync(
        int serverId, string databaseName, DateTime? nowUtc = null, CancellationToken cancellationToken = default)
    {
        var floor = (nowUtc ?? DateTime.UtcNow) - FindDatabaseLookBack;
        using var connection = await OpenConnectionAsync(timer: null, cancellationToken);

        if ((await DatabaseNameMatchesAsync(connection, HotDatabaseStates, serverId, floor, databaseName, cancellationToken)
            ?? await DatabaseNameMatchesAsync(connection, ArchivedDatabaseStates, serverId, floor, databaseName, cancellationToken)) is not { } list)
        {
            return null;
        }

        var (names, taken) = list;
        if (names.Count > 0 && !names.Contains(databaseName))
        {
            var (seen, hotReachesTheFloor) = await DatabaseNameSeenInHotAsync(connection, serverId, floor, databaseName, cancellationToken);
            if (seen || (!hotReachesTheFloor && await DatabaseNameSeenInArchiveAsync(connection, serverId, floor, databaseName, cancellationToken)))
            {
                names.Add(databaseName);
            }
        }

        return new PerformanceMonitor.Common.McpDatabaseNameLookup(names, taken);
    }

    /// <summary>The names on the newest list in <paramref name="source"/> that equal the name ignoring case, and when that list was taken; null when it has no list.</summary>
    private static async Task<(List<string> Names, DateTime Taken)?> DatabaseNameMatchesAsync(
        Database.LockedConnection connection, string source, int serverId, DateTime floor, string databaseName,
        CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT d.database_name, n.t
FROM (SELECT MAX(collection_time) AS t
      FROM {source}
      WHERE server_id = $1
      AND   collection_time > $2) n
LEFT JOIN {source} d
       ON d.server_id = $1
      AND d.collection_time = n.t
      AND lower(d.database_name) = lower($3)
WHERE n.t IS NOT NULL";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = floor });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });

        List<string>? names = null;
        var taken = default(DateTime);
        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names ??= new List<string>();
            taken = reader.GetDateTime(1);
            if (!reader.IsDBNull(0))
            {
                names.Add(reader.GetString(0));
            }
        }

        return names is null ? null : (names, taken);
    }

    /// <summary>
    /// Whether any hot list newer than <paramref name="floor"/> holds the name exactly, and whether the hot table holds a list of
    /// the server at or before <paramref name="floor"/>, which makes the hot table the whole look-back.
    /// </summary>
    private static async Task<(bool Seen, bool ReachesTheFloor)> DatabaseNameSeenInHotAsync(
        Database.LockedConnection connection, int serverId, DateTime floor, string databaseName, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT EXISTS (
           SELECT 1
           FROM {HotDatabaseStates}
           WHERE server_id = $1
           AND   collection_time > $2
           AND   database_name = $3),
       EXISTS (
           SELECT 1
           FROM {HotDatabaseStates}
           WHERE server_id = $1
           AND   collection_time <= $2)";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = floor });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });

        using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? (reader.GetBoolean(0), reader.GetBoolean(1)) : (false, false);
    }

    /// <summary>Whether any list newer than <paramref name="floor"/>, hot or archived, holds the name exactly.</summary>
    private static async Task<bool> DatabaseNameSeenInArchiveAsync(
        Database.LockedConnection connection, int serverId, DateTime floor, string databaseName, CancellationToken cancellationToken)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $@"
SELECT EXISTS (
    SELECT 1
    FROM {ArchivedDatabaseStates}
    WHERE server_id = $1
    AND   collection_time > $2
    AND   database_name = $3)";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = floor });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });
        return await command.ExecuteScalarAsync(cancellationToken) is true;
    }
}
