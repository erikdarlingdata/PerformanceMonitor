/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
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
    /// #9065: the names on one server's newest <c>database_states</c> list that equal <paramref name="databaseName"/> ignoring case, for
    /// the MCP <c>database_name</c> resolution (<c>McpDatabaseNames</c>). Null when the server has no list in
    /// <see cref="FindDatabaseLookBack"/>, empty when its list holds no such name. Darling's twin is
    /// <c>DarlingMcpDatabaseNames.CandidatesSql</c>.
    /// </summary>
    internal async Task<IReadOnlyList<string>?> DatabaseNameCandidatesAsync(int serverId, string databaseName, DateTime? nowUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT d.database_name
FROM (SELECT MAX(collection_time) AS t
      FROM v_database_states
      WHERE server_id = $1
      AND   collection_time > $2) n
LEFT JOIN v_database_states d
       ON d.server_id = $1
      AND d.collection_time = n.t
      AND lower(d.database_name) = lower($3)
WHERE n.t IS NOT NULL";
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = (nowUtc ?? DateTime.UtcNow) - FindDatabaseLookBack });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = databaseName });

        List<string>? names = null;
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            names ??= new List<string>();
            if (!reader.IsDBNull(0))
            {
                names.Add(reader.GetString(0));
            }
        }

        return names;
    }
}
