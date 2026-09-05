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

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// Store reads backing the Excluded Databases picker. The viewer never connects to a monitored SQL
/// Server (so it can't enumerate its LIVE database list the way Lite's dialog does), but the Darling
/// store already HAS the databases the service collected — <c>v_database_config</c> (the sys.databases
/// snapshot) and <c>v_database_size_stats</c> (per-file sizes). The picker offers those collected user
/// databases as checkboxes instead of a free-text editor, keeping a manual-add fallback for a
/// not-yet-collected database. System databases (master/model/msdb/tempdb) are always excluded from the
/// list — they mirror Lite's <c>database_id &gt; 4</c> user-database filter and are never per-database
/// collector targets anyway.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>
    /// DISTINCT user-database names the store has collected for one server, from either the database
    /// config snapshot or the per-file size stats (UNION dedupes across both), system databases removed,
    /// ordered by name. $1 server_id.
    /// </summary>
    public const string CollectedDatabaseNamesSql = """
        SELECT database_name
        FROM (
            SELECT database_name FROM v_database_config WHERE server_id = $1
            UNION
            SELECT database_name FROM v_database_size_stats WHERE server_id = $1
        ) AS d
        WHERE database_name IS NOT NULL
        AND   database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
        ORDER BY database_name
        """;

    /// <summary>The store's server_id for a registry server name, matched on server_name or display_name. $1 name.</summary>
    public const string ServerIdByNameSql = """
        SELECT server_id
        FROM servers
        WHERE lower(server_name) = lower($1)
        OR    lower(display_name) = lower($1)
        ORDER BY server_id
        LIMIT 1
        """;

    /// <summary>
    /// The distinct user databases the store has collected for one server (empty when nothing has been
    /// collected yet). Feeds the Excluded Databases picker's checkbox list.
    /// </summary>
    public async Task<List<string>> GetCollectedDatabaseNamesAsync(int serverId, CancellationToken cancellationToken = default)
    {
        var names = new List<string>();

        await using var command = _dataSource.CreateCommand(CollectedDatabaseNamesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            names.Add(reader.GetString(0));
        }

        return names;
    }

    /// <summary>
    /// Resolves a viewer-registry server name to the store's server_id, or null when the service has not
    /// registered a server by that name yet (nothing collected → the picker falls back to manual-only).
    /// </summary>
    public async Task<int?> GetServerIdByNameAsync(string serverName, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(serverName))
        {
            return null;
        }

        await using var command = _dataSource.CreateCommand(ServerIdByNameSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        command.Parameters.Add(new NpgsqlParameter<string> { TypedValue = serverName });
        var result = await command.ExecuteScalarAsync(cancellationToken);
        return result is null or DBNull ? null : Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
    }
}
