/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>
    /// DISTINCT user-database names the store has collected for one server, from either the database
    /// config snapshot or the per-file size stats (UNION dedupes across both), system databases removed,
    /// ordered by name. Ported verbatim from the Darling viewer's collected-database query (issue #1319)
    /// so the global database filter's picker offers the same list on both apps. $1 = server_id.
    /// </summary>
    public const string CollectedDatabaseNamesSql = @"
SELECT database_name
FROM (
    SELECT database_name FROM v_database_config WHERE server_id = $1
    UNION
    SELECT database_name FROM v_database_size_stats WHERE server_id = $1
) AS d
WHERE database_name IS NOT NULL
AND   database_name NOT IN ('master', 'model', 'msdb', 'tempdb')
ORDER BY lower(database_name), database_name";

    /// <summary>
    /// The distinct user databases the store has collected for one server (empty when nothing has been
    /// collected yet). Feeds the global database-filter picker's checkbox list (issue #1319).
    /// </summary>
    public async Task<List<string>> GetCollectedDatabaseNamesAsync(int serverId)
    {
        var names = new List<string>();

        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = CollectedDatabaseNamesSql;
        command.Parameters.Add(new DuckDBParameter { Value = serverId });

        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                names.Add(reader.GetString(0));
            }
        }

        return names;
    }
}
