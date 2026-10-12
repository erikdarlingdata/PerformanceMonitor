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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>How old the newest <c>database_states</c> snapshot may be and still decide which databases are gone: six
    /// hours, the twin of Darling's <c>CurrentDatabaseScope.SnapshotHorizon</c>.</summary>
    internal static readonly TimeSpan CurrentDatabasesHorizon = TimeSpan.FromHours(6);

    /// <summary>
    /// #5633 item 4: the databases the server's newest <c>database_states</c> snapshot names, on the same SQL text as the
    /// Query Store backfill's gone-database check (<see cref="QueryStoreBackfillState.NewestSnapshotSql"/>), read from
    /// <c>v_database_states</c> (hot plus archive). Null when that is unknown (no snapshot, as on an Azure SQL Database or a
    /// store that never collected it; one older than <see cref="CurrentDatabasesHorizon"/>; no database names; a failed
    /// read), and every caller then filters nothing.
    /// </summary>
    public async Task<IReadOnlyList<string>?> GetCurrentDatabasesAsync(int serverId, DateTime? nowUtc = null)
    {
        try
        {
            using var connection = await OpenConnectionAsync();
            using var command = connection.CreateCommand();
            command.CommandText = QueryStoreBackfillState.NewestSnapshotSql.Replace("FROM database_states", "FROM v_database_states", StringComparison.Ordinal);
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = (nowUtc ?? DateTime.UtcNow) - CurrentDatabasesHorizon });
            var names = new List<string>();
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                if (!reader.IsDBNull(0))
                {
                    names.Add(reader.GetString(0));
                }
            }

            return names.Count == 0 ? null : names;
        }
        catch (Exception)
        {
            return null;
        }
    }
}
