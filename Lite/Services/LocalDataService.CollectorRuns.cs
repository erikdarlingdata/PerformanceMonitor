/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using DuckDB.NET.Data;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// What the store says about which collectors have ever run for one server. A tab reads it once per refresh and its
/// empty-state notes ask it <see cref="NeverRan"/>.
/// </summary>
/// <param name="LoggedCollectors">Collectors with at least one collection_log row for the server, in all retained history.</param>
/// <param name="CollectorsWithData">In-scope collectors whose data table holds at least one row for the server.</param>
/// <param name="ServerHasAnyLogRow">Whether the server has a collection_log row from any collector.</param>
public sealed record CollectorRunHistory(
    IReadOnlySet<string> LoggedCollectors,
    IReadOnlySet<string> CollectorsWithData,
    bool ServerHasAnyLogRow)
{
    /// <summary>No claim: nothing is known about this server, so nothing is said to have never run.</summary>
    public static CollectorRunHistory Empty { get; } = new(
        new HashSet<string>(StringComparer.Ordinal),
        new HashSet<string>(StringComparer.Ordinal),
        false);

    /// <summary>
    /// True when the server has collected something, and this collector has no log row and no data row in all
    /// retained history. There is no time window. The server_config and trace_flags collectors run once at load, so a
    /// window would call them never-run once that load-time row ages out.
    /// </summary>
    public bool NeverRan(string collectorName) =>
        ServerHasAnyLogRow && !LoggedCollectors.Contains(collectorName) && !CollectorsWithData.Contains(collectorName);
}

public partial class LocalDataService
{
    /// <summary>
    /// Reads, for one server, the collectors that have a collection_log row in all retained history, whether the
    /// server has any such row, and which of the collectors whose surfaces say "not collected" have a data row. The
    /// data tables are the ones those surfaces read: server_config, trace_flags, memory_pressure_events and
    /// database_states. Any data row proves its collector ran, even where the log row has aged out.
    /// </summary>
    public async Task<CollectorRunHistory> GetCollectorRunHistoryAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();

        var logged = new HashSet<string>(StringComparer.Ordinal);
        var anyLogRow = false;

        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
SELECT collector_name
FROM collection_log
WHERE server_id = $1
GROUP BY collector_name";

            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                anyLogRow = true;

                if (!reader.IsDBNull(0))
                {
                    logged.Add(reader.GetString(0));
                }
            }
        }

        var withData = new HashSet<string>(StringComparer.Ordinal);

        using (var command = connection.CreateCommand())
        {
            command.CommandText = @"
SELECT 'server_config' AS collector_name
WHERE EXISTS (SELECT 1 FROM v_server_config WHERE server_id = $1)
UNION ALL
SELECT 'trace_flags'
WHERE EXISTS (SELECT 1 FROM v_trace_flags WHERE server_id = $1)
UNION ALL
SELECT 'memory_pressure_events'
WHERE EXISTS (SELECT 1 FROM v_memory_pressure_events WHERE server_id = $1)
UNION ALL
SELECT 'database_states'
WHERE EXISTS (SELECT 1 FROM database_states WHERE server_id = $1)";

            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                withData.Add(reader.GetString(0));
            }
        }

        return new CollectorRunHistory(logged, withData, anyLogRow);
    }
}
