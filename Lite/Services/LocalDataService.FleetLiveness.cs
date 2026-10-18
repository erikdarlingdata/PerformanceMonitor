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
    /// <summary>#6074: one collector's newest success and newest failure per server in a window, from <c>v_collection_log</c> (hot plus archive).
    /// The statement is the one Darling runs (<see cref="CollectorFleetLiveness.LivenessSql"/>).</summary>
    internal async Task<List<CollectorFleetLiveness.RunFacts>> GetCollectorFleetLivenessAsync(string collectorName, DateTime windowStartUtc, DateTime windowEndUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = CollectorFleetLiveness.LivenessSql("v_collection_log");
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = collectorName });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = windowStartUtc });
        command.Parameters.Add(new DuckDB.NET.Data.DuckDBParameter { Value = windowEndUtc });

        var rows = new List<CollectorFleetLiveness.RunFacts>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new CollectorFleetLiveness.RunFacts(
                (int)ToInt64(reader.GetValue(0)),
                reader.IsDBNull(1) ? null : reader.GetDateTime(1),
                reader.IsDBNull(2) ? null : reader.GetDateTime(2),
                ToInt64(reader.GetValue(3)),
                ToInt64(reader.GetValue(4)),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.IsDBNull(6) ? null : reader.GetString(6)));
        }

        return rows;
    }
}
