/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading.Tasks;
using DuckDB.NET.Data;
using PerformanceMonitor.Common;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>The newest <see cref="CollectorRunFacts.RunsCap"/> successful runs of one collector, newest first. $1 server_id, $2 collector_name, $3 the cap (bound, never a literal).</summary>
    public const string CollectorLatestSuccessSql = @"
SELECT collection_time, rows_collected
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = 'SUCCESS'
ORDER BY collection_time DESC
LIMIT $3";

    /// <summary>Successful runs of one collector inside a window. $1 server_id, $2 collector_name, $3/$4 window.</summary>
    public const string CollectorRunsInWindowSql = @"
SELECT COUNT(*)
FROM v_collection_log
WHERE server_id = $1
AND   collector_name = $2
AND   status = 'SUCCESS'
AND   collection_time >= $3
AND   collection_time <= $4";

    /// <summary>
    /// #5633 items 3 and 19: what <c>collection_log</c> says about one collector (the Darling twin is
    /// <c>DarlingCollectorRunFactsReader</c>; the words are shared in <see cref="CollectorRunFacts"/>). Pass
    /// <paramref name="hoursBack"/> (with <paramref name="asOfUtc"/>) to also count the runs inside the window; leave it
    /// null for a read with no window question. Only SUCCESS rows count. Asked only on an empty path, and a failed read
    /// propagates to the tool's own catch rather than becoming a quiet "no runs".
    /// </summary>
    public async Task<CollectorRunFacts> GetCollectorRunFactsAsync(int serverId, string collectorName, int? hoursBack = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();

        long runs = 0;
        DateTime? last = null;
        long? lastRows = null;

        using (var command = connection.CreateCommand())
        {
            command.CommandText = CollectorLatestSuccessSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = CollectorRunFacts.RunsCap });
            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                runs++;
                if (runs == 1)
                {
                    last = reader.IsDBNull(0) ? null : DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc);
                    lastRows = reader.IsDBNull(1) ? null : Convert.ToInt64(reader.GetValue(1));
                }
            }
        }

        long? inWindow = null;
        if (hoursBack is { } hours)
        {
            var (startTime, endTime) = GetTimeRange(hours, null, null, asOfUtc);
            using var command = connection.CreateCommand();
            command.CommandText = CollectorRunsInWindowSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = collectorName });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });
            inWindow = Convert.ToInt64(await command.ExecuteScalarAsync() ?? 0L);
        }

        return new CollectorRunFacts(runs, inWindow, last, lastRows);
    }
}
