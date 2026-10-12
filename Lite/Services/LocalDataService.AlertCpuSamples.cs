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
using PerformanceMonitor.Alerting;

namespace PerformanceMonitorLite.Services;

public partial class LocalDataService
{
    /// <summary>How far back the alert engine's CPU sample list looks (#5642): the PostgreSQL check's window.</summary>
    public static readonly TimeSpan AlertCpuSampleLookback = TimeSpan.FromMinutes(15);

    /// <summary>The most CPU samples the alert engine's list carries (#5642): the PostgreSQL check's bound.</summary>
    public const int AlertCpuSampleLimit = 32;

    /// <summary>
    /// The recent CPU samples the shared alert engine's persistence gate judges one by one (#5642): the last
    /// <see cref="AlertCpuSampleLookback"/> before <paramref name="nowUtc"/>, at most <paramref name="limit"/>
    /// of them, returned OLDEST FIRST in UTC. Only rows with a true <c>sample_time_utc</c> are listed (a row
    /// from before v63 has only the monitored server's local stamp, which is not an instant), so a store with
    /// none returns an empty list and the engine falls back to the single latest sample.
    ///
    /// <para>The newest <paramref name="limit"/> rows are kept, then reversed: bounding the OLDEST end would drop
    /// the latest readings, the ones that matter. Hot table first, then the archive view, as every latest-row
    /// read here does (#3895). <c>total</c> is SQL CPU plus other-process CPU, the same sum
    /// <see cref="ServerSummaryItem.TotalCpuPercent"/> reports; a row without SQL CPU has neither value.</para>
    /// </summary>
    public async Task<List<AlertCpuSample>> GetRecentCpuSamplesAsync(
        int serverId, DateTime nowUtc, int limit = AlertCpuSampleLimit)
    {
        using var connection = await OpenConnectionAsync();
        var samples = new List<AlertCpuSample>();

        foreach (var relation in NewestFirstRelations("cpu_utilization_stats"))
        {
            using var cmd = connection.CreateCommand();
            cmd.CommandText = @"
SELECT sample_time_utc, sqlserver_cpu_utilization, other_process_cpu_utilization
FROM " + relation + @"
WHERE server_id = $1
AND   sample_time_utc IS NOT NULL
AND   sample_time_utc >= $2
ORDER BY sample_time_utc DESC
LIMIT $3";
            cmd.Parameters.Add(new DuckDBParameter { Value = serverId });
            cmd.Parameters.Add(new DuckDBParameter { Value = nowUtc - AlertCpuSampleLookback });
            cmd.Parameters.Add(new DuckDBParameter { Value = limit });
            using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                double? sql = reader.IsDBNull(1) ? null : ToDouble(reader.GetValue(1));
                double? other = reader.IsDBNull(2) ? null : ToDouble(reader.GetValue(2));
                samples.Add(new AlertCpuSample(
                    DateTime.SpecifyKind(reader.GetDateTime(0), DateTimeKind.Utc),
                    sql,
                    sql.HasValue ? sql.Value + (other ?? 0) : null));
            }

            if (samples.Count > 0)
            {
                break;
            }
        }

        samples.Reverse();
        return samples;
    }
}
