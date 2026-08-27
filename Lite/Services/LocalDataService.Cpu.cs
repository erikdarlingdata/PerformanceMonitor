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

public partial class LocalDataService
{
    /// <summary>
    /// Gets CPU utilization data for charting.
    /// Note: sample_time is stored in server local time (from SYSDATETIME()), not UTC.
    /// </summary>
    public async Task<List<CpuUtilizationRow>> GetCpuUtilizationAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* sample_time is in server local time, not UTC */
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, fromDate, toDate, asOfUtc);

        command.CommandText = @"
SELECT
    sample_time,
    sqlserver_cpu_utilization,
    other_process_cpu_utilization
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   sample_time >= $2
AND   sample_time <= $3
ORDER BY sample_time";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        var items = new List<CpuUtilizationRow>();
        using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            items.Add(new CpuUtilizationRow
            {
                SampleTime = reader.GetDateTime(0),
                SqlServerCpu = reader.IsDBNull(1) ? 0 : reader.GetInt32(1),
                OtherProcessCpu = reader.IsDBNull(2) ? 0 : reader.GetInt32(2)
            });
        }

        return items;
    }

    /// <summary>
    /// The attributed-CPU denominator's pieces (#2320): sample count, coverage bounds, and average SQL
    /// CPU% over the window. Windowed on collection_time (UTC) — the SAME bounds the top-queries and
    /// top-procedures rankings use — so numerator and denominator share collection gaps; sample_time's
    /// server-local skew is irrelevant to an average. Takes the window EXPLICITLY (not hours_back) so
    /// the caller can hand the identical bounds to CpuAttribution.Compute — review catch: three
    /// independently-sampled UtcNow calls backing one disclosure is drift by construction.
    /// </summary>
    public async Task<CpuWindowAggregateRow> GetCpuWindowAggregateAsync(int serverId, DateTime startUtc, DateTime endUtc)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        command.CommandText = @"
SELECT
    COUNT(*),
    MIN(collection_time),
    MAX(collection_time),
    AVG(CAST(sqlserver_cpu_utilization AS DOUBLE))
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   collection_time >= $2
AND   collection_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startUtc });
        command.Parameters.Add(new DuckDBParameter { Value = endUtc });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return new CpuWindowAggregateRow(0, null, null, null);
        }

        return new CpuWindowAggregateRow(
            reader.IsDBNull(0) ? 0 : Convert.ToInt32(reader.GetValue(0)),
            reader.IsDBNull(1) ? null : reader.GetDateTime(1),
            reader.IsDBNull(2) ? null : reader.GetDateTime(2),
            reader.IsDBNull(3) ? null : reader.GetDouble(3));
    }
}

public sealed record CpuWindowAggregateRow(int SampleCount, DateTime? FirstSample, DateTime? LastSample, double? AvgSqlCpuPercent);

public class CpuUtilizationRow
{
    public DateTime SampleTime { get; set; }
    public int SqlServerCpu { get; set; }
    public int OtherProcessCpu { get; set; }
    public int TotalCpu => SqlServerCpu + OtherProcessCpu;
    public int IdleCpu => Math.Max(0, 100 - TotalCpu);
}
