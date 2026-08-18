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
    public async Task<List<CpuUtilizationRow>> GetCpuUtilizationAsync(int serverId, int hoursBack = 4, DateTime? fromDate = null, DateTime? toDate = null)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();

        /* sample_time is in server local time, not UTC */
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, fromDate, toDate);

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
    /// #2320: the attribution denominator's raw ingredients for one window — average SQL-process CPU
    /// percent and the sample count the coverage gate needs. Mirrors Darling's
    /// GetCpuWindowAverageAsync; windows on server-local sample_time exactly like
    /// <see cref="GetCpuUtilizationAsync"/> above, because that is this store's clock for this table.
    /// </summary>
    public async Task<(double? AvgSqlCpuPercent, int Samples, double SpanHours)> GetCpuWindowAverageAsync(int serverId, int hoursBack)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        var (startTime, endTime) = GetTimeRangeServerLocal(hoursBack, null, null);

        /* Span rides along for the cadence-agnostic coverage gate — mirrors Darling. */
        command.CommandText = @"
SELECT
    AVG(sqlserver_cpu_utilization),
    COUNT(*),
    COALESCE(EXTRACT(EPOCH FROM (MAX(sample_time) - MIN(sample_time))) / 3600.0, 0)
FROM v_cpu_utilization_stats
WHERE server_id = $1
AND   sample_time >= $2
AND   sample_time <= $3";

        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        command.Parameters.Add(new DuckDBParameter { Value = startTime });
        command.Parameters.Add(new DuckDBParameter { Value = endTime });

        using var reader = await command.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
        {
            return (null, 0, 0);
        }

        var avg = reader.IsDBNull(0) ? (double?)null : reader.GetDouble(0);
        var samples = reader.IsDBNull(1) ? 0 : Convert.ToInt32(reader.GetValue(1), System.Globalization.CultureInfo.InvariantCulture);
        var spanHours = reader.IsDBNull(2) ? 0 : Convert.ToDouble(reader.GetValue(2), System.Globalization.CultureInfo.InvariantCulture);
        return (avg, samples, spanHours);
    }

    /// <summary>
    /// #2320: the server's core count for the denominator — the same Azure-aware
    /// COALESCE(vcore_count, cpu_count) read the FinOps utilization CTE uses. Null when properties
    /// were never collected; the caller omits the ratio then.
    /// </summary>
    public async Task<int?> GetLatestCpuCountAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT COALESCE(vcore_count, cpu_count)
FROM v_server_properties
WHERE server_id = $1
ORDER BY collection_time DESC
LIMIT 1";
        command.Parameters.Add(new DuckDBParameter { Value = serverId });
        var result = await command.ExecuteScalarAsync();
        if (result is null || result is DBNull)
        {
            return null;
        }

        var cores = Convert.ToInt32(result, System.Globalization.CultureInfo.InvariantCulture);
        return cores > 0 ? cores : null;
    }
}

public class CpuUtilizationRow
{
    public DateTime SampleTime { get; set; }
    public int SqlServerCpu { get; set; }
    public int OtherProcessCpu { get; set; }
    public int TotalCpu => SqlServerCpu + OtherProcessCpu;
    public int IdleCpu => Math.Max(0, 100 - TotalCpu);
}
