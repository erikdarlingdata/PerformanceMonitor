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
}

public class CpuUtilizationRow
{
    public DateTime SampleTime { get; set; }
    public int SqlServerCpu { get; set; }
    public int OtherProcessCpu { get; set; }
    public int TotalCpu => SqlServerCpu + OtherProcessCpu;
    public int IdleCpu => Math.Max(0, 100 - TotalCpu);
}
