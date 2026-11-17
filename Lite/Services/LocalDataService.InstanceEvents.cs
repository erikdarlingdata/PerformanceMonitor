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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The DuckDB execution of <see cref="InstanceEvents"/> for the Instance Restarted and Memory Dumps alerts (#8884): the shared SQL text runs unchanged
/// against Lite's <c>collector_state</c>, and the rows go to the shared judges, which hold the whole read-side rule. Darling's twin is
/// <c>DarlingAlertReadAdapter.GetInstanceEventsAsync</c>.
/// </summary>
public sealed partial class LocalDataService
{
    public async Task<InstanceEventInfo> GetInstanceEventsAsync(int serverId)
    {
        using var connection = await OpenConnectionAsync();

        var state = new List<BaselineDiscontinuities.StateRow>();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = InstanceEvents.StateSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                state.Add(new BaselineDiscontinuities.StateRow(
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetDateTime(3)));
            }
        }

        return InstanceEventInfoMapper.ToInstanceEventInfo(state);
    }
}
