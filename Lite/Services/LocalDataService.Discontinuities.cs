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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitorLite.Services;

/// <summary>
/// The DuckDB execution of <see cref="BaselineDiscontinuities"/> (#3653 A5): the two shared SQL texts run
/// unchanged against Lite's store, and the rows go to <see cref="BaselineDiscontinuities.Compose"/>, which
/// holds the whole read-side rule. Read by Lite's trend MCP tools (the payload's trailing
/// <c>discontinuities[]</c>) and by the server tab's four Performance Trends charts (a dashed marker per
/// event), so the two surfaces mark the same instants. Darling's twin is the Storage-side
/// <c>BaselineDiscontinuityReader</c>, which its MCP trend tools and desktop viewer share.
/// </summary>
public partial class LocalDataService
{
    /// <summary>
    /// The baseline discontinuities inside the window every other trend read here takes
    /// (<c>GetTimeRange</c>: hours back from now, a server-local custom range, or an MCP <c>as_of</c> anchor),
    /// oldest first; empty when none. The pair read runs only when a marker row came back.
    /// </summary>
    public async Task<IReadOnlyList<BaselineDiscontinuity>> GetBaselineDiscontinuitiesAsync(
        int serverId, int hoursBack = 24, DateTime? fromDate = null, DateTime? toDate = null, DateTime? asOfUtc = null)
    {
        using var connection = await OpenConnectionAsync();

        var (startTime, endTime) = GetTimeRange(hoursBack, fromDate, toDate, asOfUtc, SelectedServerTabUtcOffsetMinutes);

        var rows = new List<BaselineDiscontinuities.MarkerRow>();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = BaselineDiscontinuities.MarkerRowsSql;
            command.Parameters.Add(new DuckDBParameter { Value = serverId });
            command.Parameters.Add(new DuckDBParameter { Value = startTime });
            command.Parameters.Add(new DuckDBParameter { Value = endTime });

            using var reader = await command.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
                rows.Add(new BaselineDiscontinuities.MarkerRow(
                    reader.GetDateTime(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? null : reader.GetString(2)));
            }
        }

        if (rows.Count == 0)
        {
            return Array.Empty<BaselineDiscontinuity>();
        }

        var state = new List<BaselineDiscontinuities.StateRow>();
        using (var command = connection.CreateCommand())
        {
            command.CommandText = BaselineDiscontinuities.IdentityPairSql;
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

        return BaselineDiscontinuities.Compose(rows, state);
    }
}
