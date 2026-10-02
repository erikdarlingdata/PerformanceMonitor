/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// The PostgreSQL-store execution of <see cref="BaselineDiscontinuities"/> (#3653 A5): runs the two shared SQL
/// texts over Npgsql and hands the rows to <see cref="BaselineDiscontinuities.Compose"/>, which holds the whole
/// read-side rule. Here in Storage rather than in the MCP reader because two Darling surfaces read it — the
/// service's trend tools through <c>DarlingTrendReader</c> and the desktop viewer's Performance Trends charts
/// through <c>ViewerDataService</c> — and the viewer does not reference the service. Same placement as
/// <see cref="QueryStoreTrendRouting.ResolveAsync"/>, for the same reason. Lite's twin is
/// <c>LocalDataService.GetBaselineDiscontinuitiesAsync</c> over DuckDB, executing the same two texts.
/// </summary>
public static class BaselineDiscontinuityReader
{
    /// <summary>
    /// The discontinuities for <paramref name="serverId"/> inside [<paramref name="startUtc"/>, <paramref name="endUtc"/>],
    /// oldest first; empty when the window holds none. The pair read runs only when at least one marker row came
    /// back, so the steady-state cost of attaching this to every trend payload is one indexed range scan over
    /// the rows that carry a note.
    /// </summary>
    /// <param name="commandTimeoutSeconds">The caller's deadline class — the MCP read deadline by default; the viewer passes its interactive one.</param>
    public static async Task<IReadOnlyList<BaselineDiscontinuity>> ReadAsync(
        NpgsqlDataSource postgres, int serverId, DateTime startUtc, DateTime endUtc,
        int commandTimeoutSeconds = StorageCommandDeadlines.McpReadSeconds,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(postgres);

        var rows = new List<BaselineDiscontinuities.MarkerRow>();
        await using (var command = postgres.CreateCommand(BaselineDiscontinuities.MarkerRowsSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            /* SpecifyKind(Unspecified), not the bare value: Npgsql infers timestamptz from Kind=Utc and PostgreSQL
               then zone-shifts the window against the store's NAIVE timestamp columns — the convention every
               other PostgreSQL window read here follows (DarlingPgBlockingReader, DarlingPgXminReader). */
            command.Parameters.AddWithValue(DateTime.SpecifyKind(startUtc, DateTimeKind.Unspecified));
            command.Parameters.AddWithValue(DateTime.SpecifyKind(endUtc, DateTimeKind.Unspecified));
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
        await using (var command = postgres.CreateCommand(BaselineDiscontinuities.IdentityPairSql))
        {
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.AddWithValue(serverId);
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
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
