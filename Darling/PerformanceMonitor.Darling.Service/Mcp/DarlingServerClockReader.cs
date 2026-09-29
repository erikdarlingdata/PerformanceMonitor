/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// The one place the MCP readers read a monitored server's clock (#4793). Several SQL Server columns hold the
/// server's own LOCAL wall-clock time (a blocked process's last transaction start, a Default Trace event, a
/// running job's start, an index's last access, a version-store cleaner run), while every other timestamp the
/// MCP surface returns is naive UTC. The readers used to turn the local value into UTC in SQL by subtracting the
/// ONE newest <c>server_properties.utc_offset_minutes</c>, which is right only for a value from after the
/// zone's last daylight saving change: a value from before it came back an hour off. Each reader now returns
/// the raw local column and converts each row in C# through the <see cref="ServerClock"/> read here. Where the
/// server reports its time zone (SQL Server 2022 and later) that clock follows the zone across a change; an
/// older server keeps its fixed offset.
///
/// <para>The clock is the newest <c>server_properties</c> row that has an offset: its <c>time_zone_id</c> (a
/// Windows zone id such as "Eastern Standard Time", collected on SQL Server 2022 and later, NULL before) and
/// its <c>utc_offset_minutes</c> come from the SAME row, so the two describe one snapshot.
/// <see cref="ServerClock.Resolve"/> picks the zone when one is reported and resolves on this machine, else
/// the fixed offset, else UTC. A server with no offset collected yet reads as UTC, which is what the old
/// single-row <c>COALESCE(..., 0)</c> did.</para>
/// </summary>
internal static class DarlingServerClockReader
{
    /// <summary>The newest snapshot that has an offset, with its time zone id from the same row. $1 server_id.</summary>
    public const string ServerClockSql = """
        SELECT sp.utc_offset_minutes, sp.time_zone_id
        FROM server_properties AS sp
        WHERE sp.server_id = $1
        AND   sp.utc_offset_minutes IS NOT NULL
        ORDER BY sp.collection_time DESC
        LIMIT 1
        """;

    /// <summary>
    /// The server's clock: its time zone where the newest snapshot carries a resolvable id, else that
    /// snapshot's fixed offset, else UTC (no offset collected yet).
    /// </summary>
    public static async Task<ServerClock> ReadAsync(
        NpgsqlDataSource postgres, int serverId, CancellationToken cancellationToken = default)
    {
        await using var command = postgres.CreateCommand(ServerClockSql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        if (!await reader.ReadAsync(cancellationToken))
        {
            return ServerClock.Utc;
        }

        return ServerClock.Resolve(
            reader.IsDBNull(1) ? null : reader.GetString(1),
            reader.IsDBNull(0) ? null : reader.GetInt32(0));
    }

    /// <summary>
    /// A stored server-local time as naive UTC through <paramref name="clock"/>, or null when the column is
    /// NULL. The one conversion every reader shares, so a NULL stays a NULL and no reader converts a
    /// different way.
    /// </summary>
    public static DateTime? ToUtc(ServerClock clock, System.Data.Common.DbDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : clock.ToUtc(reader.GetDateTime(ordinal));
}
