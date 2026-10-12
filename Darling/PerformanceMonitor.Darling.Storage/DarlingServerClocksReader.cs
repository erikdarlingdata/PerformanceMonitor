/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Analysis.Baselines;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Each server's clock from its newest <c>server_properties</c> row that has an offset (the time zone id alongside it
/// where the store has the V134 column, else the offset alone), keyed by server id. A server with no row is absent,
/// and its stored times are read as UTC. With a server id it reads that one server (#4766).
/// </summary>
public static class DarlingServerClocksReader
{
    /// <summary>
    /// The clocks, one per server (or the one asked for). A store below V134 has no <c>time_zone_id</c>: that read
    /// falls back to the offset alone.
    /// </summary>
    public static async Task<Dictionary<int, ServerClock>> GetAsync(
        NpgsqlDataSource dataSource, int? serverId, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        try
        {
            return await ReadAsync(dataSource, BuildSql(serverId.HasValue, withZone: true), serverId, withZone: true, commandTimeoutSeconds, cancellationToken);
        }
        catch (PostgresException ex) when (ex.SqlState == PostgresErrorCodes.UndefinedColumn)
        {
            return await ReadAsync(dataSource, BuildSql(serverId.HasValue, withZone: false), serverId, withZone: false, commandTimeoutSeconds, cancellationToken);
        }
    }

    /// <summary>The statement: $1 is the server id when <paramref name="scopedToServer"/>.</summary>
    public static string BuildSql(bool scopedToServer, bool withZone)
    {
        var zone = withZone ? ",\n    time_zone_id" : "";
        var scope = scopedToServer ? "AND   server_id = $1\n" : "";
        return $@"
SELECT DISTINCT ON (server_id)
    server_id,
    utc_offset_minutes{zone}
FROM server_properties
WHERE utc_offset_minutes IS NOT NULL
{scope}ORDER BY server_id, collection_time DESC";
    }

    /// <summary><paramref name="serverId"/>'s entry in <paramref name="clocks"/>, else the UTC clock.</summary>
    public static ServerClock ClockFor(IReadOnlyDictionary<int, ServerClock> clocks, int serverId)
    {
        System.ArgumentNullException.ThrowIfNull(clocks);
        return clocks.TryGetValue(serverId, out var known) ? known : ServerClock.Utc;
    }

    private static async Task<Dictionary<int, ServerClock>> ReadAsync(
        NpgsqlDataSource dataSource, string sql, int? serverId, bool withZone, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var clocks = new Dictionary<int, ServerClock>();

        await using var command = dataSource.CreateCommand(sql);
        command.CommandTimeout = commandTimeoutSeconds;
        if (serverId.HasValue)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId.Value });
        }

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            clocks[reader.GetInt32(0)] = ServerClock.Resolve(
                withZone && !reader.IsDBNull(2) ? reader.GetString(2) : null,
                reader.IsDBNull(1) ? null : reader.GetInt32(1));
        }

        return clocks;
    }
}
