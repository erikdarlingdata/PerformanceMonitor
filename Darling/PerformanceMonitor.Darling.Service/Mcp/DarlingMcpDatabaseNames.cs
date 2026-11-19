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
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9065: Darling's read behind <see cref="McpDatabaseNameFilter"/>: the stored spellings of a database name on one server, from
/// that server's newest database list. A SQL Server's list is its newest <c>database_states</c> snapshot, the same list
/// <c>find_database</c> and the dropped-database scope read; a PostgreSQL target's is its newest <c>pg_database_stats</c> snapshot.
/// A server writes one of the two, so the second is read only when the first holds no list. Lite's twin is <c>McpDatabaseNames</c>.
///
/// <para>The server is resolved as every tool resolves it (<see cref="DarlingServerResolver.ResolveOrErrorAsync"/>), so the name is
/// looked up on the server the tool then reads; a server that does not resolve is left to the tool, which reports it. A server with
/// neither list in <see cref="DarlingMcpFindDatabaseTools.LookBack"/> (an Azure SQL Database, which writes none, or a stopped
/// collector) has nothing to resolve against.</para>
/// </summary>
internal static class DarlingMcpDatabaseNames
{
    /// <summary>
    /// The names on a SQL Server's newest database list that equal <c>$3</c> ignoring case: no row when the server has no list newer
    /// than <c>$2</c>, one row with a NULL name when the list holds no such name. <c>$1</c> server_id. The newest stamp and the rows
    /// at it both read <c>idx_database_states_time (server_id, collection_time)</c>; only the matching names leave the store.
    /// </summary>
    public const string CandidatesSql = @"
SELECT d.database_name
FROM (SELECT MAX(collection_time) AS t
      FROM database_states
      WHERE server_id = $1
      AND   collection_time > $2) n
LEFT JOIN database_states d
       ON d.server_id = $1
      AND d.collection_time = n.t
      AND lower(d.database_name) = lower($3)
WHERE n.t IS NOT NULL";

    /// <summary>
    /// <see cref="CandidatesSql"/> for a PostgreSQL target, over its newest <c>pg_database_stats</c> snapshot and
    /// <c>idx_pg_database_stats_time (server_id, collection_time)</c>. The snapshot's shared-relations row has a NULL name and
    /// matches nothing.
    /// </summary>
    public const string PgCandidatesSql = @"
SELECT d.database_name
FROM (SELECT MAX(collection_time) AS t
      FROM pg_database_stats
      WHERE server_id = $1
      AND   collection_time > $2) n
LEFT JOIN pg_database_stats d
       ON d.server_id = $1
      AND d.collection_time = n.t
      AND lower(d.database_name) = lower($3)
WHERE n.t IS NOT NULL";

    /// <summary>The <see cref="McpDatabaseNameSource"/> the host registers: the store is the request's <see cref="NpgsqlDataSource"/>.</summary>
    public static async Task<IReadOnlyList<string>?> CandidatesAsync(
        IServiceProvider? services, string? serverName, string databaseName, CancellationToken cancellationToken)
    {
        if (services?.GetService<NpgsqlDataSource>() is not { } postgres)
        {
            return null;
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(postgres, serverName, cancellationToken).ConfigureAwait(false);
        return error is null
            ? await CandidatesAsync(postgres, resolved.ServerId, databaseName, DateTime.UtcNow, cancellationToken).ConfigureAwait(false)
            : null;
    }

    /// <summary>The read itself, for one resolved server: null for no list, empty for a list without the name.</summary>
    internal static async Task<IReadOnlyList<string>?> CandidatesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var floor = nowUtc - DarlingMcpFindDatabaseTools.LookBack;
        return await ReadAsync(postgres, CandidatesSql, serverId, floor, databaseName, cancellationToken).ConfigureAwait(false)
            ?? await ReadAsync(postgres, PgCandidatesSql, serverId, floor, databaseName, cancellationToken).ConfigureAwait(false);
    }

    private static async Task<IReadOnlyList<string>?> ReadAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime floor, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = postgres.CreateCommand(sql);
        command.CommandTimeout = McpCommandDeadlines.ReadSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, floor);
        DarlingMcpReadParameters.AddText(command, databaseName);

        List<string>? names = null;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            names ??= new List<string>();
            if (!reader.IsDBNull(0))
            {
                names.Add(reader.GetString(0));
            }
        }

        return names;
    }
}
