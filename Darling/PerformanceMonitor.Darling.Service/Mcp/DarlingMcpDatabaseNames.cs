/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using PerformanceMonitor.Common;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service.Mcp;

/// <summary>
/// #9065: Darling's read behind <see cref="McpDatabaseNameFilter"/>: the stored spellings of a database name on one server, from
/// that server's database lists. A SQL Server's list is a <c>database_states</c> snapshot, the same list <c>find_database</c> and
/// the dropped-database scope read; a PostgreSQL target's is a <c>pg_database_stats</c> snapshot. A server writes one of the two,
/// so the second is read only when the first holds no list. Lite's twin is <c>McpDatabaseNames</c>.
///
/// <para>The newest list gives the spellings that equal the given name ignoring case. When none of them is the given spelling,
/// one more read asks whether one of the server's hourly lists in the look-back holds the given spelling exactly
/// (<see cref="SeenSql"/>), and adds it when one does: a database renamed or recreated in another letter case keeps its earlier
/// history reachable under the spelling that history was stored with. The hourly lists, not every list, are read because the
/// lists are collected as often as once a minute and no index holds the name: every list of a week is millions of rows for a
/// large server, the hourly ones are at most 167 lists however often they are collected. A spelling the server carried for less
/// than an hour can fall between two of them and is then not recognised. A name that is on the newest list as given, and a name
/// the newest list holds in no letter case, never pay for that read.</para>
///
/// <para>The server is resolved as the tool resolves it (<see cref="DarlingServerResolver.ResolveOrErrorAsync"/>, with
/// <see cref="NameClash.Refuse"/> for a tool that changes state), so the name is looked up on the server the tool then uses; a
/// server that does not resolve is left to the tool, which reports it. A server with neither list in
/// <see cref="DarlingMcpFindDatabaseTools.LookBack"/> (an Azure SQL Database, which writes none, or a stopped collector) has
/// nothing to resolve against. Every statement carries the lookup's own short timeout.</para>
/// </summary>
internal static class DarlingMcpDatabaseNames
{
    /// <summary>
    /// The names on a SQL Server's newest database list that equal <c>$3</c> ignoring case: no row when the server has no list newer
    /// than <c>$2</c>, one row with a NULL name when the list holds no such name. <c>$1</c> server_id. The newest stamp and the rows
    /// at it both read <c>idx_database_states_server_time_cover (server_id, collection_time) INCLUDE (database_name)</c>; only the matching names leave the store,
    /// each with the time the list was taken.
    /// </summary>
    public const string CandidatesSql = @"
SELECT d.database_name, n.t
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
SELECT d.database_name, n.t
FROM (SELECT MAX(collection_time) AS t
      FROM pg_database_stats
      WHERE server_id = $1
      AND   collection_time > $2) n
LEFT JOIN pg_database_stats d
       ON d.server_id = $1
      AND d.collection_time = n.t
      AND lower(d.database_name) = lower($3)
WHERE n.t IS NOT NULL";

    /// <summary>
    /// Whether one of a SQL Server's hourly database lists newer than <c>$2</c> holds the name <c>$3</c> exactly, for server
    /// <c>$1</c>. The hourly lists are the newest list at or before each of <c>$4</c> points one hour apart, counted forward
    /// from <c>$2</c>. Each point is one backward probe of <c>idx_database_states_server_time_cover</c> and each list one range read of it, so
    /// the cost is bounded by the points and the size of one list, not by how often the lists are collected.
    /// </summary>
    public const string SeenSql = @"
SELECT EXISTS (
    SELECT 1
    FROM (SELECT DISTINCT newest.collection_time
          FROM generate_series(1, $4) AS step(n)
          CROSS JOIN LATERAL (
              SELECT s.collection_time
              FROM database_states s
              WHERE s.server_id = $1
              AND   s.collection_time > $2
              AND   s.collection_time <= $2 + step.n * interval '1 hour'
              ORDER BY s.collection_time DESC
              LIMIT 1) newest) hourly
    JOIN database_states d
      ON  d.server_id = $1
      AND d.collection_time = hourly.collection_time
      AND d.database_name = $3)";

    /// <summary><see cref="SeenSql"/> for a PostgreSQL target, over <c>pg_database_stats</c> and <c>idx_pg_database_stats_time</c>.</summary>
    public const string PgSeenSql = @"
SELECT EXISTS (
    SELECT 1
    FROM (SELECT DISTINCT newest.collection_time
          FROM generate_series(1, $4) AS step(n)
          CROSS JOIN LATERAL (
              SELECT s.collection_time
              FROM pg_database_stats s
              WHERE s.server_id = $1
              AND   s.collection_time > $2
              AND   s.collection_time <= $2 + step.n * interval '1 hour'
              ORDER BY s.collection_time DESC
              LIMIT 1) newest) hourly
    JOIN pg_database_stats d
      ON  d.server_id = $1
      AND d.collection_time = hourly.collection_time
      AND d.database_name = $3)";

    /// <summary>
    /// The hourly points <see cref="SeenSql"/> reads: one for each whole hour inside <see cref="DarlingMcpFindDatabaseTools.LookBack"/>.
    /// The point at its end is left out: the list there is the newest one, which the lookup has already read.
    /// </summary>
    internal static readonly int HourlyLists = (int)DarlingMcpFindDatabaseTools.LookBack.TotalHours - 1;

    /// <summary>The <see cref="McpDatabaseNameSource"/> the host registers: the store is the request's <see cref="NpgsqlDataSource"/>.</summary>
    public static async Task<McpDatabaseNameLookup?> CandidatesAsync(
        IServiceProvider? services, string? serverName, string databaseName, bool changesState, CancellationToken cancellationToken)
    {
        if (services?.GetService<NpgsqlDataSource>() is not { } postgres)
        {
            return null;
        }

        var (resolved, error) = await DarlingServerResolver.ResolveOrErrorAsync(
            postgres, serverName, cancellationToken, changesState ? NameClash.Refuse : NameClash.StoredNameWins).ConfigureAwait(false);
        return error is null
            ? await CandidatesAsync(postgres, resolved.ServerId, databaseName, DateTime.UtcNow, cancellationToken).ConfigureAwait(false)
            : null;
    }

    /// <summary>
    /// The read itself, for one resolved server: null for no list, empty for a list without the name, else the newest list's
    /// spellings of it plus the given spelling when an earlier hourly list holds it. The time is the newest list's.
    /// </summary>
    internal static async Task<McpDatabaseNameLookup?> CandidatesAsync(
        NpgsqlDataSource postgres, int serverId, string databaseName, DateTime nowUtc, CancellationToken cancellationToken)
    {
        var floor = nowUtc - DarlingMcpFindDatabaseTools.LookBack;
        foreach (var (candidatesSql, seenSql) in new[] { (CandidatesSql, SeenSql), (PgCandidatesSql, PgSeenSql) })
        {
            if (await ReadAsync(postgres, candidatesSql, serverId, floor, databaseName, cancellationToken).ConfigureAwait(false) is not { } list)
            {
                continue;
            }

            var (names, taken) = list;
            if (names.Count > 0 && !names.Contains(databaseName, StringComparer.Ordinal)
                && await SeenAsync(postgres, seenSql, serverId, floor, databaseName, cancellationToken).ConfigureAwait(false))
            {
                names.Add(databaseName);
            }

            return new McpDatabaseNameLookup(names, taken);
        }

        return null;
    }

    private static NpgsqlCommand Command(NpgsqlDataSource postgres, string sql, int serverId, DateTime floor, string databaseName)
    {
        var command = postgres.CreateCommand(sql);
        command.CommandTimeout = (int)McpDatabaseNameFilter.LookupDeadline.TotalSeconds;
        DarlingMcpReadParameters.AddInt(command, serverId);
        DarlingMcpReadParameters.AddTimestamp(command, floor);
        DarlingMcpReadParameters.AddText(command, databaseName);
        return command;
    }

    private static async Task<(List<string> Names, DateTime Taken)?> ReadAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime floor, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = Command(postgres, sql, serverId, floor, databaseName);

        List<string>? names = null;
        var taken = default(DateTime);
        await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
        {
            names ??= new List<string>();
            taken = reader.GetDateTime(1);
            if (!reader.IsDBNull(0))
            {
                names.Add(reader.GetString(0));
            }
        }

        return names is null ? null : (names, taken);
    }

    private static async Task<bool> SeenAsync(
        NpgsqlDataSource postgres, string sql, int serverId, DateTime floor, string databaseName, CancellationToken cancellationToken)
    {
        await using var command = Command(postgres, sql, serverId, floor, databaseName);
        DarlingMcpReadParameters.AddInt(command, HourlyLists);
        return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false) is true;
    }
}
