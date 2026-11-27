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
using Npgsql;
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// #8913: the optional extensions each server's newest extension census (<c>pg_extension_availability</c>) shows as ABSENT, meaning the
/// server does not list them in <c>pg_available_extensions</c> at all. Only the extensions the Aurora-gated collectors
/// (<see cref="CollectorEngineCapability.AuroraOptionalExtensionCollectors"/>) cannot run without are read. The answer feeds
/// <see cref="CollectorEngineCapability.NotCollectedMessage"/>, which turns it into an engine limit on Aurora only. Unknown is not absent:
/// a server with no census row inside the lookback, or a read that fails, is left out and makes no claim.
/// </summary>
public static class ExtensionsNotOffered
{
    /// <summary>How far back the newest census row may be. The census is a daily collector, so a week keeps a server whose census
    /// paused over a weekend from falling back to the setup wording.</summary>
    public const int LookbackDays = 7;

    /// <summary>
    /// The (server, extension) pairs whose newest census capture has no row in any state but <c>absent</c>. The state is a cluster-wide
    /// fact (<c>pg_available_extensions</c>), so every database row of one capture agrees. $1 extension names, $2 lookback start,
    /// $3 server id or NULL for every server.
    /// </summary>
    public const string Sql = @"
SELECT a.server_id, a.extension_name
FROM pg_extension_availability AS a
JOIN (
    SELECT server_id, extension_name, MAX(collection_time) AS newest
    FROM pg_extension_availability
    WHERE extension_name = ANY($1)
    AND   collection_time >= $2
    AND   ($3::integer IS NULL OR server_id = $3)
    GROUP BY server_id, extension_name
) AS l
  ON  l.server_id = a.server_id
  AND l.extension_name = a.extension_name
  AND l.newest = a.collection_time
GROUP BY a.server_id, a.extension_name
HAVING count(*) FILTER (WHERE a.state IS DISTINCT FROM 'absent') = 0";

    /// <summary>The extension names the Aurora-gated collectors declare, the only ones the census is asked about.</summary>
    public static string[] WatchedExtensions { get; } = CollectorEngineCapability.AuroraOptionalExtensionCollectors
        .Select(name => CollectorCatalog.Find(name))
        .Where(definition => definition is not null)
        .SelectMany(definition => definition!.RequiredPgExtensions.Select(r => r.ExtensionName))
        .Distinct(StringComparer.Ordinal)
        .OrderBy(name => name, StringComparer.Ordinal)
        .ToArray();

    /// <summary>
    /// The absent extensions by server, for one server or (<paramref name="serverId"/> null) every server. A failed read answers none,
    /// as <see cref="FleetEngineGatedCollectors.ReadAsync"/> does: with no evidence the precondition wording stands.
    /// </summary>
    public static async Task<Dictionary<int, HashSet<string>>> ReadAsync(
        NpgsqlDataSource postgres, int? serverId, int commandTimeoutSeconds, CancellationToken cancellationToken)
    {
        var byServer = new Dictionary<int, HashSet<string>>();
        try
        {
            await using var command = postgres.CreateCommand(Sql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<string[]> { TypedValue = WatchedExtensions });
            command.Parameters.Add(new NpgsqlParameter<DateTime>
            {
                TypedValue = DateTime.SpecifyKind(DateTime.UtcNow.AddDays(-LookbackDays), DateTimeKind.Unspecified),
            });
            command.Parameters.Add(new NpgsqlParameter
            {
                NpgsqlDbType = NpgsqlTypes.NpgsqlDbType.Integer,
                Value = serverId is { } wanted ? wanted : DBNull.Value,
            });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var id = reader.GetInt32(0);
                if (!byServer.TryGetValue(id, out var names))
                {
                    names = new HashSet<string>(StringComparer.Ordinal);
                    byServer[id] = names;
                }

                names.Add(reader.GetString(1));
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            byServer.Clear();
        }

        return byServer;
    }
}
