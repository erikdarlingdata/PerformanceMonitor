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
/// #6554: the readable-secondary question for a Query Store read, asked of the store. Moved here from the service's
/// <c>DarlingRuntimePrecondition</c> (#7376) because the viewer's "Resource use by query tag" window has no route to the service assembly and
/// must say the same words the MCP tools say. The judgment and the wording are <see cref="CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage"/>.
/// </summary>
public static class DarlingQueryStoreReadableSecondary
{
    /// <summary>
    /// The three facts <see cref="CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage"/> judges, in one round trip:
    /// the newest collected <c>ag_replica_role</c>, whether the server names a <c>query_store_replica</c>, and how many databases
    /// in the newest <c>query_store_health</c> capture carry the readable-secondary bit (of how many, leaving out tempdb and databases whose
    /// Query Store is OFF: the on-prem capture always holds tempdb, and neither has Query Store rows to read). $1 server_id.
    /// </summary>
    public const string FactsSql = @"
SELECT
    (SELECT sp.ag_replica_role FROM server_properties AS sp WHERE sp.server_id = $1 ORDER BY sp.collection_time DESC LIMIT 1),
    (SELECT COALESCE(NULLIF(btrim(c.query_store_replica), ''), NULL) FROM config.config_monitored_servers AS c WHERE c.server_id = $1),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'),
    (SELECT COUNT(*) FROM v_query_store_health AS h
      WHERE h.server_id = $1 AND (h.readonly_reason & 8) <> 0
        AND h.capture_time = (SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1)
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF')";

    /// <summary>
    /// The one-line reason a Query Store read on this server is empty because it is a readable secondary with <c>query_store_replica</c> off,
    /// or <c>null</c> when it is not or the read fails: it runs on a path that already found nothing.
    /// </summary>
    public static Task<string?> MessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        MessageAsync(postgres, serverId, serverName, healthCaptureRefreshes: true, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// #8361: the same judgment as the gate the collection scheduler applies to <c>query_store_hints</c>. The newest
    /// <c>query_store_health</c> capture is evidence only while the <c>query_store_health</c> collector can refresh it on this server
    /// (<paramref name="healthCaptureRefreshes"/>, its effective schedule's enabled state). When it cannot, the capture is frozen at
    /// whatever role the server had when it was taken, so a server that failed over to primary would read as a secondary for as long
    /// as the capture stood: the capture is then left out and the newest <c>ag_replica_role</c> decides alone. A read's wording
    /// (<see cref="MessageAsync(NpgsqlDataSource, int, string, int, CancellationToken)"/>) always counts the capture.
    /// </summary>
    public static async Task<string?> MessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, bool healthCaptureRefreshes, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        try
        {
            await using var command = postgres.CreateCommand(FactsSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = serverId });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                return null;
            }

            return CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage(
                serverName,
                reader.IsDBNull(0) ? null : reader.GetString(0),
                healthCaptureRefreshes ? (int)reader.GetInt64(2) : 0,
                healthCaptureRefreshes ? (int)reader.GetInt64(3) : 0,
                replicaConfigured: !reader.IsDBNull(1),
                SettingClause);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// #8361: <see cref="FactsSql"/> for a set of servers in one statement, $1 an integer array of server ids: one row per id, with
    /// the newest <c>ag_replica_role</c>, whether a <c>query_store_replica</c> is named, and the newest capture's counts (0 and 0 for
    /// a server with no capture). The fleet form of <c>get_collection_health</c> answers outside admission, so its reasons cost one
    /// command for the whole fleet rather than one per server.
    /// </summary>
    public const string FleetFactsSql = @"
WITH ids AS (SELECT unnest($1::integer[]) AS server_id),
role AS (
    SELECT DISTINCT ON (sp.server_id) sp.server_id, sp.ag_replica_role
    FROM server_properties AS sp
    JOIN ids ON ids.server_id = sp.server_id
    ORDER BY sp.server_id, sp.collection_time DESC),
newest AS (
    SELECT h.server_id, MAX(h.capture_time) AS capture_time
    FROM v_query_store_health AS h
    JOIN ids ON ids.server_id = h.server_id
    GROUP BY h.server_id),
counts AS (
    SELECT h.server_id,
           COUNT(*) AS health_databases,
           COUNT(*) FILTER (WHERE (h.readonly_reason & 8) <> 0) AS health_secondary_databases
    FROM v_query_store_health AS h
    JOIN newest AS n ON n.server_id = h.server_id AND n.capture_time = h.capture_time
    WHERE h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'
    GROUP BY h.server_id)
SELECT ids.server_id,
       role.ag_replica_role,
       EXISTS (SELECT 1 FROM config.config_monitored_servers AS c WHERE c.server_id = ids.server_id AND NULLIF(btrim(c.query_store_replica), '') IS NOT NULL) AS replica_configured,
       COALESCE(counts.health_databases, 0),
       COALESCE(counts.health_secondary_databases, 0)
FROM ids
LEFT JOIN role ON role.server_id = ids.server_id
LEFT JOIN counts ON counts.server_id = ids.server_id";

    /// <summary>One server the fleet form asks about: its id, the name the sentence carries, and whether its <c>query_store_health</c> capture can refresh.</summary>
    public readonly record struct FleetServer(int ServerId, string ServerName, bool HealthCaptureRefreshes);

    /// <summary>
    /// #8361: the readable-secondary sentence for each of <paramref name="servers"/> that is one, under the gate's judgment
    /// (<see cref="MessageAsync(NpgsqlDataSource, int, string, bool, int, CancellationToken)"/>), from one <see cref="FleetFactsSql"/>
    /// round trip; a server that is not one has no entry. Empty when the read fails or there is no server to ask about.
    /// </summary>
    public static async Task<IReadOnlyDictionary<int, string>> MessagesAsync(
        NpgsqlDataSource postgres, IReadOnlyList<FleetServer> servers, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
    {
        var messages = new Dictionary<int, string>();
        if (servers.Count == 0)
        {
            return messages;
        }

        try
        {
            var byId = servers.ToDictionary(s => s.ServerId);
            await using var command = postgres.CreateCommand(FleetFactsSql);
            command.CommandTimeout = commandTimeoutSeconds;
            command.Parameters.Add(new NpgsqlParameter<int[]> { TypedValue = servers.Select(s => s.ServerId).Distinct().ToArray() });
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                if (!byId.TryGetValue(reader.GetInt32(0), out var server))
                {
                    continue;
                }

                var message = CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage(
                    server.ServerName,
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    server.HealthCaptureRefreshes ? (int)reader.GetInt64(3) : 0,
                    server.HealthCaptureRefreshes ? (int)reader.GetInt64(4) : 0,
                    replicaConfigured: reader.GetBoolean(2),
                    SettingClause);
                if (message is not null)
                {
                    messages[server.ServerId] = message;
                }
            }
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            messages.Clear();
        }

        return messages;
    }

    /// <summary>Why the product does not read Query Store on a readable secondary, and the setting that changes that.</summary>
    private const string SettingClause =
        "Darling collects Query Store from a registered server only when that server is not a readable secondary, and query_store_replica is off for it. " +
        "Register the primary, or set query_store_replica on the primary to read Query Store from a readable secondary (edit_server).";
}
