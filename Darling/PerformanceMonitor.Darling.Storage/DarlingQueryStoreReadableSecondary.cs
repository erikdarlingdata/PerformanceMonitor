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
    public static async Task<string?> MessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int commandTimeoutSeconds, CancellationToken cancellationToken = default)
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
                (int)reader.GetInt64(2),
                (int)reader.GetInt64(3),
                replicaConfigured: !reader.IsDBNull(1),
                "Darling collects Query Store from a registered server only when that server is not a readable secondary, and query_store_replica is off for it. " +
                "Register the primary, or set query_store_replica on the primary to read Query Store from a readable secondary (edit_server).");
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }
}
