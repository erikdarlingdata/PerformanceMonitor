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
    /// Query Store is OFF: the on-prem capture always holds tempdb, and neither has Query Store rows to read). Beside them, the
    /// newest successful <c>query_store</c> and <c>query_store_hints</c> runs newer than that capture: the first's enumeration
    /// count, the second's stored rows and its age on the store's clock. $1 server_id.
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
        AND h.database_name <> 'tempdb' AND UPPER(COALESCE(h.actual_state, '')) <> 'OFF'),
    run.fanout_item_count::bigint,
    run.log_id IS NOT NULL,
    hints.rows_collected::bigint,
    hints.age_seconds
FROM (SELECT 1) AS one
LEFT JOIN LATERAL (
    SELECT cl.log_id, cl.fanout_item_count
    FROM collection_log AS cl
    WHERE cl.server_id = $1 AND cl.collector_name = 'query_store' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE((SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1), '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS run ON TRUE
LEFT JOIN LATERAL (
    SELECT cl.rows_collected, EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - cl.collection_time))::double precision AS age_seconds
    FROM collection_log AS cl
    WHERE cl.server_id = $1 AND cl.collector_name = 'query_store_hints' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE((SELECT MAX(h2.capture_time) FROM v_query_store_health AS h2 WHERE h2.server_id = $1), '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS hints ON TRUE";

    /// <summary>
    /// #8541: how long a <c>query_store_hints</c> run that captured no database keeps the scheduler's gate closed over a frozen
    /// capture: one hour, the cadence the <c>query_store_health</c> collector refreshes the capture at where it runs. After it the
    /// gate lets one run through, and that run's own read of each database's <c>readonly_reason</c> decides the next hour.
    /// </summary>
    public static readonly TimeSpan OwnRunProbeInterval = TimeSpan.FromHours(1);

    /// <summary>
    /// The judgment with the newest capture counted as one the <c>query_store_health</c> collector refreshes, for a caller that has not
    /// resolved that collector's schedule. The service's reads, the scheduler's gate, <c>get_collection_health</c> and the viewer's tag
    /// roll-up resolve it and call <see cref="MessageAsync(NpgsqlDataSource, int, string, bool, int, CancellationToken)"/>.
    /// </summary>
    public static Task<string?> MessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        MessageAsync(postgres, serverId, serverName, healthCaptureRefreshes: true, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// #6554, #8361: the one-line reason a Query Store read on this server is empty because it is a readable secondary with
    /// <c>query_store_replica</c> off, or <c>null</c> when it is not or the read fails (it runs on a path that already found nothing).
    /// The judgment the collection scheduler's gate on <c>query_store_hints</c>, <c>get_collection_health</c>, the Query Store reads'
    /// <c>not_collected</c> answer and the viewer's tag roll-up all apply, <see cref="Message"/>, from <see cref="FactsSql"/>.
    /// <paramref name="healthCaptureRefreshes"/> is whether the <c>query_store_health</c> collector's effective schedule lets it refresh
    /// the capture on this server.
    /// </summary>
    public static Task<string?> MessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, bool healthCaptureRefreshes, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        MessageCoreAsync(postgres, serverId, serverName, healthCaptureRefreshes, ownRunMaxAge: null, commandTimeoutSeconds, cancellationToken);

    /// <summary>
    /// #8541: the judgment as the scheduler's gate on <c>query_store_hints</c> asks it. The same facts and rule as
    /// <see cref="MessageAsync(NpgsqlDataSource, int, string, bool, int, CancellationToken)"/>, except that the collector's own run
    /// that captured no database counts only while it is no older than <see cref="OwnRunProbeInterval"/>. Past that the gate opens for
    /// one run, so a server whose databases stopped being secondaries is found within the hour, while the reads keep the answer the
    /// newest run gave.
    /// </summary>
    public static Task<string?> GateMessageAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, bool healthCaptureRefreshes, int commandTimeoutSeconds, CancellationToken cancellationToken = default) =>
        MessageCoreAsync(postgres, serverId, serverName, healthCaptureRefreshes, OwnRunProbeInterval, commandTimeoutSeconds, cancellationToken);

    private static async Task<string?> MessageCoreAsync(
        NpgsqlDataSource postgres, int serverId, string serverName, bool healthCaptureRefreshes, TimeSpan? ownRunMaxAge, int commandTimeoutSeconds, CancellationToken cancellationToken)
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

            return Message(
                serverName,
                reader.IsDBNull(0) ? null : reader.GetString(0),
                (int)reader.GetInt64(2),
                (int)reader.GetInt64(3),
                replicaConfigured: !reader.IsDBNull(1),
                healthCaptureRefreshes,
                reader.IsDBNull(4) ? null : reader.GetInt64(4),
                reader.GetBoolean(5),
                OwnRunCapturedNothing(
                    reader.IsDBNull(6) ? null : reader.GetInt64(6),
                    reader.IsDBNull(7) ? null : reader.GetDouble(7),
                    ownRunMaxAge));
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            return null;
        }
    }

    /// <summary>
    /// #8402: the one judgment the scheduler's gate, <c>get_collection_health</c> and the Query Store reads apply to a server's facts,
    /// <see cref="CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage"/> with the frozen-capture rule in front of it. The
    /// newest <c>query_store_health</c> capture counts while the <c>query_store_health</c> collector can refresh it
    /// (<paramref name="healthCaptureRefreshes"/>). When it cannot, the capture is frozen at whatever role the server had when it was
    /// taken, and only an Availability Group role in the newest <c>ag_replica_role</c> can overrule it (<see cref="RoleAnswers"/>): a
    /// frozen all-secondary capture under a PRIMARY role is a failover, and the collector runs; a frozen capture under a SECONDARY role
    /// is read as that role. A role that is null, Standalone or anything else carries no answer: the properties collector writes
    /// Standalone for every instance outside an availability group, a readable standby and a primary promoted after a failover alike,
    /// so the newest successful <c>query_store</c> collector run newer than the capture decides, by its enumeration (a fact that
    /// refreshes on its own cadence whatever <c>query_store_health</c>'s schedule). The enumeration skips the same readable-secondary
    /// databases, and a database whose Query Store is off, so a run that found databases to read (<paramref name="newerRunFanout"/>
    /// above zero) leaves the capture out whatever it stored; a run that found none (no fanout) corroborates the frozen capture, which
    /// then decides; a run from before the capture, or none at all (<paramref name="newerRunExists"/> false), decides nothing and the
    /// role decides. A database with Query Store off is left out of the capture's counts, so a frozen capture of such databases never
    /// reads as a secondary. Null when the server is not a readable secondary under that rule, or a <c>query_store_replica</c> is set for it.
    /// This form judges without the <c>query_store_hints</c> collector's own run; the store reads pass it to the form that takes it.
    /// </summary>
    /// <param name="healthDatabases">Databases in the newest capture, leaving out tempdb and any database whose Query Store is OFF.</param>
    /// <param name="healthSecondaryDatabases">Of those, how many carry the readable-secondary bit.</param>
    /// <param name="newerRunFanout">The databases the newest successful <c>query_store</c> run newer than the capture found to read (<c>fanout_item_count</c>); null when it found none, or when there is no such run.</param>
    /// <param name="newerRunExists">Whether a successful <c>query_store</c> run newer than the capture is logged at all. A run from before the capture decides nothing.</param>
    public static string? Message(
        string serverName, string? agReplicaRole, int healthDatabases, int healthSecondaryDatabases, bool replicaConfigured, bool healthCaptureRefreshes,
        long? newerRunFanout, bool newerRunExists) =>
        Message(
            serverName, agReplicaRole, healthDatabases, healthSecondaryDatabases, replicaConfigured, healthCaptureRefreshes,
            newerRunFanout, newerRunExists, ownRunCapturedNothing: false);

    /// <summary>
    /// #8541: <see cref="Message(string, string?, int, int, bool, bool, long?, bool)"/> with the <c>query_store_hints</c> collector's
    /// own newest run as a fact. That collector writes one marker row for every database it captures and skips a database whose
    /// <c>readonly_reason</c> carries the readable-secondary bit, so a successful run newer than the capture that stored no row
    /// (<paramref name="ownRunCapturedNothing"/>) read each database's own Query Store state and found none to capture. It keeps a
    /// frozen capture counting whatever the role reads: the forwarder of a distributed availability group, a cross-region read
    /// replica among them, reports PRIMARY for its local group while every database's Query Store is a secondary's. A newer
    /// <c>query_store</c> run that found databases to read overrules it, and a run that stored a marker row leaves the role and the
    /// <c>query_store</c> run to decide, as a failover does.
    /// </summary>
    public static string? Message(
        string serverName, string? agReplicaRole, int healthDatabases, int healthSecondaryDatabases, bool replicaConfigured, bool healthCaptureRefreshes,
        long? newerRunFanout, bool newerRunExists, bool ownRunCapturedNothing)
    {
        /* A live primary's run stores zero rows most slots (closed intervals only), so its rows say nothing; the enumeration does. */
        var queryStoreRunFoundDatabases = newerRunExists && (newerRunFanout ?? 0) > 0;
        var captureCounts = healthCaptureRefreshes
            || (ownRunCapturedNothing && !queryStoreRunFoundDatabases)
            || (!RoleAnswers(agReplicaRole) && newerRunExists && (newerRunFanout ?? 0) == 0);
        return CollectorRuntimePrecondition.QueryStoreReadableSecondaryMessage(
            serverName,
            agReplicaRole,
            captureCounts ? healthDatabases : 0,
            captureCounts ? healthSecondaryDatabases : 0,
            replicaConfigured,
            SettingClause);
    }

    /// <summary>
    /// Whether the newest successful <c>query_store_hints</c> run newer than the capture stored no row at all
    /// (<paramref name="rowsCollected"/> zero; null when there is no such run), and is no older than
    /// <paramref name="maxAge"/> when one is given (<paramref name="ageSeconds"/> on the store's clock).
    /// </summary>
    public static bool OwnRunCapturedNothing(long? rowsCollected, double? ageSeconds, TimeSpan? maxAge) =>
        rowsCollected == 0 && (maxAge is null || (ageSeconds ?? double.MaxValue) <= maxAge.Value.TotalSeconds);

    /// <summary>
    /// Whether a collected <c>ag_replica_role</c> names an Availability Group role, PRIMARY or SECONDARY. Standalone (a server
    /// outside any availability group, a readable standby among them), RESOLVING, blank and null do not, and say nothing about
    /// whether the server's Query Store is the primary's, replicated.
    /// </summary>
    public static bool RoleAnswers(string? agReplicaRole)
    {
        var role = agReplicaRole?.Trim();
        return string.Equals(role, "Primary", StringComparison.OrdinalIgnoreCase)
            || string.Equals(role, "Secondary", StringComparison.OrdinalIgnoreCase);
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
       COALESCE(counts.health_secondary_databases, 0),
       run.fanout_item_count::bigint,
       run.log_id IS NOT NULL,
       hints.rows_collected::bigint,
       hints.age_seconds
FROM ids
LEFT JOIN role ON role.server_id = ids.server_id
LEFT JOIN newest AS n ON n.server_id = ids.server_id
LEFT JOIN counts ON counts.server_id = ids.server_id
LEFT JOIN LATERAL (
    SELECT cl.log_id, cl.fanout_item_count
    FROM collection_log AS cl
    WHERE cl.server_id = ids.server_id AND cl.collector_name = 'query_store' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE(n.capture_time, '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS run ON TRUE
LEFT JOIN LATERAL (
    SELECT cl.rows_collected, EXTRACT(EPOCH FROM ((now() AT TIME ZONE 'UTC') - cl.collection_time))::double precision AS age_seconds
    FROM collection_log AS cl
    WHERE cl.server_id = ids.server_id AND cl.collector_name = 'query_store_hints' AND cl.status = 'SUCCESS'
      AND cl.collection_time > COALESCE(n.capture_time, '-infinity'::timestamp)
    ORDER BY cl.collection_time DESC
    LIMIT 1) AS hints ON TRUE";

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

                var message = Message(
                    server.ServerName,
                    reader.IsDBNull(1) ? null : reader.GetString(1),
                    (int)reader.GetInt64(3),
                    (int)reader.GetInt64(4),
                    replicaConfigured: reader.GetBoolean(2),
                    server.HealthCaptureRefreshes,
                    reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    reader.GetBoolean(6),
                    OwnRunCapturedNothing(
                        reader.IsDBNull(7) ? null : reader.GetInt64(7),
                        reader.IsDBNull(8) ? null : reader.GetDouble(8),
                        maxAge: null));
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
