/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using PerformanceMonitor.Common;

namespace PerformanceMonitor.Darling.Viewer;

/// <summary>
/// The Availability Group topology read for the viewer's fleet-level AG tab (#991) — the STORE half only. Every
/// banding rule and the card projection live in <see cref="AgTopology"/> (PerformanceMonitor.Common), shared
/// with the web dashboard and Lite's AG tab, so the three surfaces cannot reach different verdicts about the
/// same AG. This file reads Postgres and maps rows; it decides nothing.
/// </summary>
public sealed partial class ViewerDataService
{
    /// <summary>Every replica row from each server's NEWEST AG collection. The inner aggregate picks the latest
    /// instant per server and the join keeps ALL rows at that instant (not <c>DISTINCT ON</c>, which would keep
    /// only one replica). Joined to the ENABLED registry so a disabled server's AGs leave the surface with it.
    /// Bare table names — the store's search_path resolves <c>collect</c>. $ none.</summary>
    public const string AgReplicaStatesSql = @"
SELECT
    r.server_id,
    r.server_name,
    r.collection_time,
    r.ag_name,
    r.replica_server_name,
    r.role_desc,
    r.is_local,
    r.operational_state_desc,
    r.connected_state_desc,
    r.recovery_health_desc,
    r.synchronization_health_desc,
    r.availability_mode_desc,
    r.failover_mode_desc,
    r.endpoint_url
FROM ag_replica_states AS r
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM ag_replica_states
    GROUP BY server_id
) AS latest
    ON r.server_id = latest.server_id
    AND r.collection_time = latest.max_collection_time
JOIN servers AS s
    ON s.server_id = r.server_id
    AND s.is_enabled
ORDER BY r.server_name, r.ag_name, r.replica_server_name";

    /// <summary>Every database-grain row from each server's NEWEST AG collection, same latest-snapshot shape as
    /// <see cref="AgReplicaStatesSql"/>. Windowed independently: the two collectors sweep separately, so their
    /// newest instants need not coincide. $ none.</summary>
    public const string AgDatabaseReplicaStatesSql = @"
SELECT
    d.server_id,
    d.server_name,
    d.collection_time,
    d.ag_name,
    d.database_name,
    d.replica_server_name,
    d.is_local,
    d.synchronization_state_desc,
    d.log_send_queue_size,
    d.redo_queue_size,
    d.log_send_rate,
    d.redo_rate,
    d.is_suspended,
    d.suspend_reason_desc,
    d.availability_mode_desc,
    d.secondary_lag_seconds
FROM ag_database_replica_states AS d
JOIN
(
    SELECT server_id, MAX(collection_time) AS max_collection_time
    FROM ag_database_replica_states
    GROUP BY server_id
) AS latest
    ON d.server_id = latest.server_id
    AND d.collection_time = latest.max_collection_time
JOIN servers AS s
    ON s.server_id = d.server_id
    AND s.is_enabled
ORDER BY d.server_name, d.ag_name, d.database_name, d.replica_server_name";

    /// <summary>
    /// Reads the fleet's AG topology into per-(reporting server, AG) cards. An AG-less fleet — the common case —
    /// costs exactly one indexed read that returns nothing, because the database-grain read is skipped entirely
    /// when there are no replicas. This runs on the viewer's refresh timer, so that short-circuit matters.
    /// </summary>
    public async Task<List<AgTopologyCard>> GetAvailabilityGroupsAsync(CancellationToken cancellationToken = default)
    {
        var replicas = await ReadAgReplicasAsync(cancellationToken);
        if (replicas.Count == 0)
        {
            return new List<AgTopologyCard>();
        }

        return AgTopology.BuildCards(replicas, await ReadAgDatabasesAsync(cancellationToken));
    }

    private async Task<List<AgTopologyReplicaRow>> ReadAgReplicasAsync(CancellationToken cancellationToken)
    {
        var rows = new List<AgTopologyReplicaRow>();
        await using var command = _dataSource.CreateCommand(AgReplicaStatesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AgTopologyReplicaRow
            {
                ServerId = reader.GetInt32(0),
                ServerName = reader.GetString(1),
                CollectionTime = reader.GetDateTime(2),
                AgName = AgText(reader, 3),
                ReplicaServerName = AgText(reader, 4),
                RoleDesc = AgText(reader, 5),
                IsLocal = AgFlag(reader, 6),
                OperationalStateDesc = AgText(reader, 7),
                ConnectedStateDesc = AgText(reader, 8),
                RecoveryHealthDesc = AgText(reader, 9),
                SynchronizationHealthDesc = AgText(reader, 10),
                AvailabilityModeDesc = AgText(reader, 11),
                FailoverModeDesc = AgText(reader, 12),
                EndpointUrl = AgText(reader, 13),
            });
        }

        return rows;
    }

    private async Task<List<AgTopologyDatabaseRow>> ReadAgDatabasesAsync(CancellationToken cancellationToken)
    {
        var rows = new List<AgTopologyDatabaseRow>();
        await using var command = _dataSource.CreateCommand(AgDatabaseReplicaStatesSql);
        command.CommandTimeout = ViewerCommandDeadlines.CurrentInteractiveReadSeconds;
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            rows.Add(new AgTopologyDatabaseRow
            {
                ServerId = reader.GetInt32(0),
                ServerName = reader.GetString(1),
                CollectionTime = reader.GetDateTime(2),
                AgName = AgText(reader, 3),
                DatabaseName = AgText(reader, 4),
                ReplicaServerName = AgText(reader, 5),
                IsLocal = AgFlag(reader, 6),
                SynchronizationStateDesc = AgText(reader, 7),
                LogSendQueueKb = AgCount(reader, 8),
                RedoQueueKb = AgCount(reader, 9),
                LogSendRateKbPerSec = AgCount(reader, 10),
                RedoRateKbPerSec = AgCount(reader, 11),
                IsSuspended = AgFlag(reader, 12),
                SuspendReasonDesc = AgText(reader, 13),
                AvailabilityModeDesc = AgText(reader, 14),
                SecondaryLagSeconds = AgCount(reader, 15),
            });
        }

        return rows;
    }

    private static string? AgText(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetString(ordinal);

    private static bool? AgFlag(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : reader.GetBoolean(ordinal);

    private static long? AgCount(NpgsqlDataReader reader, int ordinal) =>
        reader.IsDBNull(ordinal) ? null : Convert.ToInt64(reader.GetValue(ordinal), CultureInfo.InvariantCulture);
}
